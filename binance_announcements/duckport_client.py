"""Arrow Flight client for the announcement table."""

from __future__ import annotations

import json
import logging
import math
import os
import threading
from contextlib import nullcontext
from datetime import datetime, timezone
from importlib import metadata
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight

from binance_announcements.config import (
    DUCKPORT_APPEND_MAX_MESSAGE_BYTES,
    TABLE_NAME,
    WATERMARK_MAX_LAG_SECONDS,
)
from binance_announcements.fetch import ANNOUNCEMENT_COLUMNS

logger = logging.getLogger(__name__)

# duckport-server answers a query with zero rows by closing the stream without a data
# message; pyarrow surfaces that as FlightInternalError with this text.
_ZERO_ROW_MESSAGE = "never sent a data message"

COLUMN_TYPES = {
    "id": "BIGINT",
    "code": "VARCHAR",
    "catalog_id": "INTEGER",
    "title": "VARCHAR",
    "url": "VARCHAR",
    "release_time": "TIMESTAMP",
    "publish_time": "TIMESTAMP",
    "seo_desc": "VARCHAR",
    "body_json": "VARCHAR",
    "body_text": "VARCHAR",
    "lang": "VARCHAR",
}
COL_DEFS = [f"{name} {COLUMN_TYPES[name]}" for name in ANNOUNCEMENT_COLUMNS]
STRING_COLUMNS = [
    "code",
    "title",
    "url",
    "seo_desc",
    "body_json",
    "body_text",
    "lang",
]
ARROW_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("code", pa.string()),
        ("catalog_id", pa.int32()),
        ("title", pa.string()),
        ("url", pa.string()),
        ("release_time", pa.timestamp("us")),
        ("publish_time", pa.timestamp("us")),
        ("seo_desc", pa.string()),
        ("body_json", pa.string()),
        ("body_text", pa.string()),
        ("lang", pa.string()),
    ]
)


def announcement_insert_sql(target: str, staging: str) -> str:
    """Insert rows whose (catalog_id, code) is not already in the business table."""
    return (
        f"INSERT INTO {target} "
        f"SELECT s.* FROM ("
        f"SELECT DISTINCT ON (catalog_id, code) * FROM {staging} "
        f"WHERE code IS NOT NULL AND code <> '' "
        f"ORDER BY catalog_id, code"
        f") s "
        f"ANTI JOIN (SELECT catalog_id, code FROM {target}) t "
        f"ON s.catalog_id = t.catalog_id AND s.code = t.code"
    )


def estimate_row_nbytes(table: pa.Table) -> list[int]:
    count = table.num_rows
    sizes = [0] * count
    if count == 0:
        return sizes
    for column in table.columns:
        typ = column.type
        if (
            pa.types.is_string(typ)
            or pa.types.is_large_string(typ)
            or pa.types.is_binary(typ)
            or pa.types.is_large_binary(typ)
        ):
            lengths = pc.fill_null(pc.binary_length(column), 0).to_pylist()
            for index, value in enumerate(lengths):
                sizes[index] += int(value)
            continue
        bit_width = getattr(typ, "bit_width", 64) or 64
        width = max(1, bit_width // 8)
        for index in range(count):
            sizes[index] += width
    return sizes


def split_arrow_by_bytes(table: pa.Table, target_bytes: int) -> list[pa.Table]:
    count = table.num_rows
    if count == 0:
        return []
    if target_bytes < 1:
        raise ValueError("target_bytes 必须为正")
    sizes = estimate_row_nbytes(table)
    parts: list[pa.Table] = []
    start = 0
    acc = 0
    for index, size in enumerate(sizes):
        if index > start and acc + size > target_bytes:
            parts.append(table.slice(start, index - start))
            start = index
            acc = 0
        acc += size
    parts.append(table.slice(start, count - start))
    return parts


def _normalize_flight_location(addr: str) -> str:
    if addr.startswith("grpc://") or addr.startswith("grpc+tls://"):
        return addr
    return f"grpc://{addr}"


def _resolve_token(token: Optional[str]) -> str:
    if token is not None:
        return token
    return os.getenv("DUCKPORT_TOKEN", "")


class _BearerMiddleware(flight.ClientMiddleware):
    def __init__(self, token: str) -> None:
        self._value = f"Bearer {token}".encode()

    def sending_headers(self):
        return {b"authorization": self._value}


class _BearerFactory(flight.ClientMiddlewareFactory):
    def __init__(self, token: str) -> None:
        self._token = token

    def start_call(self, info):
        return _BearerMiddleware(self._token)


def _open_flight_client(location: str, token: str, **kwargs):
    if token:
        kwargs["middleware"] = [_BearerFactory(token)]
    return flight.FlightClient(location, **kwargs)


class DuckportClient:
    WRITER = "binance-announcements"
    _KEEPALIVE_OPTIONS = [
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    ]

    def __init__(self, addr: str = "localhost:50051", schema: str = "data", token: Optional[str] = None):
        self.location = _normalize_flight_location(addr)
        self._token = _resolve_token(token)
        self.schema = schema
        self.ingestor = (
            os.getenv("INGESTOR_NAME")
            or os.getenv("INGESTOR_INSTANCE")
            or os.getenv("INGESTOR_EXEC")
            or self.WRITER
        )
        self.max_lag_seconds = WATERMARK_MAX_LAG_SECONDS
        self.append_max_message_bytes = int(
            os.getenv("DUCKPORT_APPEND_MAX_MESSAGE_BYTES", str(DUCKPORT_APPEND_MAX_MESSAGE_BYTES))
        )
        self._write_lock = threading.Lock()
        self.client = self._connect()
        logger.info(
            "DuckportClient connected: %s, schema=%s, append_max_message_bytes=%s",
            self.location,
            schema,
            self.append_max_message_bytes,
        )

    @staticmethod
    def _sql_str(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _reconnect(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass
        self.client = self._connect()
        logger.warning("DuckportClient reconnected: %s", self.location)

    def _connect(self):
        return _open_flight_client(
            self.location,
            self._token,
            generic_options=self._KEEPALIVE_OPTIONS,
        )

    def _append_max_rows_per_message(self, data: pa.Table, max_rows: Optional[int] = None) -> int:
        if len(data) == 0:
            return 1
        bytes_per_row = max(1, math.ceil(data.nbytes / len(data)))
        rows = max(1, self.append_max_message_bytes // bytes_per_row)
        return min(rows, max_rows) if max_rows is not None else rows

    def _write_context(self):
        lock = getattr(self, "_write_lock", None)
        return lock if lock is not None else nullcontext()

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass

    def ping(self) -> dict:
        action = flight.Action("duckport.ping", b"")
        results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute(self, sql: str) -> dict:
        body = json.dumps({"sql": sql}).encode("utf-8")
        action = flight.Action("duckport.execute", body)
        try:
            results = list(self.client.do_action(action))
        except flight.FlightUnavailableError:
            self._reconnect()
            results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def execute_transaction(self, statements: list[str]) -> dict:
        body = json.dumps({"statements": statements}).encode("utf-8")
        action = flight.Action("duckport.execute_transaction", body)
        try:
            results = list(self.client.do_action(action))
        except flight.FlightUnavailableError:
            self._reconnect()
            results = list(self.client.do_action(action))
        return json.loads(results[0].body.to_pybytes())

    def append(self, schema: str, table: str, data: pa.Table) -> dict:
        descriptor = flight.FlightDescriptor.for_path("duckport.append", schema, table)
        try:
            writer, reader = self.client.do_put(descriptor, data.schema)
        except flight.FlightUnavailableError:
            self._reconnect()
            writer, reader = self.client.do_put(descriptor, data.schema)
        max_chunksize = self._append_max_rows_per_message(data)
        writer.write_table(data, max_chunksize=max_chunksize)
        writer.done_writing()
        buf = reader.read()
        writer.close()
        return json.loads(bytes(buf))

    def query(self, sql: str) -> pa.Table:
        stripped = sql.lstrip()
        if not stripped.upper().startswith(("SELECT", "WITH", "EXPLAIN")):
            raise ValueError("duckport.query 只接受 SELECT、WITH 或 EXPLAIN")
        ticket = json.dumps({"type": "duckport.query", "sql": sql}).encode("utf-8")
        try:
            try:
                reader = self.client.do_get(flight.Ticket(ticket))
            except flight.FlightUnavailableError:
                self._reconnect()
                reader = self.client.do_get(flight.Ticket(ticket))
            return reader.read_all()
        except flight.FlightInternalError as exc:
            if _ZERO_ROW_MESSAGE in str(exc):
                return pa.table({})
            raise

    def init_schema(self) -> None:
        schema = self.schema
        columns = ", ".join(COL_DEFS)
        ingestor_sql = self._sql_str(self.ingestor)
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {schema}",
            f"CREATE TABLE IF NOT EXISTS {schema}.config_dict (key VARCHAR PRIMARY KEY, value VARCHAR)",
            f"CREATE TABLE IF NOT EXISTS {schema}.{TABLE_NAME} ({columns})",
            f"DROP TABLE IF EXISTS _staging_{TABLE_NAME}",
            f"CREATE TABLE _staging_{TABLE_NAME} ({columns})",
            f"INSERT OR REPLACE INTO {schema}.config_dict (key, value) "
            f"VALUES ('write_mode:{TABLE_NAME}', 'anti_join')",
            f"INSERT INTO {schema}.watermark "
            f"(table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
            f"VALUES ('{TABLE_NAME}', {ingestor_sql}, {self.max_lag_seconds}, 'release_time', "
            f"NULL, NULL, CURRENT_TIMESTAMP) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
            f"time_column = excluded.time_column",
        ]
        self.execute_transaction(statements)
        logger.info("Schema '%s' initialised for %s", schema, TABLE_NAME)
        self.register_table_specs([self._spec_sql()])

    def _spec_sql(self) -> str:
        try:
            version = metadata.version("binance-announcements")
        except metadata.PackageNotFoundError:
            version = ""

        def text(value: object) -> str:
            return "NULL" if value is None else self._sql_str(str(value))

        def doc(value: object) -> str:
            return self._sql_str(json.dumps(value or {}, ensure_ascii=True, sort_keys=True))

        def strings(values) -> str:
            return "[" + ", ".join(self._sql_str(item) for item in values) + "]::VARCHAR[]"

        columns = {name: COLUMN_TYPES[name] for name in ANNOUNCEMENT_COLUMNS}
        checks = {"code": {"max_len": 64}}
        return (
            f"INSERT OR REPLACE INTO {self.schema}.table_specs (table_name, writer, writer_version, kind, "
            f"time_column, period_seconds, dedup_key, write_mode, columns, column_defaults, string_checks, "
            f"legacy_columns, updated_at) VALUES ({text(TABLE_NAME)}, {text(self.WRITER)}, {text(version)}, "
            f"{text('append')}, {text('release_time')}, NULL, "
            f"{strings(['catalog_id', 'code'])}, 'anti_join', {doc(columns)}, {doc({})}, "
            f"{doc(checks)}, {strings([])}, CURRENT_TIMESTAMP)"
        )

    def register_table_specs(self, statements: list[str]) -> bool:
        try:
            self.execute_transaction(statements)
        except Exception as exc:
            logger.info(
                "table specs not registered (duckport-server < 1.2.0?): %s",
                str(exc).splitlines()[0][:200],
            )
            return False
        logger.info("table specs registered: %s table(s)", len(statements))
        return True

    def read_known_codes(self) -> dict[int, set[str]]:
        table = self.query(
            f"SELECT catalog_id, code FROM {self.schema}.{TABLE_NAME} "
            f"WHERE code IS NOT NULL AND code <> ''"
        )
        found: dict[int, set[str]] = {}
        if table.num_rows == 0:
            return found
        catalogs = table.column("catalog_id").to_pylist()
        codes = table.column("code").to_pylist()
        for catalog_id, code in zip(catalogs, codes):
            if catalog_id is None or not code:
                continue
            found.setdefault(int(catalog_id), set()).add(str(code))
        return found

    def _to_arrow(self, df: pd.DataFrame) -> pa.Table:
        sub = df.loc[:, ANNOUNCEMENT_COLUMNS].copy()
        for column in STRING_COLUMNS:
            sub[column] = sub[column].fillna("").astype(str)
        sub["id"] = pd.to_numeric(sub["id"], errors="coerce").astype("Int64")
        sub["catalog_id"] = pd.to_numeric(sub["catalog_id"], errors="coerce").astype("Int32")
        for column in ("release_time", "publish_time"):
            sub[column] = pd.to_datetime(sub[column], utc=True).dt.tz_localize(None)
        return pa.Table.from_pandas(sub, schema=ARROW_SCHEMA, preserve_index=False)

    def write_announcements(self, df: pd.DataFrame, *, target_bytes: int = 32 * 1024 * 1024) -> dict:
        if df is None or df.empty:
            return {"total_rows": 0}
        arrow = self._to_arrow(df)
        parts = split_arrow_by_bytes(arrow, target_bytes)
        staging = f"_staging_{TABLE_NAME}"
        target = f"{self.schema}.{TABLE_NAME}"
        merge = announcement_insert_sql(target, staging)
        total = 0
        with self._write_context():
            for part in parts:
                self.execute(f"TRUNCATE {staging}")
                self.append("main", staging, part)
                self.execute_transaction([merge, f"TRUNCATE {staging}"])
                total += part.num_rows
        logger.info("write_announcements: rows=%s chunks=%s", total, len(parts))
        return {"total_rows": total}

    def sync_duck_time(self) -> None:
        table = self.query(
            f"SELECT max(release_time) AS duck_time FROM {self.schema}.{TABLE_NAME}"
        )
        if table.num_rows == 0:
            return
        value = table.column("duck_time")[0].as_py()
        if value is None or pd.isna(value):
            return
        if isinstance(value, datetime) and value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        stamp = pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S.%f")
        schema = self.schema
        self.execute_transaction(
            [
                f"INSERT INTO {schema}.watermark "
                f"(table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
                f"VALUES ('{TABLE_NAME}', {self._sql_str(self.ingestor)}, {self.max_lag_seconds}, "
                f"'release_time', NULL, '{stamp}', CURRENT_TIMESTAMP) "
                f"ON CONFLICT (table_name) DO UPDATE SET "
                f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
                f"duck_time = excluded.duck_time, updated_at = excluded.updated_at"
            ]
        )
