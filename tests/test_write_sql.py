import pyarrow as pa
import pyarrow.flight as flight
import pytest

from binance_announcements.duckport_client import (
    COL_DEFS,
    DuckportClient,
    announcement_insert_sql,
    split_arrow_by_bytes,
)
from binance_announcements.fetch import records_to_frame


def test_insert_sql_is_anti_join_on_catalog_and_code():
    sql = announcement_insert_sql("data.binance_announcements", "_staging_binance_announcements")
    assert "DISTINCT ON (catalog_id, code)" in sql
    assert "ANTI JOIN" in sql
    assert "ON CONFLICT" not in sql.upper()


def test_insert_sql_is_idempotent_in_duckdb():
    import duckdb

    columns = ", ".join(COL_DEFS)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE announcements ({columns})")
    con.execute(f"CREATE TABLE staging ({columns})")
    con.execute(
        """
        INSERT INTO staging VALUES
        (1, 'a', 48, 't1', 'u', TIMESTAMP '2026-01-01', NULL, '', '{}', 'body', 'zh-CN'),
        (1, 'a', 48, 't2', 'u', TIMESTAMP '2026-01-02', NULL, '', '{}', 'body2', 'zh-CN'),
        (2, 'a', 49, 'other', 'u', TIMESTAMP '2026-01-03', NULL, '', '{}', 'x', 'zh-CN'),
        (3, '', 48, 'blank', 'u', TIMESTAMP '2026-01-04', NULL, '', '{}', 'z', 'zh-CN')
        """
    )
    sql = announcement_insert_sql("announcements", "staging")
    con.execute(sql)
    con.execute(sql)
    assert con.execute("SELECT count(*) FROM announcements").fetchone()[0] == 2
    codes = con.execute(
        "SELECT catalog_id, code FROM announcements ORDER BY catalog_id"
    ).fetchall()
    assert codes == [(48, "a"), (49, "a")]


def test_split_arrow_respects_byte_budget_and_keeps_oversized_row():
    payload = "x" * 1000
    table = pa.table({"body": pa.array([payload] * 10, type=pa.string())})
    parts = split_arrow_by_bytes(table, target_bytes=2500)
    assert len(parts) >= 4
    assert sum(part.num_rows for part in parts) == 10

    huge = pa.table({"body": pa.array(["z" * 5000, "y"], type=pa.string())})
    huge_parts = split_arrow_by_bytes(huge, target_bytes=1000)
    assert [part.num_rows for part in huge_parts] == [1, 1]


def test_to_arrow_forces_strings_and_microsecond_timestamps():
    frame = records_to_frame(
        [
            {
                "id": 7,
                "code": "abc",
                "catalog_id": 48,
                "title": None,
                "release_ms": 1788252309916,
                "publish_ms": None,
                "body_json": "{}",
                "body_text": "正文",
                "lang_requested": "zh-CN",
            }
        ]
    )
    table = DuckportClient._to_arrow(None, frame)
    assert table.schema.field("code").type == pa.string()
    assert table.schema.field("title").type == pa.string()
    assert table.column("title")[0].as_py() == ""
    assert table.schema.field("release_time").type == pa.timestamp("us")
    assert table.column("publish_time")[0].as_py() is None


def test_split_empty_table():
    empty = pa.table({"body": pa.array([], type=pa.string())})
    assert split_arrow_by_bytes(empty, target_bytes=100) == []


class _FlightRaising:
    def __init__(self, exc: Exception):
        self.exc = exc

    def do_get(self, ticket):
        raise self.exc


def _client_raising(exc: Exception) -> DuckportClient:
    client = DuckportClient.__new__(DuckportClient)
    client.schema = "data"
    client.client = _FlightRaising(exc)
    return client


def test_zero_row_query_reads_as_empty_table():
    client = _client_raising(flight.FlightInternalError("Server never sent a data message"))
    assert client.query("SELECT 1 WHERE false").num_rows == 0
    assert client.read_known_codes() == {}


def test_other_internal_errors_still_raise():
    client = _client_raising(flight.FlightInternalError("Catalog Error: Table does not exist"))
    with pytest.raises(flight.FlightInternalError):
        client.read_known_codes()
