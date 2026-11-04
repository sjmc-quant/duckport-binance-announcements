"""Write a few fixture rows through a running duckport-server, then delete them.

Opt-in: set DUCKPORT_TEST_ADDR (host:port) to a throwaway server. The plugin's own
DUCKPORT_ADDR is not used, because on a duckport host it resolves to the production
server from /opt/duckport/server.env, and this test creates the table, its watermark
and table_specs rows there.
"""

import json
import os
import socket

import pytest

from binance_announcements.config import DUCKPORT_SCHEMA, TABLE_NAME
from binance_announcements.duckport_client import DuckportClient
from binance_announcements.loadhist import import_jsonl_dir

FIXTURE_CODES = (
    "dpfixture00000000000000000000000a",
    "dpfixture00000000000000000000000b",
)


DUCKPORT_ADDR = os.getenv("DUCKPORT_TEST_ADDR", "")


def _server_reachable() -> bool:
    host, _, port = DUCKPORT_ADDR.rpartition(":")
    try:
        with socket.create_connection((host or "localhost", int(port)), timeout=1):
            return True
    except OSError:
        return False


def _write_fixture(root) -> None:
    rows = {
        48: {
            "id": 1,
            "code": FIXTURE_CODES[0],
            "catalog_id": 48,
            "title": "fixture-a",
            "url": "https://www.binance.com/zh-CN/support/announcement/" + FIXTURE_CODES[0],
            "release_ms": 1700000000000,
            "publish_ms": 1700000001000,
            "seo_desc": "",
            "body_json": "{}",
            "body_text": "fixture",
            "lang_requested": "zh-CN",
        },
        49: {
            "id": 2,
            "code": FIXTURE_CODES[1],
            "catalog_id": 49,
            "title": "fixture-b",
            "url": "https://www.binance.com/zh-CN/support/announcement/" + FIXTURE_CODES[1],
            "release_ms": 1700000002000,
            "publish_ms": None,
            "seo_desc": "",
            "body_json": "{}",
            "body_text": "fixture",
            "lang_requested": "zh-CN",
        },
    }
    for catalog_id, rec in rows.items():
        path = root / f"binance-announcements-{catalog_id}.jsonl"
        path.write_text("not-json\n" + json.dumps(rec, ensure_ascii=False) + "\n", encoding="utf-8")
    (root / "binance-announcements-161.jsonl").write_text("", encoding="utf-8")


def _quoted_codes() -> str:
    return ", ".join("'" + code + "'" for code in FIXTURE_CODES)


def test_fixture_import_is_idempotent(tmp_path):
    if not DUCKPORT_ADDR:
        pytest.skip("未设置 DUCKPORT_TEST_ADDR（指向一次性 duckport-server，不要用生产实例）")
    if not _server_reachable():
        pytest.skip(f"duckport-server 未监听 {DUCKPORT_ADDR}")
    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)
    started = False
    try:
        try:
            client.ping()
        except Exception as exc:
            pytest.skip(f"duckport ping 失败: {exc}")
        client.init_schema()
        started = True
        _write_fixture(tmp_path)
        import_jsonl_dir(client, tmp_path, (48, 49, 161))
        client.sync_duck_time()
        import_jsonl_dir(client, tmp_path, (48, 49, 161))
        found = client.query(
            f"SELECT code FROM {DUCKPORT_SCHEMA}.{TABLE_NAME} "
            f"WHERE code IN ({_quoted_codes()})"
        )
        assert sorted(found.column("code").to_pylist()) == sorted(FIXTURE_CODES)
        watermark = client.query(
            f"SELECT duck_time FROM {DUCKPORT_SCHEMA}.watermark "
            f"WHERE table_name = '{TABLE_NAME}'"
        )
        assert watermark.column("duck_time")[0].as_py() is not None
    finally:
        if started:
            client.execute(
                f"DELETE FROM {DUCKPORT_SCHEMA}.{TABLE_NAME} WHERE code IN ({_quoted_codes()})"
            )
            client.execute(f"TRUNCATE _staging_{TABLE_NAME}")
            remaining = client.query(
                f"SELECT count(*) AS n FROM {DUCKPORT_SCHEMA}.{TABLE_NAME}"
            )
            left = int(remaining.column("n")[0].as_py())
            if left == 0:
                client.execute(
                    f"UPDATE {DUCKPORT_SCHEMA}.watermark "
                    f"SET duck_time = NULL, updated_at = CURRENT_TIMESTAMP "
                    f"WHERE table_name = '{TABLE_NAME}'"
                )
            else:
                client.sync_duck_time()
        client.close()
