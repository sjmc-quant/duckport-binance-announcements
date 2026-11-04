#!/usr/bin/env python3
"""导入币安公告历史。

默认读取 JSONL_DIR。加上 --from-binance 才请求币安公开接口，并先打印警告。
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from pathlib import Path

os.environ["TZ"] = "UTC"

from binance_announcements.config import (  # noqa: E402
    APPEND_CHUNK_TARGET_BYTES,
    CATALOGS,
    DUCKPORT_ADDR,
    DUCKPORT_SCHEMA,
    JSONL_DIR,
    PROXY_URL,
    REQUEST_DELAY_SECONDS,
    setup_logging,
)
from binance_announcements.data_jobs import write_pending_details  # noqa: E402
from binance_announcements.duckport_client import DuckportClient  # noqa: E402
from binance_announcements.fetch import (  # noqa: E402
    Client,
    HardStop,
    collect_backfill_from_list,
    iter_jsonl_frames,
)

logger = logging.getLogger(__name__)

BINANCE_PULL_WARNING = """
======== 警告 ========
即将从币安公开接口拉取公告，而不是读取 JSONL_DIR。
单线程、慢间隔。全量大约数千篇，可能要数小时。
遇到 403 或验证页会立刻停止，不会绕过验证。
请确认同实例的常驻进程已经停止，避免同时写 staging 表。
======================
"""


def warn_binance_pull() -> None:
    print(BINANCE_PULL_WARNING, file=sys.stderr, flush=True)
    logger.warning(
        "loadhist 将从币安公开接口拉取公告，不读取 JSONL_DIR；"
        "遇到 403 或验证页会停止，不会绕过。"
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="导入币安公告历史。默认读取 JSONL_DIR；--from-binance 改为请求币安公开接口。"
    )
    parser.add_argument(
        "--from-binance",
        action="store_true",
        help="从币安公开接口拉取尚未入库的公告，不读 JSONL_DIR。会先打印警告。",
    )
    return parser.parse_args(argv)


def jsonl_path(root: Path, catalog_id: int) -> Path:
    return root / f"binance-announcements-{catalog_id}.jsonl"


def import_jsonl_dir(client: DuckportClient, root: Path, catalogs: tuple[int, ...] | list[int]) -> int:
    if not root.is_dir():
        raise FileNotFoundError(root)
    total = 0
    for catalog_id in catalogs:
        path = jsonl_path(root, catalog_id)
        if not path.is_file():
            raise FileNotFoundError(path)
        logger.info("导入 %s", path)
        for frame in iter_jsonl_frames(path, catalog_id, APPEND_CHUNK_TARGET_BYTES):
            if frame.empty:
                continue
            resp = client.write_announcements(frame, target_bytes=APPEND_CHUNK_TARGET_BYTES)
            rows = int(resp.get("total_rows", len(frame)))
            total += rows
            logger.info("[%s] 已提交 %s 行", catalog_id, rows)
    return total


def import_from_binance(client: DuckportClient, catalogs: tuple[int, ...] | list[int]) -> int:
    http = Client(delay=REQUEST_DELAY_SECONDS, proxy=PROXY_URL or None)

    def request_stop(signum, frame) -> None:
        if http.stop_after:
            return
        http.stop_after = True
        logger.warning("收到中断，写完当前批后停止。")

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    known = client.read_known_codes()
    total_rows = 0
    for catalog_id in catalogs:
        if http.stop_after:
            break
        http.set_catalog(catalog_id)
        stored = known.get(catalog_id, set())
        listed, pending = collect_backfill_from_list(http, catalog_id, stored)
        logger.info("[%s] 接口 total=%s，待拉正文 %s 篇", catalog_id, listed, len(pending))
        if not pending:
            continue
        written = write_pending_details(client, http, catalog_id, pending)
        total_rows += written
        stored.update(item["code"] for item in pending)
        logger.info("[%s] 本轮写入尝试 %s 行", catalog_id, written)
    return total_rows


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    args = parse_args(argv)
    if args.from_binance:
        warn_binance_pull()
    elif not JSONL_DIR:
        logger.error("JSONL_DIR 为空，无法导入历史公告。要从币安拉取请加 --from-binance。")
        sys.exit(1)

    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)
    try:
        try:
            info = client.ping()
        except Exception as exc:
            logger.error("Cannot reach duckport-rs at %s: %s", DUCKPORT_ADDR, exc)
            sys.exit(1)
        logger.info("duckport-rs server: %s", info)
        client.init_schema()
        try:
            if args.from_binance:
                rows = import_from_binance(client, CATALOGS)
            else:
                rows = import_jsonl_dir(client, Path(JSONL_DIR), CATALOGS)
        except FileNotFoundError as exc:
            logger.error("缺少 jsonl：%s", exc)
            sys.exit(1)
        except HardStop as exc:
            logger.error("%s", exc)
            client.sync_duck_time()
            sys.exit(2)
        client.sync_duck_time()
        logger.info("loadhist 完成，写入尝试 %s 行", rows)
    finally:
        client.close()


if __name__ == "__main__":
    main()
