"""Polling loop: list new announcement codes, fetch bodies, write through staging."""

from __future__ import annotations

import logging
import threading

from binance_announcements.config import (
    APPEND_CHUNK_TARGET_BYTES,
    CATALOGS,
    CHALLENGE_COOLDOWN_SECONDS,
    POLL_INTERVAL_SECONDS,
    PROXY_URL,
    REQUEST_DELAY_SECONDS,
)
from binance_announcements.duckport_client import DuckportClient
from binance_announcements.fetch import (
    Client,
    HardStop,
    Transient,
    collect_new_from_list,
    iter_detail_records,
    record_payload_bytes,
    records_to_frame,
)

logger = logging.getLogger(__name__)


def write_pending_details(client: DuckportClient, http: Client, catalog_id: int, pending: list[dict]) -> int:
    """拉取正文并分块写入。遇到 HardStop 时先写下已拿到的正文再抛出。"""
    written = 0
    batch: list[dict] = []
    size = 0

    def flush() -> None:
        nonlocal written
        if not batch:
            return
        frame = records_to_frame(batch)
        batch.clear()
        if frame.empty:
            return
        resp = client.write_announcements(frame, target_bytes=APPEND_CHUNK_TARGET_BYTES)
        written += int(resp.get("total_rows", len(frame)))

    try:
        for rec in iter_detail_records(http, catalog_id, pending):
            batch.append(rec)
            size += record_payload_bytes(rec)
            if size >= APPEND_CHUNK_TARGET_BYTES:
                flush()
                size = 0
    except HardStop:
        flush()
        raise
    flush()
    return written


class AnnouncementJobs:
    def __init__(self, client: DuckportClient):
        self.client = client
        self._stop = threading.Event()
        self._http: Client | None = None
        self.thread = threading.Thread(
            target=self._loop, name="binance-announcements", daemon=True
        )
        self.thread.start()

    def request_stop(self) -> None:
        self._stop.set()
        http = self._http
        if http is not None:
            http.stop_after = True

    def _loop(self) -> None:
        http = Client(delay=REQUEST_DELAY_SECONDS, proxy=PROXY_URL or None)
        self._http = http
        while not self._stop.is_set():
            try:
                self.poll_once(http)
            except Transient as exc:
                logger.warning("%s；%s 秒后再试", exc, POLL_INTERVAL_SECONDS)
                if self._stop.wait(POLL_INTERVAL_SECONDS):
                    return
                continue
            except HardStop as exc:
                logger.error("%s；%s 秒后再试", exc, CHALLENGE_COOLDOWN_SECONDS)
                if self._stop.wait(CHALLENGE_COOLDOWN_SECONDS):
                    return
                continue
            except Exception:
                logger.exception("本轮公告拉取失败，%s 秒后重试", POLL_INTERVAL_SECONDS)
                if self._stop.wait(POLL_INTERVAL_SECONDS):
                    return
                continue
            if self._stop.wait(POLL_INTERVAL_SECONDS):
                return

    def poll_once(self, http: Client) -> None:
        # 连续失败只在一轮之内累计；上一轮冷却前的计数不带进新一轮。
        http.consecutive_bad = 0
        known = self.client.read_known_codes()
        wrote = False
        try:
            for catalog_id in CATALOGS:
                if http.stop_after or self._stop.is_set():
                    break
                http.set_catalog(catalog_id)
                stored = known.get(catalog_id, set())
                if not stored:
                    logger.warning(
                        "[%s] 尚无历史，只收录首页。全量历史请先 duckport loadhist",
                        catalog_id,
                    )
                total, pending = collect_new_from_list(
                    http,
                    catalog_id,
                    stored,
                    catalog_empty=not stored,
                )
                logger.info(
                    "[%s] 接口 total=%s，待拉正文 %s 篇",
                    catalog_id,
                    total,
                    len(pending),
                )
                if not pending:
                    continue
                wrote = write_pending_details(self.client, http, catalog_id, pending) > 0 or wrote
        except HardStop:
            if wrote:
                self.client.sync_duck_time()
            raise
        self.client.sync_duck_time()
