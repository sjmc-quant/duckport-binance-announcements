"""Public Binance announcement fetch and record normalization.

Single-threaded GET, fixed user agent, certificate verification, no cookies.
HTTP 403 and HTML challenge pages raise HardStop; callers must not bypass them.
"""
from __future__ import annotations

import json
import logging
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

MIN_REQUEST_DELAY_SECONDS = 1.5
PAGE_SIZE = 20
DEFAULT_DELAY = 2.0
JITTER_LO = 0.3
JITTER_HI = 0.8
BACKOFF_SECS = (30, 60, 120)
MAX_BACKOFF_TRIES = 3
MAX_CONSECUTIVE_BAD = 3
REQUEST_TIMEOUT = 60
DEFAULT_CATALOGS = (48, 49, 161)

LIST_URL = "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query"
DETAIL_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query"
ARTICLE_URL = "https://www.binance.com/zh-CN/support/announcement/{code}"

ANNOUNCEMENT_COLUMNS = [
    "id",
    "code",
    "catalog_id",
    "title",
    "url",
    "release_time",
    "publish_time",
    "seo_desc",
    "body_json",
    "body_text",
    "lang",
]

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN",
    "lang": "zh-CN",
    "clienttype": "web",
    "Connection": "close",
}

OK_BUSINESS_CODES = {"000000", "00000"}


class HardStop(Exception):
    """验证墙、403、或连续失败。调用方应停下来，不要绕过。"""


class Transient(HardStop):
    """网络错误或 5xx 退避耗尽。和 HardStop 一样中止本轮，但常驻进程按轮询间隔重试，不进入验证冷却。"""


def validate_request_delay(delay: float) -> float:
    if delay < MIN_REQUEST_DELAY_SECONDS:
        raise ValueError(
            f"REQUEST_DELAY_SECONDS 不允许小于 {MIN_REQUEST_DELAY_SECONDS}"
        )
    return delay


def make_ssl_context(*, tls12_only: bool = False) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    if tls12_only and hasattr(ssl, "TLSVersion"):
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def catalog_referer(catalog_id: int) -> str:
    return f"https://www.binance.com/zh-CN/support/announcement/list/{catalog_id}"


def list_query_url(catalog_id: int, page: int, page_size: int = PAGE_SIZE) -> str:
    qs = urllib.parse.urlencode(
        {
            "type": "1",
            "catalogId": str(catalog_id),
            "pageNo": str(page),
            "pageSize": str(page_size),
        }
    )
    return f"{LIST_URL}?{qs}"


def detail_query_url(code: str) -> str:
    qs = urllib.parse.urlencode({"articleCode": code})
    return f"{DETAIL_URL}?{qs}"


def extract_body_text(body: object) -> str:
    """从详情 body（AST JSON 字符串或对象）抽出纯文本。"""
    node: object = body
    if isinstance(node, str):
        raw = node.strip()
        if not raw:
            return ""
        try:
            node = json.loads(raw)
        except json.JSONDecodeError:
            return raw
    texts: list[str] = []

    def walk(n: object) -> None:
        if isinstance(n, dict):
            text = n.get("text")
            if isinstance(text, str):
                texts.append(text)
            for key, value in n.items():
                if key == "text":
                    continue
                walk(value)
        elif isinstance(n, list):
            for item in n:
                walk(item)

    walk(node)
    return "".join(texts)


def looks_like_challenge(raw: bytes, content_type: str) -> bool:
    ctype = content_type.lower()
    if "text/html" in ctype:
        return True
    head = raw.lstrip()[:32].lower()
    if head.startswith(b"<!doctype") or head.startswith(b"<html"):
        return True
    sample = raw[:4000].decode("utf-8", "replace").lower()
    needles = (
        "just a moment",
        "not a robot",
        "cf-challenge",
        "cf-browser-verification",
        "enable javascript and then reload",
    )
    return any(needle in sample for needle in needles)


def parse_list_payload(payload: dict) -> tuple[int | None, list[dict]]:
    if str(payload.get("code", "")) not in OK_BUSINESS_CODES:
        raise HardStop(f"列表接口业务码异常：{payload.get('code')}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise HardStop("列表接口缺少 data。")
    catalogs = data.get("catalogs")
    if not isinstance(catalogs, list) or not catalogs:
        raise HardStop("列表接口缺少 catalogs。")
    catalog = catalogs[0]
    if not isinstance(catalog, dict):
        raise HardStop("catalogs[0] 不是对象。")
    total = catalog.get("total")
    total_n = int(total) if isinstance(total, int) else None
    raw_articles = catalog.get("articles") or []
    if not isinstance(raw_articles, list):
        raise HardStop("articles 不是数组。")
    articles: list[dict] = []
    for item in raw_articles:
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        if not isinstance(code, str) or not code:
            continue
        articles.append(item)
    return total_n, articles


def page_frontier(
    articles: list[dict],
    stored_codes: set[str],
    *,
    catalog_empty: bool,
) -> tuple[list[dict], bool]:
    """返回本页尚未入库的公告，以及翻完本页后是否停止。

    整页都已入库才停止，和原脚本一致：列表偶尔不按时间排序，或某篇正文上轮没拉到时，
    有新旧混排的页仍会继续往后翻。该 catalog 还没有任何行时也停止，
    这样空表只会留下首页，不会把整段历史再请求一遍。
    """
    fresh: list[dict] = []
    for item in articles:
        code = item.get("code")
        if not isinstance(code, str) or not code:
            continue
        if code in stored_codes:
            continue
        fresh.append(item)
    if not articles or catalog_empty or not fresh:
        return fresh, True
    return fresh, False


def collect_new_from_list(
    client: "Client",
    catalog_id: int,
    stored_codes: set[str],
    *,
    catalog_empty: bool,
) -> tuple[int | None, list[dict]]:
    collected: list[dict] = []
    seen_new: set[str] = set()
    total: int | None = None
    page = 1
    while True:
        if client.stop_after:
            break
        logger.info("[%s] 拉列表第 %s 页", catalog_id, page)
        payload = client.get_json(list_query_url(catalog_id, page))
        page_total, articles = parse_list_payload(payload)
        if page_total is not None:
            total = page_total
        fresh, stop = page_frontier(
            articles, stored_codes, catalog_empty=catalog_empty
        )
        new_on_page = 0
        for item in fresh:
            code = item["code"]
            if code in seen_new:
                continue
            seen_new.add(code)
            collected.append(item)
            new_on_page += 1
        logger.info(
            "[%s] 第 %s 页新 %s/%s", catalog_id, page, new_on_page, len(articles)
        )
        if stop or not articles:
            break
        if total is not None and page * PAGE_SIZE >= total:
            break
        page += 1
    return total, collected


def collect_backfill_from_list(
    client: "Client",
    catalog_id: int,
    stored_codes: set[str],
) -> tuple[int | None, list[dict]]:
    """翻完整个 catalog，只收集尚未入库的 code。已入库的页不会让翻页停下来。"""
    collected: list[dict] = []
    seen_new: set[str] = set()
    total: int | None = None
    page = 1
    while True:
        if client.stop_after:
            break
        logger.info("[%s] 拉列表第 %s 页", catalog_id, page)
        payload = client.get_json(list_query_url(catalog_id, page))
        page_total, articles = parse_list_payload(payload)
        if page_total is not None:
            total = page_total
        if not articles:
            break
        new_on_page = 0
        for item in articles:
            code = item["code"]
            if code in stored_codes or code in seen_new:
                continue
            seen_new.add(code)
            collected.append(item)
            new_on_page += 1
        logger.info(
            "[%s] 第 %s 页新 %s/%s", catalog_id, page, new_on_page, len(articles)
        )
        if total is not None and page * PAGE_SIZE >= total:
            break
        page += 1
    return total, collected


def record_from_detail(meta: dict, detail: dict, catalog_id: int) -> dict:
    data = detail.get("data") if isinstance(detail.get("data"), dict) else {}
    code = str(meta.get("code") or data.get("code") or "")
    title = data.get("title") or meta.get("title") or ""
    body = data.get("body")
    if isinstance(body, str):
        body_json = body
    elif body is None:
        body_json = ""
    else:
        body_json = json.dumps(body, ensure_ascii=False)
    return {
        "id": data.get("id", meta.get("id")),
        "code": code,
        "title": title,
        "url": ARTICLE_URL.format(code=code),
        "release_ms": meta.get("releaseDate"),
        "publish_ms": data.get("publishDate"),
        "seo_desc": data.get("seoDesc") or "",
        "body_json": body_json,
        "body_text": extract_body_text(body) if body else "",
        "lang_requested": "zh-CN",
        "catalog_id": catalog_id,
    }


def iter_detail_records(client: "Client", catalog_id: int, pending: list[dict]):
    for index, meta in enumerate(pending, start=1):
        if client.stop_after:
            logger.info("[%s] 中断，停止拉正文。", catalog_id)
            return
        code = str(meta["code"])
        logger.info(
            "[%s] 正文 %s/%s %s %s",
            catalog_id,
            index,
            len(pending),
            code,
            meta.get("title", ""),
        )
        try:
            payload = client.get_json(detail_query_url(code))
        except RuntimeError as exc:
            logger.warning("[%s] 跳过 %s：%s", catalog_id, code, exc)
            continue
        if str(payload.get("code", "")) not in OK_BUSINESS_CODES:
            client.note_bad()
            logger.warning("[%s] 跳过 %s：业务码 %s", catalog_id, code, payload.get("code"))
            continue
        if not isinstance(payload.get("data"), dict):
            client.note_bad()
            logger.warning("[%s] 跳过 %s：详情无 data", catalog_id, code)
            continue
        yield record_from_detail(meta, payload, catalog_id)


def ms_to_naive_utc(value: object) -> pd.Timestamp:
    if value is None or value == "":
        return pd.NaT
    try:
        millis = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return pd.NaT
    return pd.to_datetime(millis, unit="ms", utc=True).tz_localize(None)


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _as_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _pick_time(rec: dict, ms_key: str, ts_key: str) -> pd.Timestamp:
    if rec.get(ms_key) not in (None, ""):
        return ms_to_naive_utc(rec.get(ms_key))
    if rec.get(ts_key) in (None, ""):
        return pd.NaT
    parsed = pd.to_datetime(rec.get(ts_key), utc=True, errors="coerce")
    if pd.isna(parsed):
        return pd.NaT
    if getattr(parsed, "tzinfo", None) is not None:
        return parsed.tz_convert("UTC").tz_localize(None)
    return parsed


def records_to_frame(records: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for rec in records:
        code = rec.get("code")
        if not isinstance(code, str) or not code.strip():
            continue
        catalog_id = _optional_int(rec.get("catalog_id"))
        if catalog_id is None:
            continue
        rows.append(
            {
                "id": _optional_int(rec.get("id")),
                "code": code.strip(),
                "catalog_id": catalog_id,
                "title": _as_text(rec.get("title")),
                "url": _as_text(rec.get("url")),
                "release_time": _pick_time(rec, "release_ms", "release_time"),
                "publish_time": _pick_time(rec, "publish_ms", "publish_time"),
                "seo_desc": _as_text(rec.get("seo_desc")),
                "body_json": _as_text(rec.get("body_json")),
                "body_text": _as_text(rec.get("body_text")),
                "lang": _as_text(rec.get("lang") or rec.get("lang_requested") or "zh-CN"),
            }
        )
    frame = pd.DataFrame(rows, columns=ANNOUNCEMENT_COLUMNS)
    if frame.empty:
        frame["release_time"] = pd.to_datetime(frame["release_time"])
        frame["publish_time"] = pd.to_datetime(frame["publish_time"])
    return frame


def record_payload_bytes(rec: dict) -> int:
    return (
        len(_as_text(rec.get("body_json")))
        + len(_as_text(rec.get("body_text")))
        + len(_as_text(rec.get("title")))
        + len(_as_text(rec.get("seo_desc")))
        + 256
    )


def parse_jsonl_line(line: str, catalog_id: int) -> dict | None:
    raw = line.strip()
    if not raw:
        return None
    try:
        rec = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(rec, dict):
        return None
    code = rec.get("code")
    if not isinstance(code, str) or not code.strip():
        return None
    out = dict(rec)
    out["code"] = code.strip()
    if out.get("catalog_id") is None:
        out["catalog_id"] = catalog_id
    return out


def iter_jsonl_frames(path: Path, catalog_id: int, target_bytes: int):
    """按大约 target_bytes 产出 DataFrame，跳过空行、坏 JSON 和没有 code 的行。"""
    if target_bytes < 1:
        raise ValueError("target_bytes 必须为正")
    batch: list[dict] = []
    size = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            rec = parse_jsonl_line(line, catalog_id)
            if rec is None:
                logger.warning("%s:%s 无法解析或缺少 code，已跳过", path.name, line_no)
                continue
            weight = record_payload_bytes(rec)
            if batch and size + weight > target_bytes:
                yield records_to_frame(batch)
                batch = []
                size = 0
            batch.append(rec)
            size += weight
    if batch:
        yield records_to_frame(batch)


class Client:
    def __init__(self, delay: float, proxy: str | None = None) -> None:
        self.delay = validate_request_delay(delay)
        self.consecutive_bad = 0
        self._first = True
        self.stop_after = False
        self._ctx = make_ssl_context()
        self._tls12_only = False
        self.headers = dict(BASE_HEADERS)
        self.proxy = proxy
        self.catalog_id = DEFAULT_CATALOGS[0]
        self.set_catalog(self.catalog_id)
        self._rebuild_opener()

    def set_catalog(self, catalog_id: int) -> None:
        self.catalog_id = catalog_id
        self.headers["Referer"] = catalog_referer(catalog_id)

    def _rebuild_opener(self) -> None:
        handlers: list[urllib.request.BaseHandler] = [
            urllib.request.HTTPSHandler(context=self._ctx)
        ]
        if self.proxy:
            handlers.append(
                urllib.request.ProxyHandler({"http": self.proxy, "https": self.proxy})
            )
        self._opener = urllib.request.build_opener(*handlers)

    def pace(self) -> None:
        if self._first:
            self._first = False
            return
        time.sleep(self.delay + random.uniform(JITTER_LO, JITTER_HI))

    def note_bad(self) -> None:
        self.consecutive_bad += 1
        if self.consecutive_bad >= MAX_CONSECUTIVE_BAD:
            raise HardStop(f"连续 {MAX_CONSECUTIVE_BAD} 次非 200，已停止。")

    def get_json(self, url: str) -> dict:
        raw, ctype, status = self._get_bytes(url)
        if looks_like_challenge(raw, ctype):
            raise HardStop(f"收到验证页或 HTML（HTTP {status}），已停止。改天再续跑。")
        if status == 403:
            raise HardStop("HTTP 403，已停止。不要绕过验证。")
        if status != 200:
            raise HardStop(f"HTTP {status}，已停止。")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HardStop(f"响应不是 JSON：{exc}") from exc
        if not isinstance(payload, dict):
            raise HardStop("响应 JSON 不是对象。")
        self.consecutive_bad = 0
        return payload

    def _get_bytes(self, url: str) -> tuple[bytes, str, int]:
        backoff_i = 0
        while True:
            if self.stop_after:
                raise HardStop("收到中断，已停止。")
            self.pace()
            try:
                req = urllib.request.Request(url, headers=self.headers, method="GET")
                with self._opener.open(req, timeout=REQUEST_TIMEOUT) as resp:
                    status = int(resp.status)
                    raw = resp.read()
                    ctype = resp.headers.get("Content-Type", "") or ""
            except urllib.error.HTTPError as exc:
                status = int(exc.code)
                raw = exc.read() if exc.fp else b""
                ctype = ""
                if exc.headers is not None:
                    ctype = exc.headers.get("Content-Type", "") or ""
            except urllib.error.URLError as exc:
                reason = str(exc).lower()
                if not self._tls12_only and (
                    "ssl" in reason or "handshake" in reason or "eof" in reason
                ):
                    self._tls12_only = True
                    self._ctx = make_ssl_context(tls12_only=True)
                    self._rebuild_opener()
                    logger.info("TLS 握手异常，后续请求改用 TLS 1.2（仍校验证书）。")
                if backoff_i >= MAX_BACKOFF_TRIES:
                    raise Transient(f"网络错误重试耗尽：{exc}") from exc
                wait = BACKOFF_SECS[backoff_i]
                logger.warning(
                    "网络错误，%ss 后重试（%s/%s）：%s",
                    wait,
                    backoff_i + 1,
                    MAX_BACKOFF_TRIES,
                    exc,
                )
                time.sleep(wait)
                backoff_i += 1
                continue

            if looks_like_challenge(raw, ctype):
                raise HardStop(f"收到验证页或 HTML（HTTP {status}），已停止。改天再续跑。")
            if status == 403:
                raise HardStop("HTTP 403，已停止。不要绕过验证。")
            if status == 429 or status >= 500:
                if backoff_i >= MAX_BACKOFF_TRIES:
                    self.note_bad()
                    if status == 429:
                        raise HardStop(f"HTTP {status} 退避耗尽，已停止。")
                    raise Transient(f"HTTP {status} 退避耗尽，已停止。")
                wait = BACKOFF_SECS[backoff_i]
                logger.warning(
                    "HTTP %s，%ss 后重试（%s/%s）",
                    status,
                    wait,
                    backoff_i + 1,
                    MAX_BACKOFF_TRIES,
                )
                time.sleep(wait)
                backoff_i += 1
                continue
            if status != 200:
                self.note_bad()
                raise RuntimeError(f"HTTP {status}")
            return raw, ctype, status
