import json
import threading
import urllib.error
import urllib.parse

import pandas as pd
import pytest

from binance_announcements.config import (
    CHALLENGE_COOLDOWN_SECONDS,
    POLL_INTERVAL_SECONDS,
    parse_catalogs,
)
from binance_announcements.data_jobs import AnnouncementJobs
from binance_announcements import fetch
from binance_announcements.fetch import (
    Client,
    HardStop,
    Transient,
    collect_backfill_from_list,
    collect_new_from_list,
    extract_body_text,
    iter_jsonl_frames,
    looks_like_challenge,
    ms_to_naive_utc,
    page_frontier,
    parse_jsonl_line,
    parse_list_payload,
    records_to_frame,
    validate_request_delay,
)


def test_validate_request_delay_floor():
    with pytest.raises(ValueError):
        validate_request_delay(1.4)
    assert validate_request_delay(1.5) == 1.5
    assert validate_request_delay(2) == 2


def test_parse_catalogs():
    assert parse_catalogs("48, 49,161") == (48, 49, 161)
    with pytest.raises(ValueError):
        parse_catalogs(" , ")


def test_extract_body_text_walks_ast_and_plain_strings():
    ast = {"text": "ab", "child": [{"text": "c"}, {"node": "x"}]}
    assert extract_body_text(json.dumps(ast)) == "abc"
    assert extract_body_text("不是 JSON") == "不是 JSON"
    assert extract_body_text("") == ""


def test_looks_like_challenge():
    assert looks_like_challenge(b"{}", "text/html; charset=utf-8")
    assert looks_like_challenge(b"<!DOCTYPE html>", "application/json")
    assert looks_like_challenge(b'{"message":"Just a moment"}', "application/json")
    assert not looks_like_challenge(b'{"code":"000000"}', "application/json")


def test_parse_list_payload_rejects_bad_business_code():
    with pytest.raises(HardStop):
        parse_list_payload({"code": "100001", "data": {}})
    total, articles = parse_list_payload(
        {
            "code": "000000",
            "data": {
                "catalogs": [
                    {
                        "total": 2,
                        "articles": [
                            {"code": "abc", "title": "t"},
                            {"code": "", "title": "drop"},
                            "nope",
                        ],
                    }
                ]
            },
        }
    )
    assert total == 2
    assert [item["code"] for item in articles] == ["abc"]


def test_ms_to_utc_and_frame_strings():
    millis = 1788252309916
    expected = pd.to_datetime(millis, unit="ms", utc=True).tz_localize(None)
    assert ms_to_naive_utc(millis) == expected
    assert pd.isna(ms_to_naive_utc(None))
    frame = records_to_frame(
        [
            {
                "id": 7,
                "code": "abc",
                "catalog_id": 48,
                "title": None,
                "url": "https://example.test/abc",
                "release_ms": millis,
                "publish_ms": None,
                "seo_desc": "",
                "body_json": {"node": "root"},
                "body_text": "正文",
                "lang_requested": "zh-CN",
            },
            {"code": "", "catalog_id": 48},
        ]
    )
    assert len(frame) == 1
    assert frame.loc[0, "release_time"] == expected
    assert pd.isna(frame.loc[0, "publish_time"])
    assert frame.loc[0, "title"] == ""
    assert frame.loc[0, "body_json"] == '{"node": "root"}'
    assert frame.loc[0, "lang"] == "zh-CN"


def test_page_frontier_stops_only_on_page_without_new_and_empty_catalog():
    articles = [{"code": "new"}, {"code": "old"}]
    fresh, stop = page_frontier(articles, {"old"}, catalog_empty=False)
    assert [item["code"] for item in fresh] == ["new"]
    assert stop is False

    fresh, stop = page_frontier([{"code": "old"}], {"old"}, catalog_empty=False)
    assert fresh == []
    assert stop is True

    fresh, stop = page_frontier([{"code": "a"}, {"code": "b"}], set(), catalog_empty=False)
    assert len(fresh) == 2
    assert stop is False

    fresh, stop = page_frontier([{"code": "a"}], set(), catalog_empty=True)
    assert [item["code"] for item in fresh] == ["a"]
    assert stop is True

    fresh, stop = page_frontier([], {"old"}, catalog_empty=False)
    assert fresh == []
    assert stop is True


class _FakeHttp:
    def __init__(self, pages: list[list[dict]]):
        self.pages = pages
        self.stop_after = False
        self.calls = 0

    def get_json(self, url: str) -> dict:
        self.calls += 1
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        page = int(query["pageNo"][0])
        articles = self.pages[page - 1] if page <= len(self.pages) else []
        return {
            "code": "000000",
            "data": {"catalogs": [{"total": 100, "articles": articles}]},
        }


def test_collect_empty_catalog_requests_only_the_first_page():
    http = _FakeHttp(
        [
            [{"code": "a"}, {"code": "b"}],
            [{"code": "c"}],
        ]
    )
    total, pending = collect_new_from_list(http, 48, set(), catalog_empty=True)
    assert total == 100
    assert http.calls == 1
    assert [item["code"] for item in pending] == ["a", "b"]


def test_backfill_keeps_paging_after_known_codes():
    http = _FakeHttp(
        [
            [{"code": "old"}, {"code": "n1"}],
            [{"code": "n2"}],
            [{"code": "tail"}],
        ]
    )
    original = http.get_json

    def limited(url: str) -> dict:
        payload = original(url)
        payload["data"]["catalogs"][0]["total"] = 60
        return payload

    http.get_json = limited
    total, pending = collect_backfill_from_list(http, 49, {"old"})
    assert total == 60
    assert http.calls == 3
    assert [item["code"] for item in pending] == ["n1", "n2", "tail"]


def test_backfill_stops_at_reported_total():
    http = _FakeHttp(
        [
            [{"code": "n1"}],
            [{"code": "n2"}],
            [{"code": "should-not-fetch"}],
        ]
    )
    original = http.get_json

    def limited(url: str) -> dict:
        payload = original(url)
        payload["data"]["catalogs"][0]["total"] = 40
        return payload

    http.get_json = limited
    _, pending = collect_backfill_from_list(http, 48, set())
    assert http.calls == 2
    assert [item["code"] for item in pending] == ["n1", "n2"]


def test_collect_stops_at_first_page_without_new():
    http = _FakeHttp(
        [
            [{"code": "n1"}, {"code": "n2"}],
            [{"code": "n3"}, {"code": "old"}],
            [{"code": "old2"}, {"code": "old3"}],
            [{"code": "should-not-fetch"}],
        ]
    )
    _, pending = collect_new_from_list(http, 49, {"old", "old2", "old3"}, catalog_empty=False)
    assert http.calls == 3
    assert [item["code"] for item in pending] == ["n1", "n2", "n3"]


def test_collect_keeps_paging_past_mixed_page():
    # 列表不按时间排序或上轮正文没拉到时，新公告可能排在已入库的公告后面一页。
    http = _FakeHttp(
        [
            [{"code": "n1"}, {"code": "old1"}],
            [{"code": "old2"}, {"code": "n2"}],
            [{"code": "old3"}],
            [{"code": "should-not-fetch"}],
        ]
    )
    _, pending = collect_new_from_list(
        http, 48, {"old1", "old2", "old3"}, catalog_empty=False
    )
    assert http.calls == 3
    assert [item["code"] for item in pending] == ["n1", "n2"]


def test_jsonl_skips_bad_lines(tmp_path):
    path = tmp_path / "binance-announcements-48.jsonl"
    good = {
        "id": 1,
        "code": "abc",
        "title": "t",
        "catalog_id": 48,
        "release_ms": 1700000000000,
        "body_text": "x",
    }
    path.write_text(
        "\n".join(["not json", '{"code":""}', json.dumps(good), '{"code":"no-catalog"}']) + "\n",
        encoding="utf-8",
    )
    assert parse_jsonl_line("not json", 48) is None
    assert parse_jsonl_line('{"code":""}', 48) is None
    frames = list(iter_jsonl_frames(path, 48, target_bytes=10_000_000))
    assert len(frames) == 1
    assert frames[0]["code"].tolist() == ["abc", "no-catalog"]
    assert frames[0]["catalog_id"].tolist() == [48, 48]


def test_jsonl_chunks_when_over_target(tmp_path):
    path = tmp_path / "binance-announcements-49.jsonl"
    lines = []
    for index in range(4):
        lines.append(
            json.dumps(
                {
                    "code": f"c{index}",
                    "catalog_id": 49,
                    "body_text": "y" * 100,
                    "release_ms": 1700000000000 + index,
                }
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    frames = list(iter_jsonl_frames(path, 49, target_bytes=200))
    assert len(frames) > 1
    assert sum(len(frame) for frame in frames) == 4


def test_hardstop_waits_for_cooldown_instead_of_exiting():
    waited: list[float | None] = []

    class _Stop(threading.Event):
        def wait(self, timeout=None):
            waited.append(timeout)
            return True

    jobs = AnnouncementJobs.__new__(AnnouncementJobs)
    jobs._stop = _Stop()
    jobs._http = None

    def poll_once(http):
        raise HardStop("HTTP 403，已停止。不要绕过验证。")

    jobs.poll_once = poll_once
    jobs._loop()
    assert waited == [CHALLENGE_COOLDOWN_SECONDS]


class _RaisingOpener:
    def __init__(self, exc: Exception):
        self.exc = exc
        self.calls = 0

    def open(self, req, timeout=None):
        self.calls += 1
        raise self.exc


def _client_with(monkeypatch, exc: Exception) -> tuple[Client, _RaisingOpener]:
    monkeypatch.setattr(fetch.time, "sleep", lambda _seconds: None)
    http = Client(delay=2.0)
    opener = _RaisingOpener(exc)
    http._opener = opener
    return http, opener


def test_network_error_exhaustion_is_transient(monkeypatch):
    http, opener = _client_with(monkeypatch, urllib.error.URLError("connection refused"))
    with pytest.raises(Transient):
        http.get_json("https://example.invalid/")
    assert opener.calls == fetch.MAX_BACKOFF_TRIES + 1


def test_5xx_exhaustion_is_transient(monkeypatch):
    exc = urllib.error.HTTPError("https://example.invalid/", 503, "busy", None, None)
    http, _ = _client_with(monkeypatch, exc)
    with pytest.raises(Transient):
        http.get_json("https://example.invalid/")


@pytest.mark.parametrize("status", [403, 429])
def test_403_and_429_are_hard_stops(monkeypatch, status):
    exc = urllib.error.HTTPError("https://example.invalid/", status, "no", None, None)
    http, _ = _client_with(monkeypatch, exc)
    with pytest.raises(HardStop) as info:
        http.get_json("https://example.invalid/")
    assert not isinstance(info.value, Transient)


def test_transient_waits_for_poll_interval_not_cooldown():
    waited: list[float | None] = []

    class _Stop(threading.Event):
        def wait(self, timeout=None):
            waited.append(timeout)
            return True

    jobs = AnnouncementJobs.__new__(AnnouncementJobs)
    jobs._stop = _Stop()
    jobs._http = None

    def poll_once(http):
        raise Transient("网络错误重试耗尽：connection refused")

    jobs.poll_once = poll_once
    jobs._loop()
    assert waited == [POLL_INTERVAL_SECONDS]


def test_poll_once_resets_consecutive_bad():
    class _Client:
        def read_known_codes(self):
            return {}

        def sync_duck_time(self):
            pass

    class _Http:
        stop_after = True
        consecutive_bad = fetch.MAX_CONSECUTIVE_BAD

    jobs = AnnouncementJobs.__new__(AnnouncementJobs)
    jobs.client = _Client()
    jobs._stop = threading.Event()
    http = _Http()
    jobs.poll_once(http)
    assert http.consecutive_bad == 0
