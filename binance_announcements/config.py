"""Environment-driven configuration for binance-announcements."""

from __future__ import annotations

import logging
import os
import pathlib

from dotenv import load_dotenv

from binance_announcements.fetch import MIN_REQUEST_DELAY_SECONDS, validate_request_delay

_env_file = os.getenv("INGESTOR_ENV_FILE", "config.env")
load_dotenv(_env_file, override=False)

_server_env = os.getenv(
    "DUCKPORT_SERVER_ENV",
    str(pathlib.Path.home() / ".duckport" / "server.env"),
)
load_dotenv(_server_env, override=False)
load_dotenv("/opt/duckport/server.env", override=False)


def _derive_duckport_addr() -> str:
    raw = os.getenv("DUCKPORT_LISTEN_ADDR", "0.0.0.0:50051")
    host, _, port = raw.rpartition(":")
    host = "localhost" if host in ("0.0.0.0", "", "*") else host
    return f"{host}:{port}"


def parse_catalogs(raw: str) -> tuple[int, ...]:
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    if not out:
        raise ValueError("CATALOGS 至少一个 catalog id")
    return tuple(out)


def _int_env(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


DUCKPORT_ADDR = _derive_duckport_addr()
DUCKPORT_SCHEMA = "data"
TABLE_NAME = "binance_announcements"

CATALOGS = parse_catalogs(os.getenv("CATALOGS", "48,49,161"))
REQUEST_DELAY_SECONDS = validate_request_delay(
    float(os.getenv("REQUEST_DELAY_SECONDS", "2"))
)
POLL_INTERVAL_SECONDS = _int_env("POLL_INTERVAL_SECONDS", 600)
CHALLENGE_COOLDOWN_SECONDS = _int_env("CHALLENGE_COOLDOWN_SECONDS", 21600)
PROXY_URL = os.getenv("PROXY_URL", "")
JSONL_DIR = os.getenv("JSONL_DIR", "")
WATERMARK_MAX_LAG_SECONDS = _int_env("WATERMARK_MAX_LAG_SECONDS", 604800)

APPEND_CHUNK_TARGET_BYTES = 32 * 1024 * 1024
DUCKPORT_APPEND_MAX_MESSAGE_BYTES = int(
    os.getenv("DUCKPORT_APPEND_MAX_MESSAGE_BYTES", str(48 * 1024 * 1024))
)

if POLL_INTERVAL_SECONDS < 0 or CHALLENGE_COOLDOWN_SECONDS < 0:
    raise ValueError("POLL_INTERVAL_SECONDS 和 CHALLENGE_COOLDOWN_SECONDS 不能为负")

# Re-exported so callers can mention the floor next to the configured delay.
__all__ = [
    "APPEND_CHUNK_TARGET_BYTES",
    "CATALOGS",
    "CHALLENGE_COOLDOWN_SECONDS",
    "DUCKPORT_ADDR",
    "DUCKPORT_APPEND_MAX_MESSAGE_BYTES",
    "DUCKPORT_SCHEMA",
    "JSONL_DIR",
    "MIN_REQUEST_DELAY_SECONDS",
    "POLL_INTERVAL_SECONDS",
    "PROXY_URL",
    "REQUEST_DELAY_SECONDS",
    "TABLE_NAME",
    "WATERMARK_MAX_LAG_SECONDS",
]
