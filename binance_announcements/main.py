#!/usr/bin/env python3
import os
import signal
import sys

os.environ["TZ"] = "UTC"

from binance_announcements.config import (  # noqa: E402
    CATALOGS,
    DUCKPORT_ADDR,
    DUCKPORT_SCHEMA,
    PROXY_URL,
    setup_logging,
)
from binance_announcements.data_jobs import AnnouncementJobs  # noqa: E402
from binance_announcements.duckport_client import DuckportClient  # noqa: E402

def main() -> None:
    setup_logging()
    import logging

    logger = logging.getLogger(__name__)
    logger.info(
        "Binance announcements starting catalogs=%s proxy=%s",
        ",".join(str(item) for item in CATALOGS),
        "on" if PROXY_URL else "off",
    )

    client = DuckportClient(addr=DUCKPORT_ADDR, schema=DUCKPORT_SCHEMA)
    jobs: AnnouncementJobs | None = None

    def signal_handler(signum, frame):
        logger.info("Received exit signal, shutting down...")
        if jobs is not None:
            jobs.request_stop()
        client.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        info = client.ping()
    except Exception as exc:
        logger.error("Cannot reach duckport-rs at %s: %s", DUCKPORT_ADDR, exc)
        client.close()
        sys.exit(1)
    logger.info("duckport-rs server: %s", info)

    client.init_schema()
    jobs = AnnouncementJobs(client)

    try:
        signal.pause()
    except AttributeError:
        import threading

        threading.Event().wait()


if __name__ == "__main__":
    main()
