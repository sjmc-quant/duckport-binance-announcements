# duckport-binance-announcements

Binance support-announcement ingestor for duckport-rs. It archives the announcement
catalogs (by default 48 new listings, 49 updates, 161 delistings) into one table and
keeps it current by polling the public announcement API.

## Table

`data.binance_announcements`, one row per `(catalog_id, code)`:

| Column | Type | Notes |
|---|---|---|
| `id` | BIGINT | Binance article id |
| `code` | VARCHAR | Article code, 32 hex chars |
| `catalog_id` | INTEGER | 48 / 49 / 161 |
| `title` | VARCHAR | |
| `url` | VARCHAR | `https://www.binance.com/zh-CN/support/announcement/{code}` |
| `release_time` | TIMESTAMP | UTC, from the list's `releaseDate`; the watermark time column |
| `publish_time` | TIMESTAMP | UTC, from the detail's `publishDate` |
| `seo_desc` | VARCHAR | |
| `body_json` | VARCHAR | Raw article body (AST JSON) |
| `body_text` | VARCHAR | Plain text extracted from `body_json` |
| `lang` | VARCHAR | Requested language, `zh-CN` |

There is no primary key. Writes go through `main._staging_binance_announcements`, and
rows whose `(catalog_id, code)` is already present are skipped (anti-join), so
re-running a load is safe. The table spec is registered in `data.table_specs`
(duckport-server >= 1.2.0) for `duckport doctor` / `upgrade`.

`data.watermark.duck_time` is `max(release_time)`. Announcements are irregular (gaps of
4-9 days happen), so `WATERMARK_MAX_LAG_SECONDS` defaults to 7 days.

## How it polls

Each round, for every catalog in `CATALOGS`:

1. Read the codes already stored for the catalog.
2. Page through the list API (20 per page) from the newest, until a page has no new code.
   If the catalog has no rows yet, only the first page is taken: load the history with
   `loadhist` instead of crawling it.
3. Fetch the body of each new article and write them in chunks.

Then it sleeps `POLL_INTERVAL_SECONDS`. Requests are single-threaded, at least 1.5 s
apart (`REQUEST_DELAY_SECONDS`, plus 0.3-0.8 s jitter), with a fixed user agent, no
cookies, and certificate verification.

| Failure | What happens |
|---|---|
| HTTP 403, HTML / challenge page, HTTP 429 after backoff, unexpected payload, 3 failures in a row | Stop the round, wait `CHALLENGE_COOLDOWN_SECONDS` (6 h). Never bypassed. |
| Network error or HTTP 5xx after backoff (30 / 60 / 120 s) | Stop the round, retry after `POLL_INTERVAL_SECONDS`. |
| One article's body fails | Skip it; it is still new next round and is retried. |

Bodies fetched before a stop are written first.

## Install with duckport

```bash
sudo duckport install binance-announcements    # prompts for the settings below
sudo duckport loadhist binance-announcements   # import history from JSONL_DIR
sudo duckport start binance-announcements
```

| Setting | Default | |
|---|---|---|
| `CATALOGS` | `48,49,161` | Comma-separated catalog ids |
| `REQUEST_DELAY_SECONDS` | `2` | Not below 1.5 |
| `POLL_INTERVAL_SECONDS` | `600` | |
| `CHALLENGE_COOLDOWN_SECONDS` | `21600` | |
| `PROXY_URL` | empty | HTTP proxy, empty for direct |
| `JSONL_DIR` | empty | Directory with the history jsonl files |
| `WATERMARK_MAX_LAG_SECONDS` | `604800` | Staleness threshold for `duckport status` |

They live in the instance's `config.env`; edit with `duckport config` and restart.

## History

`loadhist` imports `JSONL_DIR/binance-announcements-{catalog}.jsonl` for each catalog in
`CATALOGS` (the format written by the original `dump_binance_announcements.py`). Each
line is one article; `code` is required, and `catalog_id` falls back to the file's
catalog. Times are read from `release_ms` / `publish_ms` (epoch ms) or
`release_time` / `publish_time`. Bad lines are logged and skipped. Importing the full
archive (about 6,800 articles, 180 MB) takes a few seconds.

Without an archive, `--from-binance` crawls every catalog page by page and fetches the
bodies of codes not yet stored (several thousand requests, hours at the default delay):

```bash
sudo duckport loadhist binance-announcements --from-binance
```

Passing arguments through `duckport loadhist` needs duckport-rs with commit `f325215` or
later. `duckport loadhist` stops the instance first, because both write the same staging
table.

## Querying

```sql
SELECT release_time, catalog_id, title
FROM data.binance_announcements
WHERE catalog_id = 161 AND release_time >= now() - INTERVAL 30 DAY
ORDER BY release_time DESC;
```

## Local Development

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e . pytest
cp config.env.example config.env
```

Start duckport-rs first, then run the ingestor or loadhist:

```bash
INGESTOR_ENV_FILE=./config.env binance-announcements
INGESTOR_ENV_FILE=./config.env loadhist
```

The duckport address comes from `DUCKPORT_LISTEN_ADDR` in `/opt/duckport/server.env`
(or `~/.duckport/server.env`), so on a duckport host these commands write to that
server.

## Verification

```bash
python -m pytest -q
```

`tests/test_roundtrip.py` writes fixture rows through a real server and is skipped unless
`DUCKPORT_TEST_ADDR` is set. Point it at a throwaway duckport-server, never at the
production one:

```bash
DUCKPORT_DB_PATH=/tmp/it.db DUCKPORT_LISTEN_ADDR=127.0.0.1:50072 /opt/duckport/bin/duckport-server &
DUCKPORT_TEST_ADDR=127.0.0.1:50072 python -m pytest -q
```
