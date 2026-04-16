# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project purpose

Daily crawler that searches configured subreddits for posts matching ICE / DHS / immigration-enforcement keywords, caches the raw Reddit JSON and any media, and records each post to a ledger. Downstream analysis (`ledger_filter.py`) reads the ledger to rank social-first video footage.

Reddit credentials are loaded from `.env` via `pydantic-settings` (`RedditCredentials` in `src/social_crawler/config.py`) — never hardcode them.

## Commands

The project uses a local virtualenv at `.venv/`. Bare command names (`pytest`, `python`) are expected to resolve to that venv (wired via `.claude/settings.local.json` `env.PATH` — don't re-add `source .venv/bin/activate` shims; Bash calls run in fresh shells and won't persist activation).

```bash
# Install / refresh
pip install -r requirements.txt

# Run the scraper (config-file form — matches cron usage)
python -m social_crawler.cli --config config.json

# Run the scraper (flag form)
python -m social_crawler.cli --query ICE --subreddit nyc --media-only --download-media

# Tests
pytest                                # full suite
pytest tests/test_scraper.py          # one file
pytest tests/test_scraper.py::test_scraper_media_only_filters_and_records  # one test
pytest -k storage                     # by keyword

# Post-hoc ranking of the ledger into a top-200 video list
python ledger_filter.py
```

There is no linter or formatter wired in — don't invent one.

## Architecture

Layered around a `RedditScraper` (`src/social_crawler/scraper.py`) that composes three pluggable pieces, all configured via `ScraperConfig`:

1. **`RedditClient`** (`reddit_client.py`) — thin PRAW wrapper. Yields `RedditPost` dataclasses. Two code paths:
   - If `queries` is non-empty → `subreddit.search(query, sort, time_filter, limit)` per (subreddit × query) pair (subreddit defaults to `all` when no subreddits given).
   - If `queries` is empty → listing mode (`.new` / `.top` / `.hot`) per subreddit.
   `_extract_media_url` prefers Reddit-hosted video `fallback_url`, then `preview.images[0].source.url`, then falls back to the submission URL if it ends in a known media extension.

2. **`StorageBackend`** (`storage.py`) — abstract interface with `save_json`, `save_bytes`, `exists`. Two implementations: `LocalStorage` (writes under `storage.local_path`) and `GCSStorage` (uploads to a GCS bucket under `gcs_prefix`). `build_storage_backend` is the factory; extend it when adding a backend. Path layout produced by the scraper is `json/<subreddit>/<post_id>.json` and `media/<subreddit>/<post_id><ext>`.

3. **`Ledger`** (`ledger.py`) — append-only CSV or upsert-by-`post_id` SQLite. Schema lives in `Ledger.FIELDNAMES`; both backends mirror the same columns. `scraped_utc`, `score`, and `num_comments` are written on every row so engagement metadata can be refreshed for posts we've already seen (see `docs/adr/0001-ledger-dedup-and-metadata-refresh.md`). The scraper loads `load_known_post_ids()` at run start and, for any post already in the ledger, skips the JSON/media downloads but still appends a fresh row — so a given `post_id` can legitimately appear in multiple CSV rows over time, distinguished by `scraped_utc`. `ledger_filter.py` collapses these to the latest snapshot before ranking. Older CSVs written under the pre-`scraped_utc` schema are migrated in place on first load.

`ScraperConfig.ensure_paths()` is responsible for mkdir-ing local storage + ledger parent dirs; it's called in `RedditScraper.__init__` before anything else runs.

## Config file format

`config.json` mirrors the Pydantic models exactly — `queries`, `storage`, `ledger` are the three top-level keys. `cli.py::load_config_from_file` uses `ScraperConfig.model_validate`, so new fields must be added to the model (not just the JSON) to take effect. The committed `config.json` at the repo root is the production crawl config; edit it deliberately.

## Downstream: `ledger_filter.py`

Standalone script at the repo root (not part of the package). Reads `data/ledger.csv`, filters for ICE/raid/brutality keywords + likely-video URLs, de-duplicates, scores (social-raw boost, news-domain penalty, engagement), and writes `data/filtered_ice_videos_top200.csv`. Column-name detection is fuzzy — if you rename ledger fields, update the `possible_*_cols` lists at the top of the file.

## Test style

`tests/` uses pytest with dependency injection via constructor args (`session=`, `reddit=`, `client=`) and `monkeypatch` for swapping `praw` / `gcs` at the module level. When adding a new storage backend, follow `test_gcs_storage_uses_blob_operations` — monkeypatch the third-party client symbol, build the backend with a dummy client, assert on a plain dict side-effect store.

## Conventions worth preserving

- Optional third-party imports (`praw`, `google.cloud.storage`) are guarded with `try/except ImportError` and surface a runtime error only when the feature is actually used. Keep this pattern when adding new backends — don't make optional deps import-time required.
- Dataclasses for value types (`RedditPost`, `LedgerEntry`), Pydantic for config. Don't mix.
- Relative paths in `config.json` resolve against the current working directory — cron jobs must `cd` into the repo root (or the config must use absolute paths, as the committed one does).
