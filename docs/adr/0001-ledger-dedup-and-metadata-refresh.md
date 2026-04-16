# ADR-0001: Ledger deduplication and metadata refresh

## Status
Accepted

## Context
The scraper was writing duplicate records at three layers:

1. **Within a run**: `RedditClient.iter_posts` emits one result per
   `(subreddit × query)` pair. A post matching multiple queries was therefore
   piped through the full pipeline multiple times, producing duplicate
   ledger rows and (for known posts) redundant writes to the JSON blob.
2. **Across runs**: the CSV ledger was append-only with no `post_id` check,
   so each daily run re-appended every post that still fell inside the
   configured `time_filter` window.
3. **JSON cache**: `_cache_post_json` unconditionally rewrote the blob on
   every hit, even when the file already existed (unlike `_cache_media`,
   which short-circuits on `storage.exists`).

Downstream, `ledger_filter.py` ranks posts partly by engagement (`score`,
`num_comments`). Engagement numbers drift over time — a post that gains
comments overnight should rank higher tomorrow than it did today. A pure
"skip known posts" approach would have fixed the duplicate problem but
frozen engagement signals at first-sight.

## Options considered
- **A. Switch `ledger.mode` to `sqlite`.** Gets `post_id` upsert for free
  via the primary key, but breaks `ledger_filter.py` (reads CSV) and
  changes the operational shape of the project.
- **B. Within-run seen-set + storage-level `exists()` checks, skip
  known posts entirely.** Cheapest fix. Loses engagement freshness.
- **C. Within-run seen-set + `exists()` checks + known posts still get
  a fresh ledger row with updated metadata, downloads skipped.**
  Preserves CSV, fixes the duplication, keeps engagement current.

## Decision
Option C. Specifically:

- `RedditScraper.run()` loads all known `post_id`s from the ledger at
  startup and maintains a `seen_this_run` set.
- A post already present in either set is skipped from the download
  path (JSON + media). It still produces a ledger row.
- `_cache_post_json` now honors `storage.exists()` — symmetric with
  `_cache_media`.
- `LedgerEntry` / `Ledger.FIELDNAMES` gain three columns: `scraped_utc`,
  `score`, `num_comments`. New columns are appended at the end so older
  CSVs migrate by adding trailing empty fields rather than re-ordering.
- `Ledger._init_csv` detects a legacy header and rewrites the file
  in-place, padding existing rows with empty values for the new columns.
- `ledger_filter.py` dedups by `post_id` keeping the row with the latest
  `scraped_utc` (falling back to `created_utc` for legacy rows) before
  running its URL/title dedup.

## Consequences
- Idempotent JSON / media writes: one blob per post, forever.
- Ledger row count now grows ~once per post per run (snapshot history),
  rather than once per `(query × subreddit)` match per run. Disk growth
  is bounded by unique-post-count × runs, not by query fanout.
- Engagement metadata remains current — the latest row for any given
  `post_id` reflects that run's `score` / `num_comments`.
- `ledger_filter.py` depends on the new `post_id` dedup step. Any future
  downstream consumer must either apply the same dedup or accept that
  a given `post_id` may appear in multiple rows.
- One-time migration the first time the new code runs against an
  existing CSV. Cheap (single read + rewrite) but not atomic — a crash
  mid-migration would leave a truncated file. Mitigated by the fact
  that the ledger lives in a synced Drive folder with version history.

## Notes
- Implementation: `src/social_crawler/ledger.py`, `src/social_crawler/scraper.py`,
  `ledger_filter.py`.
- Tests: `tests/test_ledger.py` (migration + `load_known_post_ids`),
  `tests/test_scraper.py` (within-run dedup + cross-run refresh).
