# SocialCrawler

Small, configurable Reddit scraping utility built on the public Reddit API.

## Features

- Script/app OAuth flow with environment based credentials.
- Query by free-form search strings and/or target subreddits.
- Optional media-only filter with support for image/video downloads.
- Pluggable cache storage (local filesystem or Google Cloud Storage).
- Ledger of crawled posts recorded to CSV or SQLite.

## Quick Start

1. **Create a Reddit App** (script type) at <https://www.reddit.com/prefs/apps> and grab the client id/secret.
2. **Configure credentials** via environment variables or a `.env` file:

```env
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USERNAME=reddit_username
REDDIT_PASSWORD=reddit_password
REDDIT_USER_AGENT=social-crawler/0.1 by your_username
```

3. **Install dependencies** (Python 3.10+ recommended):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

4. **Run the scraper** with desired filters:

```bash
python -m social_crawler.cli \
  --query "openai" \
  --subreddit "technology" \
  --max-posts 25 \
  --media-only \
  --download-media \
  --storage-backend local \
  --storage-path cache \
  --ledger-mode csv \
  --ledger-path data/ledger.csv
```

The command above searches `r/technology` for recent posts mentioning "openai", stores post JSON (and media files if present) under `cache/`, and writes a ledger row for each post at `data/ledger.csv`.

## Storage Backends

- **Local** (default): caches JSON and media files to a directory you control.
- **Google Cloud Storage**: pass `--storage-backend gcs --gcs-bucket your-bucket --gcs-prefix optional/prefix`. Requires `google-cloud-storage` credentials set via standard environment variables or application default credentials.

## Ledger Options

- `--ledger-mode csv --ledger-path <file>`: append-only CSV ledger.
- `--ledger-mode sqlite --ledger-path <db>`: upsert into `reddit_posts` table (primary key `post_id`).

## Notes

- Reddit rate limiting applies; consider throttling invocations or adding sleeps for large crawls.
- When `--media-only` is set, only posts with Reddit-hosted video/images or direct media links are kept.
- The scraper downloads media files only when `--download-media` is on; otherwise it just records the media URL.
- For bulk or scheduled usage, wrap the scraper in cron or a workflow manager and point ledger storage to a centralized location.
