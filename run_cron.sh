#!/bin/bash
# Cron wrapper for the Reddit scraper.
#
# Why this exists:
# - macOS cron runs in a bare environment; `source .venv/bin/activate` does
#   not persist across subshells, so we call the venv python binary directly.
# - The scraper writes media + the ledger into a Google Drive Desktop mount.
#   If Drive Desktop isn't running, the mount directory is missing and writes
#   would silently land on a plain local path that then disappears when Drive
#   comes back. We pre-flight the mount and abort if it's gone.
# - All output goes to cron.log so failures are debuggable after the fact.

set -euo pipefail

REPO="/Volumes/Storage/Projects/ICE/SocialCrawler"
DRIVE_ROOT="$HOME/Library/CloudStorage/GoogleDrive-spencernorris.journalism@gmail.com/My Drive"
TARGET="$DRIVE_ROOT/SocialCrawler"
LOG="$REPO/cron.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG"; }

cd "$REPO"
log "run start"

if [ ! -d "$TARGET" ]; then
  log "ABORT: Drive target missing at $TARGET (Drive Desktop not running?)"
  exit 1
fi

PYTHONPATH=src "$REPO/.venv/bin/python" -m social_crawler.cli --config "$REPO/config.json" >> "$LOG" 2>&1
log "run end (exit $?)"
