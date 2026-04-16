import pandas as pd
import re
from urllib.parse import urlparse

# ---------- CONFIG ----------
INPUT_CSV = "data/ledger.csv"
OUTPUT_CSV = "data/filtered_ice_videos_top200.csv"

# keywords signaling ICE raids/brutality/harm
KEYWORDS = [
    r"\bICE\b", r"immigration", r"deportation", r"removal\b", r"ERO\b", r"homeland security",
    r"raid", r"raided", r"raiding", r"sweep", r"checkpoint", r"detain(ed|ment)?",
    r"arrest", r"custody", r"processing center", r"detention center",
    r"battering ram", r"breach(ed)?", r"door kick", r"no[-\s]?knock",
    r"taser", r"pepper\s?spray", r"tear\s?gas", r"baton", r"beating", r"brutalit(y|ies)",
    r"violent", r"assault", r"choke", r"restraint", r"hogtie", r"kneel(ing)? on neck",
]

# video file patterns
VIDEO_EXTS = (".mp4", ".mov", ".webm", ".m4v", ".avi", ".wmv", ".mkv")

# social-first domains to prefer (raw footage)
SOCIAL_DOMAINS_PRIORITY = {
    "x.com", "twitter.com", "tiktok.com", "vt.tiktok.com", "instagram.com", "instagr.am",
    "youtube.com", "youtu.be", "facebook.com", "fb.watch", "reddit.com", "v.redd.it",
    "rumble.com", "odysee.com", "telegram.org", "t.me", "snapchat.com", "threads.net",
}

# news / aggregator domains to de-prioritize
NEWS_DOMAINS_DOWNRANK = {
    "cbsnews.com","abcnews.go.com","nbcnews.com","foxnews.com","cnn.com","nytimes.com",
    "washingtonpost.com","usatoday.com","apnews.com","reuters.com","theguardian.com",
    "bloomberg.com","yahoo.com","msn.com","newsweek.com","businessinsider.com","vice.com",
    "huffpost.com","forbes.com","latimes.com","sfchronicle.com","nypost.com","thehill.com",
    "politico.com"
}

# ---------- LOAD ----------
df = pd.read_csv(INPUT_CSV, low_memory=False)

# The scraper appends a fresh row each time it encounters a known post so that
# engagement metadata (score, num_comments) stays current. Collapse those
# snapshots to one row per post_id, preferring the most recently scraped one.
# `scraped_utc` is the run-time timestamp; fall back to `created_utc` for rows
# written before that column existed.
if "post_id" in df.columns:
    sort_keys = [c for c in ("scraped_utc", "created_utc") if c in df.columns]
    if sort_keys:
        df = df.sort_values(by=sort_keys, ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["post_id"], keep="first")

# normalize expected columns
possible_title_cols = [c for c in df.columns if c.lower() in {"title","post_title"}]
possible_url_cols   = [c for c in df.columns if c.lower() in {"url","link","permalink"}]
possible_media_cols = [c for c in df.columns if c.lower() in {"media_url","media","fallback_url","video_url"}]
possible_domain_cols= [c for c in df.columns if c.lower() in {"domain","source_domain","site"}]
possible_time_cols  = [c for c in df.columns if c.lower() in {"created_utc","created","timestamp","time"}]
possible_score_cols = [c for c in df.columns if c.lower() in {"score","ups","upvotes","num_comments","engagement"}]

def first_or_none(cols):
    return cols[0] if cols else None

col_title  = first_or_none(possible_title_cols)  or df.columns[0]
col_url    = first_or_none(possible_url_cols)
col_media  = first_or_none(possible_media_cols)
col_domain = first_or_none(possible_domain_cols)
col_time   = first_or_none(possible_time_cols)
col_score  = first_or_none(possible_score_cols)

# helper: get best URL and domain
def best_url(row):
    candidates = []
    if col_url and pd.notna(row.get(col_url, None)):   candidates.append(str(row[col_url]))
    if col_media and pd.notna(row.get(col_media, None)): candidates.append(str(row[col_media]))
    # pick first non-empty
    for u in candidates:
        u = u.strip()
        if u and (u.startswith("http://") or u.startswith("https://")):
            return u
    return candidates[0].strip() if candidates else ""

def get_domain(u):
    try:
        return urlparse(u).netloc.lower().replace("www.","")
    except:
        return ""

def has_video(u):
    if not u: return False
    lo = u.lower()
    # direct file?
    if any(lo.endswith(ext) for ext in VIDEO_EXTS):
        return True
    # common social/video hosts even if not direct file ext
    d = get_domain(u)
    if d in SOCIAL_DOMAINS_PRIORITY or d in {"v.redd.it","reddit.com","redd.it"}:
        return True
    # reddit + query often still video
    if "v.redd.it" in lo or "video" in lo:
        return True
    return False

def title_matches(t):
    if not isinstance(t, str): return False
    tlo = t.lower()
    return any(re.search(pat, tlo, flags=re.I) for pat in KEYWORDS)

# derive working columns
df["_title"] = df[col_title].astype(str).fillna("") if col_title in df else ""
df["_url"]   = df.apply(best_url, axis=1)
df["_domain"]= df["_url"].apply(get_domain)
df["_has_video"] = df["_url"].apply(has_video)
df["_title_match"]= df["_title"].apply(title_matches)

# filter to posts that look like ICE raids/brutality *and* have video
f = df[df["_title_match"] & df["_has_video"]].copy()

# de-duplicate by normalized title and URL
def normalize_text(s):
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

f["_norm_title"] = f["_title"].apply(normalize_text)
f["_norm_url"]   = f["_url"].apply(normalize_text)

# drop exact duplicates by URL first, then by title
f = f.sort_values(by=[col_time] if col_time in f else [], ascending=False, na_position='last')
f = f.drop_duplicates(subset=["_norm_url"])
f = f.drop_duplicates(subset=["_norm_title"])

# scoring: prefer social raw, penalize news, boost likely brutality words, use engagement/time if present
def score_row(row):
    score = 0.0
    d = row["_domain"]
    t = row["_title"].lower()

    # base: social raw
    if d in SOCIAL_DOMAINS_PRIORITY: score += 4
    # news penalty
    if d in NEWS_DOMAINS_DOWNRANK:   score -= 4
    # direct video ext bonus
    if any(row["_url"].lower().endswith(ext) for ext in VIDEO_EXTS): score += 2

    # brutality/harm terms heavier weight
    harm_terms = [
        "brutality","beating","beat","assault","pepper spray","tear gas","taser",
        "battering ram","broke the door","kicked the door","no-knock","blood","injured","violent","violence"
    ]
    raid_terms = ["raid","raided","sweep","checkpoint","warrant","no warrant","door","breach","ERO","detain","arrest","deportation","removal"]
    score += sum(1.5 for term in harm_terms if term in t)
    score += sum(0.8 for term in raid_terms if term in t)

    # engagement signals if available
    if col_score in row and pd.notna(row[col_score]):
        try:
            val = float(row[col_score])
            score += min(val/1000.0, 3.0)  # cap contribution
        except:
            pass

    # recentness: slight boost if timestamp present
    if col_time in row and pd.notna(row[col_time]):
        score += 0.5

    return score

f["_score"] = f.apply(score_row, axis=1)

# sort by score desc, then by time desc if exists
sort_cols = ["_score"]
if col_time in f.columns:
    sort_cols.append(col_time)
f = f.sort_values(by=sort_cols, ascending=False, na_position='last')

# keep top 200
top = f.head(200).copy()

# choose final columns to return
keep_cols = []
for c in ["_title", "_url", "_domain", "_score", col_time, col_score, col_domain, col_title, col_url, col_media]:
    if c and c in top.columns and c not in keep_cols:
        keep_cols.append(c)

# fallbacks to common fields if present
for c in ["subreddit","author","id","permalink"]:
    if c in top.columns and c not in keep_cols:
        keep_cols.append(c)

# rename pretty
rename_map = {"_title":"title","_url":"url","_domain":"domain","_score":"rank_score"}
top = top[keep_cols].rename(columns=rename_map)

top.to_csv(OUTPUT_CSV, index=False)
print(f"Saved {len(top)} rows to {OUTPUT_CSV}")
top.head(10)