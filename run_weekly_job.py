"""
run_weekly_job.py
=================
Weekly GitHub Actions job — fetches fresh Reddit + YouTube data for each
predefined topic (last 7 days) and stores classified results in the DB.

Reuses the exact same pipeline as the app's background analysis:
  - app.api.reddit.get_reddit_comments
  - app.api.youtube.search_videos / get_video_comments
  - app.api.huggingface.process_and_store_comments (mode="api")
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────

PREDEFINED_TOPICS = [
    "Donald Trump",
    "The Boys",
    "Avengers Doomsday",
    "Macbook Neo",
    "America vs Iran",
]

REDDIT_TARGET  = 500   # max comments per topic from Reddit
YOUTUBE_TARGET = 500   # max comments per topic from YouTube
DAYS_BACK      = 7     # only fetch content published in the last 7 days

# ── Supabase client ────────────────────────────────────────────────────────────

SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("CRITICAL: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.", file=sys.stderr)
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ── Helpers ────────────────────────────────────────────────────────────────────

def get_topic_id(topic_name: str) -> int | None:
    """Return the id of a predefined (user_id IS NULL) topic, or None if not found."""
    res = (
        supabase.table("topics")
        .select("id")
        .eq("name", topic_name)
        .is_("user_id", "null")
        .execute()
    )
    if res.data:
        return res.data[0]["id"]
    print(f"[weekly] Topic '{topic_name}' not found in DB — skipping.")
    return None


def get_topic_subreddits(topic_id: int) -> list[str]:
    """Return stored subreddit names for a topic. Falls back to ['all']."""
    res = (
        supabase.table("topic_sources")
        .select("source_id")
        .eq("topic_id", topic_id)
        .eq("source_type", "reddit")
        .execute()
    )
    subs = [row["source_id"] for row in res.data] if res.data else []
    return subs if subs else ["all"]


def _title_ok(title: str, topic_name: str) -> bool:
    """Loose relevance check — at least one topic word must appear in the title."""
    title_lower = title.lower()
    words = [w.lower() for w in topic_name.split() if len(w) >= 3]
    return any(w in title_lower for w in words)


# ── Per-topic pipeline ─────────────────────────────────────────────────────────

def run_topic(topic_name: str):
    from app.api.reddit import get_reddit_comments
    from app.api.huggingface import process_and_store_comments

    print(f"\n{'='*60}")
    print(f"[weekly] Starting: {topic_name}")
    print(f"{'='*60}")

    topic_id = get_topic_id(topic_name)
    if topic_id is None:
        return

    # ── Reddit ─────────────────────────────────────────────────────────────────
    subreddits = get_topic_subreddits(topic_id)
    print(f"[reddit] Subreddits: {subreddits}")

    per_sub = max(REDDIT_TARGET // len(subreddits), 100)
    all_reddit_frames = []

    for sub in subreddits:
        print(f"[reddit] Fetching from r/{sub} ...")
        try:
            df = get_reddit_comments(
                query=topic_name,
                limit_posts=50,
                max_comments=per_sub,
                subreddit_name=sub,
                topic_name=topic_name,
                days_back=DAYS_BACK,
            )
            if not df.empty:
                df["_source_id"] = sub
                all_reddit_frames.append(df)
                print(f"[reddit] r/{sub}: {len(df)} comments")
        except Exception as e:
            print(f"[reddit] r/{sub} failed: {e}")

    if all_reddit_frames:
        combined = pd.concat(all_reddit_frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=["text"]).head(REDDIT_TARGET)
        print(f"[reddit] Total after dedup: {len(combined)} comments")

        for sub_name, sub_df in combined.groupby("_source_id"):
            sub_df = sub_df.drop(columns=["_source_id"])
            process_and_store_comments(
                topic_name=topic_name,
                df=sub_df,
                user_id=None,
                source_type="reddit",
                source_id=sub_name,
                mode="api",
            )
            print(f"[reddit] Stored {len(sub_df)} comments from r/{sub_name}")
    else:
        print(f"[reddit] No comments collected for '{topic_name}'")

    # ── YouTube ────────────────────────────────────────────────────────────────
    youtube_api_key = os.getenv("YOUTUBE_API_KEY")
    if not youtube_api_key:
        print("[youtube] YOUTUBE_API_KEY not set — skipping YouTube.")
        return

    from app.api.youtube import search_videos, get_video_comments
    from app.utils.gemini_sources import get_sources_for_topic

    print(f"[youtube] Getting search queries for '{topic_name}' ...")
    sources = get_sources_for_topic(topic_name)
    yt_queries = sources.get("youtube_queries") or [f"{topic_name} review", f"{topic_name} discussion"]
    print(f"[youtube] Queries: {yt_queries}")

    # Collect candidate videos across all queries
    candidate_videos = []
    seen_video_ids = set()
    for query in yt_queries:
        videos = search_videos(query=query, api_key=youtube_api_key, max_results=5, days_back=DAYS_BACK)
        for v in videos:
            if v["video_id"] not in seen_video_ids and _title_ok(v["title"], topic_name):
                seen_video_ids.add(v["video_id"])
                candidate_videos.append(v)

    # Sort by view count descending — most-watched first
    candidate_videos.sort(key=lambda v: v.get("view_count", 0), reverse=True)
    print(f"[youtube] {len(candidate_videos)} relevant videos found")

    yt_collected = 0
    for video in candidate_videos:
        if yt_collected >= YOUTUBE_TARGET:
            break

        vid_id = video["video_id"]
        remaining = YOUTUBE_TARGET - yt_collected
        print(f"[youtube] Fetching comments for: {video['title'][:60]}")

        try:
            df_yt = get_video_comments(
                video_id=vid_id,
                api_key=youtube_api_key,
                max_comments=min(remaining, 200),
            )
            if df_yt.empty:
                continue

            process_and_store_comments(
                topic_name=topic_name,
                df=df_yt,
                user_id=None,
                source_type="youtube",
                source_id=vid_id,
                mode="api",
            )
            yt_collected += len(df_yt)
            print(f"[youtube] Stored {len(df_yt)} comments (total: {yt_collected})")
        except Exception as e:
            print(f"[youtube] Failed for video {vid_id}: {e}")

    print(f"[youtube] Total YouTube comments stored for '{topic_name}': {yt_collected}")

    # Re-compute and store pre-computed charts after data refresh
    try:
        from app.utils.chart_cache import precompute_and_store
        precompute_and_store(topic_id, topic_name=topic_name)
    except Exception as e:
        print(f"[weekly] Chart pre-compute failed for '{topic_name}' (non-fatal): {e}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from datetime import datetime
    print(f"Weekly job started at {datetime.utcnow().isoformat()}Z")
    print(f"Topics: {PREDEFINED_TOPICS}")
    print(f"Days back: {DAYS_BACK}, Reddit target: {REDDIT_TARGET}, YouTube target: {YOUTUBE_TARGET}")

    failed = []
    for topic in PREDEFINED_TOPICS:
        try:
            run_topic(topic)
        except Exception as exc:
            print(f"[weekly] ERROR processing '{topic}': {exc}", file=sys.stderr)
            failed.append(topic)

    print(f"\nWeekly job finished at {datetime.utcnow().isoformat()}Z")
    if failed:
        print(f"Failed topics: {failed}", file=sys.stderr)
        sys.exit(1)
    else:
        print("All topics completed successfully.")
