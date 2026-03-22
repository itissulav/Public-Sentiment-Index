"""
seed_topics.py
==============
One-time local seeding script for PSI v2.

Usage
-----
Purge ALL predefined topic data first (recommended before a full re-seed):
    python seed_topics.py --purge-all

Seed ONE topic (test/sanity check):
    python seed_topics.py --topic "Donald Trump" --reddit-limit 500 --yt-limit 500

Full seed — 5 k Reddit + 5 k YouTube per topic:
    python seed_topics.py --all --reddit-limit 5000 --yt-limit 5000

Combine purge + seed in one pass:
    python seed_topics.py --all --purge --reddit-limit 5000 --yt-limit 5000

Flags
-----
--topic  NAME        Seed a specific topic by name
--all                Seed every predefined topic in the topics table
--reddit-limit  N    Max Reddit comments per topic  (default: 5000)
--yt-limit      N    Max YouTube comments per topic (default: 5000)
--purge              Wipe existing data for the given topic(s) before seeding
--purge-all          Wipe ALL predefined topic data and exit (no seeding)
--dry-run            Fetch + classify but do NOT insert to DB (for testing)
--youtube-only       Skip Reddit, only fetch YouTube
--clean-youtube      Delete existing YouTube comments before re-seeding

Prerequisites
-------------
1. Run _dev/scripts/migrate_v2.sql in Supabase SQL Editor first
2. .env must have: SUPABASE_URL, SUPABASE_SERVICE_KEY, CLIENT_ID, CLIENT_SECRET, USER_AGENT
3. Optional: GEMINI_API_KEY, YOUTUBE_API_KEY
"""

import os
import sys
import json
import time
import argparse
from datetime import date, datetime
from collections import Counter

from dotenv import load_dotenv
load_dotenv()

from supabase import create_client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def _retry(fn, max_attempts: int = 5, base_delay: int = 30, label: str = ""):
    """
    Call fn(); retry on transient 5xx / network errors with exponential back-off.
    Raises the last exception if all attempts fail.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            err = str(e)
            # Transient: Supabase 523 (origin unreachable), 502, 503, 504, connection errors
            is_transient = any(c in err for c in ("523", "502", "503", "504", "Connection", "timeout", "unreachable"))
            if is_transient and attempt < max_attempts:
                wait = base_delay * attempt
                print(f"  [retry] Transient error on attempt {attempt}/{max_attempts}"
                      f"{' (' + label + ')' if label else ''}: {err[:120]}")
                print(f"  [retry] Waiting {wait}s before retry...")
                time.sleep(wait)
            else:
                raise


# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_predefined_topics():
    res = supabase.table("topics").select("id, name, category").is_("user_id", "null").execute()
    return res.data or []


def get_or_create_topic(name: str) -> int:
    res = supabase.table("topics").select("id").eq("name", name).is_("user_id", "null").execute()
    if res.data:
        return res.data[0]["id"]
    insert = supabase.table("topics").insert({"name": name, "user_id": None}).execute()
    return insert.data[0]["id"]


def store_topic_sources(topic_id: int, sources: dict):
    rows = []
    for sub in sources.get("subreddits", []):
        rows.append({"topic_id": topic_id, "source_type": "reddit", "source_id": sub})
    if sources.get("youtube_relevant"):
        for q in sources.get("youtube_queries", []):
            rows.append({"topic_id": topic_id, "source_type": "youtube", "source_id": q})
    for kw in sources.get("news_keywords", []):
        rows.append({"topic_id": topic_id, "source_type": "news", "source_id": kw})

    for row in rows:
        try:
            supabase.table("topic_sources").upsert(row, on_conflict="topic_id,source_type,source_id").execute()
        except Exception as e:
            print(f"  [sources] Insert skipped: {e}")

    print(f"  [sources] Stored {len(rows)} source rows for topic_id={topic_id}")


# ── Purge ──────────────────────────────────────────────────────────────────────

def purge_topic_data(topic_name: str) -> dict:
    """
    Delete all comments, posts, topic_sources, and daily_snapshots for a
    predefined topic. The `topics` row itself is kept.

    Returns a dict with deletion counts.
    """
    print(f"\n  [purge] Purging data for '{topic_name}'...")

    res = supabase.table("topics").select("id").eq("name", topic_name).is_("user_id", "null").execute()
    if not res.data:
        print(f"  [purge] Topic '{topic_name}' not found in DB — nothing to purge.")
        return {}

    topic_id = res.data[0]["id"]
    counts = {}

    # 1. Find all topic_source IDs
    ts_res = supabase.table("topic_sources").select("id").eq("topic_id", topic_id).execute()
    ts_ids = [r["id"] for r in (ts_res.data or [])]

    if ts_ids:
        # 2. Find all post IDs for those sources
        post_res = supabase.table("posts").select("id").in_("topic_source_id", ts_ids).execute()
        post_ids = [r["id"] for r in (post_res.data or [])]

        if post_ids:
            # 3. Delete comments in batches (avoid URL length limits)
            batch = 200
            deleted_comments = 0
            for i in range(0, len(post_ids), batch):
                supabase.table("comments").delete().in_("post_id", post_ids[i:i + batch]).execute()
                deleted_comments += len(post_ids[i:i + batch])
            counts["comments"] = deleted_comments
            print(f"  [purge] Deleted comments for {len(post_ids)} posts ({deleted_comments} batched)")

        # 4. Delete posts
        for i in range(0, len(ts_ids), 100):
            supabase.table("posts").delete().in_("topic_source_id", ts_ids[i:i + 100]).execute()
        counts["posts"] = len(post_ids) if post_ids else 0
        print(f"  [purge] Deleted posts")

        # 5. Delete topic_sources
        supabase.table("topic_sources").delete().eq("topic_id", topic_id).execute()
        counts["topic_sources"] = len(ts_ids)
        print(f"  [purge] Deleted {len(ts_ids)} topic_source rows")
    else:
        print(f"  [purge] No topic_sources found — no posts or comments to delete")

    # 6. Delete daily_snapshots
    supabase.table("daily_snapshots").delete().eq("topic_id", topic_id).execute()
    print(f"  [purge] Deleted daily_snapshots")

    print(f"  [purge] Done for '{topic_name}' (topic_id={topic_id} kept)")
    return counts


def purge_all_predefined():
    """Purge data for every predefined topic and print a summary."""
    topics = get_predefined_topics()
    if not topics:
        print("[!] No predefined topics found in DB.")
        return

    print(f"\nPurging data for {len(topics)} predefined topics: {[t['name'] for t in topics]}")
    for t in topics:
        purge_topic_data(t["name"])

    print("\n[purge-all] Complete. Topic rows kept; all comments/posts/snapshots removed.")


# ── Seed ───────────────────────────────────────────────────────────────────────

def seed_topic(
    topic_name: str,
    reddit_limit: int = 5000,
    yt_limit: int = 5000,
    dry_run: bool = False,
    youtube_only: bool = False,
    clean_youtube: bool = False,
    purge: bool = False,
):
    print(f"\n{'='*60}")
    print(f"  SEEDING: {topic_name}")
    print(f"  Reddit limit: {reddit_limit}  |  YouTube limit: {yt_limit}{'  DRY-RUN' if dry_run else ''}")
    print(f"{'='*60}")

    # Optional purge before seeding
    if purge and not dry_run:
        purge_topic_data(topic_name)

    # Step 1: Get or create topic in DB
    topic_id = get_or_create_topic(topic_name)
    print(f"  [db] topic_id = {topic_id}")

    # Step 2: Get Gemini-recommended sources
    from app.utils.gemini_sources import get_sources_for_topic
    sources = get_sources_for_topic(topic_name)
    print(f"  [gemini] category={sources['category']}, subreddits={sources['subreddits']}")

    if not dry_run:
        store_topic_sources(topic_id, sources)

    # Update category on topic row
    if sources.get("category") and not dry_run:
        try:
            supabase.table("topics").update({"category": sources["category"]}).eq("id", topic_id).execute()
        except Exception:
            pass

    subreddits = sources.get("subreddits") or ["all"]

    # ── Reddit ──────────────────────────────────────────────────────────────────
    if not youtube_only:
        from app.utils.fetcher import get_reddit_comments
        import pandas as pd

        # Skip Reddit if enough data already exists (resume-safe)
        try:
            existing_reddit_count = (supabase.table("comments").select("id", count="exact")
                                     .eq("topic_id", topic_id).execute().count or 0)
        except Exception:
            existing_reddit_count = 0

        if existing_reddit_count >= reddit_limit and not purge:
            print(f"\n  [reddit] Already have {existing_reddit_count} comments (≥ {reddit_limit}) — skipping Reddit.")
            print(f"  [reddit] Tip: use --purge to force a fresh re-seed.")
        else:
            if existing_reddit_count > 0:
                print(f"\n  [reddit] Resuming: {existing_reddit_count} comments already in DB, fetching up to {reddit_limit}...")

            all_dfs = []
            comments_per_sub = max(reddit_limit // len(subreddits), 800)

            for sub in subreddits:
                print(f"\n  [reddit] Fetching from r/{sub} (up to {comments_per_sub} comments)...")
                try:
                    df = get_reddit_comments(
                        query=topic_name,
                        limit_posts=100,
                        max_comments=comments_per_sub,
                        subreddit_name=sub,
                        topic_name=f"{topic_name}_{sub}",
                    )
                    if not df.empty:
                        df["source_id"] = sub
                        all_dfs.append(df)
                        print(f"  [reddit] Got {len(df)} comments from r/{sub}")
                except Exception as e:
                    print(f"  [reddit] Error fetching r/{sub}: {e}")

            if not all_dfs:
                print(f"  [!] No Reddit comments fetched for '{topic_name}'. Skipping Reddit.")
            else:
                combined_df = pd.concat(all_dfs, ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=["text"])
                combined_df = combined_df.head(reddit_limit)
                print(f"\n  [combined] {len(combined_df)} unique Reddit comments")

                if dry_run:
                    print(f"  [dry-run] Would classify and insert {len(combined_df)} Reddit comments. Stopping here.")
                    print("  Sample rows:")
                    print(combined_df[["text", "score"]].head(3).to_string())
                else:
                    print(f"\n  [emotion] Classifying {len(combined_df)} comments locally...")
                    from app.utils.emotion_classifier import classify_local

                    emotion_results = classify_local(combined_df["text"].tolist())
                    label_counts = Counter(r["emotion_label"] for r in emotion_results)
                    print(f"  [emotion] Distribution: {dict(label_counts)}")

                    from app.utils.hf_analyzer import _get_or_create_topic_source, _bulk_upsert_posts
                    from collections import defaultdict

                    ts_map = {}
                    for sid in combined_df["source_id"].unique():
                        sid_str = str(sid)
                        ts_map[sid_str] = _get_or_create_topic_source(topic_id, "reddit", sid_str)

                    posts_by_ts = defaultdict(list)
                    for _, prow in combined_df[["source_id", "post_id", "post_title"]].drop_duplicates("post_id").iterrows():
                        ts_id = ts_map[str(prow["source_id"])]
                        posts_by_ts[ts_id].append({
                            "external_post_id": str(prow["post_id"]),
                            "title": str(prow.get("post_title", ""))[:500],
                        })

                    post_id_map = {}
                    for ts_id, p_rows in posts_by_ts.items():
                        post_id_map.update(_bulk_upsert_posts(ts_id, p_rows))

                    today = date.today().isoformat()
                    db_records = []
                    classified_rows = []

                    for idx, result in enumerate(emotion_results):
                        row = combined_df.iloc[idx]
                        published_at = None
                        try:
                            ts = str(row.get("timestamp", ""))
                            if ts:
                                published_at = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").isoformat()
                        except Exception:
                            pass

                        emotion_scores_dict = {
                            k: result[k] for k in ["positive", "negative", "neutral"] if k in result
                        }

                        post_fk = post_id_map.get(str(row.get("post_id", "")))
                        if post_fk is None:
                            continue

                        db_records.append({
                            "post_id":          post_fk,
                            "topic_id":         topic_id,
                            "text":             str(row.get("text", ""))[:2000],
                            "author":           str(row.get("author", "")),
                            "score":            int(row.get("score", 0)),
                            "collected_date":   today,
                            "published_at":     published_at,
                            "sentiment_label":  result["sentiment_label"],
                            "emotion_label":    result["emotion_label"],
                            "emotion_scores":   json.dumps(emotion_scores_dict),
                            "confidence_score": float(result["confidence_score"]),
                        })
                        classified_rows.append({
                            **emotion_scores_dict,
                            "sentiment_label":  result["sentiment_label"],
                            "emotion_label":    result["emotion_label"],
                            "confidence_score": float(result["confidence_score"]),
                            "score":            int(row.get("score", 0)),
                        })

                    print(f"\n  [db] Inserting {len(db_records)} Reddit records...")
                    batch_size = 50
                    inserted = 0
                    for i in range(0, len(db_records), batch_size):
                        try:
                            _retry(lambda b=db_records[i:i+batch_size]: supabase.table("comments").insert(b).execute(),
                                   label=f"reddit batch {i}")
                            inserted += len(db_records[i:i + batch_size])
                            print(f"  [db] {inserted}/{len(db_records)} inserted...")
                        except Exception as e:
                            print(f"  [db] Insert error at batch {i} (skipping): {e}")

                    from app.utils.analyzer import calculate_psi_from_df

                    df_classified = pd.DataFrame(classified_rows)
                    total = len(df_classified)
                    pos = int((df_classified["sentiment_label"] == "Positive").sum())
                    neg = int((df_classified["sentiment_label"] == "Negative").sum())
                    neu = total - pos - neg

                    emotion_cols = ["positive", "negative", "neutral"]
                    emotion_dist = {col: round(float(df_classified[col].mean()), 4)
                                    for col in emotion_cols if col in df_classified.columns}
                    dominant = max(emotion_dist, key=emotion_dist.get) if emotion_dist else "neutral"
                    psi = calculate_psi_from_df(df_classified)

                    try:
                        supabase.table("daily_snapshots").upsert({
                            "topic_id":             topic_id,
                            "snapshot_date":        today,
                            "total_comments":       total,
                            "positive_pct":         round(pos / total * 100, 2),
                            "negative_pct":         round(neg / total * 100, 2),
                            "neutral_pct":          round(neu / total * 100, 2),
                            "dominant_emotion":     dominant,
                            "emotion_distribution": emotion_dist,
                            "psi_rating":           psi,
                        }, on_conflict="topic_id,snapshot_date").execute()
                        print(f"  [snapshot] PSI={psi:+.1f} | {round(pos/total*100,1)}% pos | {round(neg/total*100,1)}% neg")
                    except Exception as e:
                        print(f"  [snapshot] Error: {e}")

                    print(f"\n  DONE (Reddit): '{topic_name}' — {inserted} comments inserted, PSI={psi:+.1f}")
    else:
        print("  [mode] --youtube-only: skipping Reddit")

    # ── YouTube ─────────────────────────────────────────────────────────────────
    if clean_youtube and not dry_run:
        print(f"\n  [clean-yt] Removing existing YouTube data for '{topic_name}'...")
        _delete_youtube_comments(topic_name)

    yt_queries = sources.get("youtube_queries") or []
    if yt_queries:
        yt_api_key = os.getenv("YOUTUBE_API_KEY")
        if yt_api_key and not dry_run:
            from app.utils.youtube_fetcher import search_videos, get_video_comments
            from app.utils.hf_analyzer import process_and_store_comments

            # Target yt_limit comments: fetch 1000 per video until we hit the target or run out of videos
            YT_COMMENTS_EACH = 1000
            YT_TARGET_VIDEOS = max(1, (yt_limit + YT_COMMENTS_EACH - 1) // YT_COMMENTS_EACH)

            topic_keywords = [w.lower() for w in topic_name.split() if len(w) >= 4]

            def _title_is_relevant(title: str) -> bool:
                t = title.lower()
                return any(kw in t for kw in topic_keywords)

            def _safe_title(t):
                return t.encode(sys.stdout.encoding or 'utf-8', errors='replace').decode(sys.stdout.encoding or 'utf-8')

            print(f"\n  [youtube] Targeting {yt_limit} comments (~{YT_TARGET_VIDEOS} videos × {YT_COMMENTS_EACH})")
            print(f"  [youtube] Searching with {len(yt_queries)} queries (title filter: {topic_keywords})...")

            candidates = {}
            for query in yt_queries:
                try:
                    for video in search_videos(query, yt_api_key, max_results=10, days_back=180):
                        vid_id = video["video_id"]
                        if vid_id not in candidates and _title_is_relevant(video["title"]):
                            candidates[vid_id] = video
                            print(f"  [youtube]   + {_safe_title(video['title'][:65])}  ({video['view_count']:,} views)")
                        elif vid_id not in candidates:
                            print(f"  [youtube]   - off-topic: {_safe_title(video['title'][:65])}")
                except Exception as e:
                    print(f"  [youtube] Search error for '{query}': {e}")

            sorted_videos = sorted(candidates.values(), key=lambda v: v.get("view_count", 0), reverse=True)
            print(f"\n  [youtube] {len(sorted_videos)} candidate videos (target: {YT_TARGET_VIDEOS} successful):")
            for i, v in enumerate(sorted_videos[:12], 1):
                print(f"    {i}. {_safe_title(v['title'][:65])}  ({v['view_count']:,} views)")

            # Detect already-inserted video IDs (resume support)
            try:
                ts_res = supabase.table("topic_sources").select("source_id") \
                    .eq("topic_id", topic_id).eq("source_type", "youtube").execute()
                already_done = {r["source_id"] for r in (ts_res.data or [])}
            except Exception:
                already_done = set()
            if already_done:
                print(f"  [youtube] Skipping {len(already_done)} already-inserted video(s): {already_done}")

            total_yt = 0
            successful_yt = 0
            for video in sorted_videos:
                if successful_yt >= YT_TARGET_VIDEOS:
                    break
                vid_id = video["video_id"]

                # Resume: skip this video if already inserted
                if vid_id in already_done:
                    print(f"  [youtube] Already inserted — skipping: {_safe_title(video['title'][:65])}")
                    successful_yt += 1  # count it toward the target so we don't over-fetch
                    continue

                print(f"\n  [youtube] Fetching {YT_COMMENTS_EACH} comments: {_safe_title(video['title'][:65])}")
                df_yt = get_video_comments(vid_id, video["title"], yt_api_key, max_comments=YT_COMMENTS_EACH)
                if not df_yt.empty:
                    print(f"  [youtube] Got {len(df_yt)} comments — classifying & storing...")
                    try:
                        _retry(
                            lambda df=df_yt, vid=vid_id: process_and_store_comments(
                                topic_name, df, user_id=None,
                                source_type="youtube", source_id=vid, mode="local",
                            ),
                            label=f"youtube {vid_id}",
                        )
                        total_yt += len(df_yt)
                        successful_yt += 1
                        print(f"  [youtube] Running total: {total_yt} comments from {successful_yt} videos")
                    except Exception as e:
                        print(f"  [youtube] FAILED after retries for {vid_id}: {e}")
                        print(f"  [youtube] Skipping this video and continuing...")
                else:
                    print(f"  [youtube] No comments (disabled/private) — trying next")

            print(f"\n  [youtube] DONE — {total_yt} new comments from {successful_yt} videos processed")
        elif not yt_api_key:
            print("\n  [youtube] YOUTUBE_API_KEY not set — skipping YouTube fetch")
    else:
        print("\n  [youtube] No YouTube queries from Gemini — skipping")

    print(f"\n  ALL DONE: '{topic_name}'")


def _delete_youtube_comments(topic_name: str) -> int:
    """Delete all YouTube comments for a topic (helper for --clean-youtube)."""
    try:
        res = supabase.table("topics").select("id").eq("name", topic_name).execute()
        if not res.data:
            return 0
        topic_id = res.data[0]["id"]

        src_res = supabase.table("topic_sources").select("id") \
            .eq("topic_id", topic_id).eq("source_type", "youtube").execute()
        if not src_res.data:
            return 0
        src_ids = [s["id"] for s in src_res.data]

        post_res = supabase.table("posts").select("id").in_("topic_source_id", src_ids).execute()
        if not post_res.data:
            return 0
        post_ids = [p["id"] for p in post_res.data]

        total_deleted = 0
        for i in range(0, len(post_ids), 100):
            supabase.table("comments").delete().in_("post_id", post_ids[i:i + 100]).execute()
            total_deleted += len(post_ids[i:i + 100])

        print(f"  [clean-yt] Deleted comments for {len(post_ids)} YouTube posts.")
        return total_deleted
    except Exception as e:
        print(f"  [clean-yt] Error: {e}")
        return 0


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PSI v2 seeder")
    parser.add_argument("--topic",         type=str,  help="Topic name to seed (wrap in quotes)")
    parser.add_argument("--all",           action="store_true", help="Seed all predefined topics")
    parser.add_argument("--reddit-limit",  dest="reddit_limit",  type=int, default=5000,
                        help="Max Reddit comments per topic (default: 5000)")
    parser.add_argument("--yt-limit",      dest="yt_limit",      type=int, default=5000,
                        help="Max YouTube comments per topic (default: 5000)")
    parser.add_argument("--purge",         action="store_true",
                        help="Wipe existing topic data before seeding")
    parser.add_argument("--purge-all",     dest="purge_all",     action="store_true",
                        help="Wipe ALL predefined topic data and exit (no seeding)")
    parser.add_argument("--dry-run",       dest="dry_run",       action="store_true",
                        help="Fetch but don't insert to DB")
    parser.add_argument("--youtube-only",  dest="youtube_only",  action="store_true",
                        help="Skip Reddit, only fetch YouTube")
    parser.add_argument("--clean-youtube", dest="clean_youtube", action="store_true",
                        help="Delete existing YouTube comments before re-seeding")
    args = parser.parse_args()

    # ── purge-all mode (data-check + wipe, then exit) ──────────────────────────
    if args.purge_all:
        purge_all_predefined()
        sys.exit(0)

    if not args.topic and not args.all:
        print("Usage:")
        print("  python seed_topics.py --purge-all")
        print("  python seed_topics.py --topic \"Donald Trump\" --reddit-limit 500 --yt-limit 500")
        print("  python seed_topics.py --all --reddit-limit 5000 --yt-limit 5000")
        print("  python seed_topics.py --all --purge --reddit-limit 5000 --yt-limit 5000")
        sys.exit(1)

    if args.topic:
        seed_topic(
            args.topic,
            reddit_limit=args.reddit_limit,
            yt_limit=args.yt_limit,
            dry_run=args.dry_run,
            youtube_only=args.youtube_only,
            clean_youtube=args.clean_youtube,
            purge=args.purge,
        )

    elif args.all:
        topics = get_predefined_topics()
        if not topics:
            print("[!] No predefined topics in DB. Did you run the migration SQL?")
            sys.exit(1)
        print(f"Found {len(topics)} predefined topics: {[t['name'] for t in topics]}")
        for t in topics:
            seed_topic(
                t["name"],
                reddit_limit=args.reddit_limit,
                yt_limit=args.yt_limit,
                dry_run=args.dry_run,
                youtube_only=args.youtube_only,
                clean_youtube=args.clean_youtube,
                purge=args.purge,
            )

    print("\nAll done!")
