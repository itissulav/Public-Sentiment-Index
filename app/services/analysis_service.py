"""
app/services/analysis_service.py
=================================
Orchestrates the background scrape → classify → store pipeline
for user-submitted custom topics.
"""

import os
import pandas as pd
from app.services.supabase_client import admin_supabase


def purge_user_topic(user_id, topic_name):
    """Delete every DB trace of a user's custom topic (comments, snapshots, sources, topic row)."""
    try:
        res = admin_supabase.table("topics").select("id") \
            .eq("name", topic_name).eq("user_id", user_id).execute()
        if not res.data:
            return
        tid = res.data[0]["id"]
        print(f"[bg] Purging topic '{topic_name}' (id={tid}) from database")
        admin_supabase.table("comments").delete().eq("topic_id", tid).execute()
        admin_supabase.table("daily_snapshots").delete().eq("topic_id", tid).execute()
        admin_supabase.table("topic_sources").delete().eq("topic_id", tid).execute()
        admin_supabase.table("topics").delete().eq("id", tid).execute()
        print(f"[bg] Purge complete for '{topic_name}'")
    except Exception as e:
        print(f"[bg] Purge error: {e}")


def run_background_analysis(app, user_id, topic_name):
    """Scrape → analyse → store for a user's custom topic."""
    # Hard limits for custom topics — keeps HF API load manageable
    REDDIT_CAP       = 1500
    COMMENTS_PER_SUB = 300
    MAX_SUBREDDITS   = 5
    YT_CAP           = 1500
    YT_PER_VIDEO     = 300
    MAX_YT_VIDEOS    = 5

    with app.app_context():
        try:
            import traceback
            from app.utils import job_tracker
            from app.api.reddit import get_reddit_comments
            from app.api.huggingface import process_and_store_comments
            from app.utils.gemini_sources import get_sources_for_topic

            print(f"[bg] Starting analysis for user={user_id} topic='{topic_name}'")

            # Step 0: Purge any existing data for this topic before fresh run
            purge_user_topic(user_id, topic_name)

            # Step 1: Gemini source discovery
            sources = get_sources_for_topic(topic_name)
            subreddits = (sources.get("subreddits") or ["all"])[:MAX_SUBREDDITS]
            print(f"[bg] Gemini sources: category={sources.get('category')}, subreddits={subreddits}")

            # Step 2: Fetch Reddit comments
            job_tracker.set_step(user_id, topic_name, "fetching")
            all_dfs = []
            for sub in subreddits:
                try:
                    df_sub = get_reddit_comments(
                        topic_name, limit_posts=50,
                        max_comments=COMMENTS_PER_SUB,
                        subreddit_name=sub, topic_name=f"{topic_name}_{sub}",
                    )
                    if not df_sub.empty:
                        df_sub["source_id"] = sub
                        all_dfs.append(df_sub)
                        print(f"[bg] Got {len(df_sub)} comments from r/{sub}")
                except Exception as e:
                    print(f"[bg] Error fetching r/{sub}: {e}")

            if not all_dfs:
                print(f"[bg] No Reddit data found for '{topic_name}'")
                job_tracker.mark_failed(user_id, topic_name)
                return

            df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=["text"]).head(REDDIT_CAP)
            print(f"[bg] Combined: {len(df)} unique Reddit comments")

            # Step 3: Classify via HF API + store in DB
            job_tracker.set_step(user_id, topic_name, "classifying")
            process_and_store_comments(topic_name, df, user_id, source_type="reddit", mode="api")

            # Step 3b: YouTube — cap at YT_PER_VIDEO per video, MAX_YT_VIDEOS videos
            yt_api_key = os.getenv("YOUTUBE_API_KEY")
            yt_source_rows = []
            yt_queries = sources.get("youtube_queries") or []
            if yt_queries and yt_api_key:
                from app.api.youtube import search_videos, get_video_comments
                topic_keywords = [w.lower() for w in topic_name.split() if len(w) >= 4]

                def _title_ok(title):
                    t = title.lower()
                    return any(kw in t for kw in topic_keywords)

                candidates = {}
                for query in yt_queries:
                    try:
                        for video in search_videos(query, yt_api_key, max_results=5, days_back=90):
                            vid_id = video["video_id"]
                            if vid_id not in candidates and _title_ok(video["title"]):
                                candidates[vid_id] = video
                    except Exception as e:
                        print(f"[bg] YouTube search error for '{query}': {e}")

                sorted_videos = sorted(candidates.values(), key=lambda v: v.get("view_count", 0), reverse=True)
                successful_yt = 0
                yt_collected  = 0
                for video in sorted_videos:
                    if successful_yt >= MAX_YT_VIDEOS or yt_collected >= YT_CAP:
                        break
                    vid_id = video["video_id"]
                    try:
                        per_video = min(YT_PER_VIDEO, YT_CAP - yt_collected)
                        print(f"[bg] Fetching YouTube comments: {video['title'][:60]}")
                        df_yt = get_video_comments(vid_id, video["title"], yt_api_key, max_comments=per_video)
                        if not df_yt.empty:
                            process_and_store_comments(
                                topic_name, df_yt, user_id,
                                source_type="youtube", source_id=vid_id, mode="api"
                            )
                            yt_source_rows.append(vid_id)
                            successful_yt += 1
                            yt_collected  += len(df_yt)
                        else:
                            print(f"[bg] No comments (disabled/private) — skipping, trying next")
                    except Exception as e:
                        print(f"[bg] YouTube fetch error for video '{vid_id}': {e}")

            # Step 4: Store Gemini-discovered sources and update category
            job_tracker.set_step(user_id, topic_name, "storing")
            try:
                topic_res = admin_supabase.table("topics").select("id") \
                    .eq("name", topic_name).eq("user_id", user_id).execute()
                if topic_res.data:
                    topic_id = topic_res.data[0]["id"]
                    source_rows = [
                        {"topic_id": topic_id, "source_type": "reddit", "source_id": sub}
                        for sub in subreddits
                    ] + [
                        {"topic_id": topic_id, "source_type": "news", "source_id": kw}
                        for kw in sources.get("news_keywords", [])
                    ] + [
                        {"topic_id": topic_id, "source_type": "youtube", "source_id": vid_id}
                        for vid_id in yt_source_rows
                    ]
                    for row in source_rows:
                        try:
                            admin_supabase.table("topic_sources").upsert(
                                row, on_conflict="topic_id,source_type,source_id"
                            ).execute()
                        except Exception:
                            pass
                    if sources.get("category"):
                        admin_supabase.table("topics").update(
                            {"category": sources["category"]}
                        ).eq("id", topic_id).execute()
            except Exception as e:
                print(f"[bg] Source storage error (non-fatal): {e}")

            job_tracker.mark_complete(user_id, topic_name)
            print(f"[bg] Analysis complete for '{topic_name}'")

        except Exception as e:
            import traceback
            print(f"[bg] Analysis failed for '{topic_name}': {e}")
            traceback.print_exc()
            try:
                from app.utils import job_tracker
                job_tracker.mark_failed(user_id, topic_name)
            except Exception:
                pass
            purge_user_topic(user_id, topic_name)
