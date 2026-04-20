"""
app/services/analysis_service.py
=================================
Orchestrates the background scrape → classify → store pipeline
for user-submitted custom topics.
"""

import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.services.supabase_client import admin_supabase
from app.utils.classifier_mode import get_classifier_mode


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


# Hard limits for custom topics — keeps HF API load manageable
REDDIT_CAP       = 1500
COMMENTS_PER_SUB = 300
MAX_SUBREDDITS   = 5
YT_CAP           = 1500
YT_PER_VIDEO     = 300
MAX_YT_VIDEOS    = 5


def purge_predefined_topic_data(topic_name):
    """Delete comments / posts / snapshots / topic_sources for a predefined topic.
    The topic row itself is kept so the admin's entry persists."""
    try:
        res = admin_supabase.table("topics").select("id") \
            .eq("name", topic_name).is_("user_id", "null").execute()
        if not res.data:
            return
        tid = res.data[0]["id"]
        print(f"[bg] Purging predefined topic '{topic_name}' (id={tid}) data (row kept)")
        admin_supabase.table("comments").delete().eq("topic_id", tid).execute()
        admin_supabase.table("daily_snapshots").delete().eq("topic_id", tid).execute()
        admin_supabase.table("topic_sources").delete().eq("topic_id", tid).execute()
    except Exception as e:
        print(f"[bg] Predefined purge error: {e}")


def run_predefined_analysis(app, topic_name, purge_existing: bool = False):
    """
    Full 5k Reddit + 5k YouTube scrape for a predefined topic (user_id=None).
    Uses local model when CLASSIFIER_MODE=local (see get_classifier_mode).

    Set purge_existing=True to wipe prior comments/snapshots before the run —
    used by the admin "Analyse" rerun button. For a first-time add (the row was
    just created and has no data yet), leave it False.
    """
    if purge_existing:
        purge_predefined_topic_data(topic_name)
    return run_background_analysis(
        app, user_id=None, topic_name=topic_name,
        reddit_cap=5000, comments_per_sub=1000, max_subreddits=5,
        yt_cap=5000, yt_per_video=500, max_yt_videos=10,
    )


def run_background_analysis(app, user_id, topic_name,
                            reddit_cap=None, comments_per_sub=None, max_subreddits=None,
                            yt_cap=None, yt_per_video=None, max_yt_videos=None):
    """Scrape → analyse → store for a user's custom topic (or a predefined topic when user_id is None)."""

    # Resolve caps (None → module defaults)
    reddit_cap       = reddit_cap       or REDDIT_CAP
    comments_per_sub = comments_per_sub or COMMENTS_PER_SUB
    max_subreddits   = max_subreddits   or MAX_SUBREDDITS
    yt_cap           = yt_cap           or YT_CAP
    yt_per_video     = yt_per_video     or YT_PER_VIDEO
    max_yt_videos    = max_yt_videos    or MAX_YT_VIDEOS

    with app.app_context():
        try:
            import traceback
            from app.utils import job_tracker
            from app.api.reddit import get_reddit_comments
            from app.api.huggingface import process_and_store_comments
            from app.api.gemini import get_sources_for_topic

            print(f"[bg] Starting analysis for user={user_id} topic='{topic_name}' "
                  f"(caps: reddit={reddit_cap}, youtube={yt_cap})")

            # Step 0: Purge any existing data for this topic before a fresh run.
            # For predefined topics (user_id=None) we don't purge the topic row —
            # admin_service.add_topic has just created it. A repeat run from the
            # "Analyse" button purges via a different path already.
            if user_id is not None:
                purge_user_topic(user_id, topic_name)

            # Step 1: Gemini source discovery
            sources = get_sources_for_topic(topic_name)
            subreddits = (sources.get("subreddits") or ["all"])[:max_subreddits]
            print(f"[bg] Gemini sources: category={sources.get('category')}, subreddits={subreddits}")

            # ── Step 2: Fetch Reddit + YouTube IN PARALLEL ──────────────────────
            job_tracker.set_step(user_id, topic_name, "fetching")
            from app.api.reddit import fetch_progress
            fetch_progress["message"] = "Fetching Reddit + YouTube in parallel..."

            # --- Reddit fetcher (runs as a single future) ---
            def _fetch_all_reddit():
                rd_dfs = []

                def _fetch_sub(sub):
                    try:
                        df_sub = get_reddit_comments(
                            topic_name, limit_posts=50,
                            max_comments=comments_per_sub,
                            subreddit_name=sub, topic_name=f"{topic_name}_{sub}",
                        )
                        if not df_sub.empty:
                            df_sub["source_id"] = sub
                        return sub, df_sub
                    except Exception as e:
                        print(f"[bg] Error fetching r/{sub}: {e}")
                        return sub, pd.DataFrame()

                done = 0
                with ThreadPoolExecutor(max_workers=min(5, len(subreddits))) as ex:
                    futures = {ex.submit(_fetch_sub, sub): sub for sub in subreddits}
                    for fut in as_completed(futures):
                        sub, df_sub = fut.result()
                        done += 1
                        if not df_sub.empty:
                            rd_dfs.append(df_sub)
                            print(f"[bg] Got {len(df_sub)} comments from r/{sub} ({done}/{len(subreddits)} done)")
                        fetch_progress["message"] = f"Fetching Reddit... ({done}/{len(subreddits)} subs done)"

                if not rd_dfs:
                    return pd.DataFrame()
                return pd.concat(rd_dfs, ignore_index=True).drop_duplicates(subset=["text"]).head(reddit_cap)

            # --- YouTube fetcher (runs as a single future) ---
            yt_api_key = os.getenv("YOUTUBE_API_KEY")
            yt_queries = sources.get("youtube_queries") or []

            def _fetch_all_youtube():
                """Discover videos, fetch comments in parallel, return (combined_df, video_ids)."""
                if not yt_queries or not yt_api_key:
                    return pd.DataFrame(), []

                from app.api.youtube import search_videos, get_video_comments
                topic_keywords = [w.lower() for w in topic_name.split() if len(w) >= 4]

                def _title_ok(title):
                    return any(kw in title.lower() for kw in topic_keywords)

                # Discover candidate videos
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
                selected = sorted_videos[:max_yt_videos]
                if not selected:
                    return pd.DataFrame(), []

                # Fetch comments from selected videos IN PARALLEL
                def _fetch_video(video):
                    vid_id = video["video_id"]
                    try:
                        print(f"[bg] Fetching YouTube comments: {video['title'][:60]}")
                        df_v = get_video_comments(vid_id, video["title"], yt_api_key, max_comments=yt_per_video)
                        if not df_v.empty:
                            df_v["source_id"] = vid_id
                            return vid_id, df_v
                        else:
                            print(f"[bg] No comments (disabled/private) — skipping")
                    except Exception as e:
                        print(f"[bg] YouTube fetch error for video '{vid_id}': {e}")
                    return vid_id, pd.DataFrame()

                yt_dfs = []
                vid_ids = []
                with ThreadPoolExecutor(max_workers=min(4, len(selected))) as ex:
                    futures = [ex.submit(_fetch_video, v) for v in selected]
                    yt_total = 0
                    for fut in as_completed(futures):
                        vid_id, df_v = fut.result()
                        if not df_v.empty and yt_total < yt_cap:
                            take = min(len(df_v), yt_cap - yt_total)
                            yt_dfs.append(df_v.head(take))
                            vid_ids.append(vid_id)
                            yt_total += take
                            print(f"[bg] Got {take} YouTube comments from {vid_id} (total: {yt_total})")

                if not yt_dfs:
                    return pd.DataFrame(), []
                return pd.concat(yt_dfs, ignore_index=True), vid_ids

            # Launch Reddit + YouTube concurrently
            with ThreadPoolExecutor(max_workers=2) as ex:
                reddit_future  = ex.submit(_fetch_all_reddit)
                youtube_future = ex.submit(_fetch_all_youtube)

            df = reddit_future.result()
            df_yt_combined, yt_source_rows = youtube_future.result()

            if df.empty and df_yt_combined.empty:
                print(f"[bg] No data found for '{topic_name}'")
                job_tracker.mark_failed(user_id, topic_name)
                return

            # ── Step 3: Classify + store (sequential — CPU model can't parallelise) ──
            job_tracker.set_step(user_id, topic_name, "classifying")
            # Custom topics always use HF API (local CPU model is too slow for
            # interactive use). Only predefined topics (user_id=None, 5k+5k
            # seeding) honour CLASSIFIER_MODE=local where API quota matters.
            _mode = "api" if user_id is not None else get_classifier_mode()

            if not df.empty:
                print(f"[bg] Combined: {len(df)} unique Reddit comments")
                process_and_store_comments(topic_name, df, user_id, source_type="reddit", mode=_mode)

            if not df_yt_combined.empty:
                print(f"[bg] Combined: {len(df_yt_combined)} YouTube comments from {len(yt_source_rows)} videos")
                for vid_id, group in df_yt_combined.groupby("source_id"):
                    process_and_store_comments(
                        topic_name, group, user_id,
                        source_type="youtube", source_id=vid_id, mode=_mode,
                    )

            # Step 4: Store Gemini-discovered sources and update category
            job_tracker.set_step(user_id, topic_name, "storing")
            try:
                from app.services import topic_service, comment_service
                if user_id is not None:
                    topic_id = topic_service.get_user_topic_id(topic_name, str(user_id))
                else:
                    topic_id = topic_service.get_predefined_topic_id(topic_name)
                if topic_id:
                    source_entries = (
                        [("reddit", sub) for sub in subreddits] +
                        [("news", kw) for kw in sources.get("news_keywords", [])] +
                        [("youtube", vid_id) for vid_id in yt_source_rows]
                    )
                    for src_type, src_id in source_entries:
                        try:
                            comment_service.get_or_create_topic_source(topic_id, src_type, src_id)
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

            # Pre-compute charts/deep-dive cache in a separate thread so the user
            # gets results immediately and the cache is ready for subsequent visits.
            def _precompute(_uid=user_id, _name=topic_name):
                try:
                    from app.services.topic_service import get_user_topic_id, get_predefined_topic_id
                    if _uid is not None:
                        _tid = get_user_topic_id(_name, str(_uid))
                    else:
                        _tid = get_predefined_topic_id(_name)
                    if _tid:
                        from app.utils.chart_cache import precompute_and_store
                        precompute_and_store(_tid, topic_name=_name)
                except Exception as _e:
                    print(f"[bg] Pre-compute failed for '{_name}': {_e}")

            import threading as _threading
            _threading.Thread(target=_precompute, daemon=True).start()

        except Exception as e:
            import traceback
            print(f"[bg] Analysis failed for '{topic_name}': {e}")
            traceback.print_exc()
            try:
                from app.utils import job_tracker
                job_tracker.mark_failed(user_id, topic_name)
            except Exception:
                pass
            # Only purge user-custom topics on failure. Predefined topics are
            # shared resources and their row was created explicitly by the admin.
            if user_id is not None:
                purge_user_topic(user_id, topic_name)


def run_incremental_analysis(app, user_id, topic_name, topic_id, since_date):
    """
    Fetch only new data since `since_date` and append to the existing topic.
    Does NOT purge existing comments. Recomputes snapshot from all comments (old + new).
    """
    from datetime import date
    import threading

    with app.app_context():
        try:
            from app.utils import job_tracker
            from app.services import topic_service, comment_service
            from app.api.reddit import get_reddit_comments, fetch_progress
            from app.api.huggingface import process_and_store_comments
            from app.api.youtube import search_videos, get_video_comments

            print(f"[incremental] Starting incremental analysis for user={user_id} topic='{topic_name}' topic_id={topic_id}")

            # 1. Compute days_back from since_date to today
            try:
                last = date.fromisoformat(str(since_date)[:10])
            except (ValueError, TypeError):
                last = date.today()
            days_back = max(1, (date.today() - last).days)
            print(f"[incremental] days_back={days_back} (since {last})")

            # 2. Get existing post IDs for deduplication
            existing_ids = comment_service.get_existing_external_post_ids(topic_id)
            print(f"[incremental] Found {len(existing_ids)} existing posts to skip")

            # 3. Get existing topic sources (subreddits + YouTube video IDs)
            res_sources = admin_supabase.table("topic_sources") \
                .select("source_type,source_id") \
                .eq("topic_id", topic_id).execute()
            subreddit_set = set()
            yt_vid_set = set()
            for src in (res_sources.data or []):
                st = src.get("source_type", "")
                si = src.get("source_id", "")
                if st == "reddit":
                    subreddit_set.add(si)
                elif st == "youtube":
                    yt_vid_set.add(si)
            subreddits = list(subreddit_set) if subreddit_set else ["all"]
            print(f"[incremental] Reusing sources: subreddits={subreddits}")

            # 4. Step: fetching — Reddit
            job_tracker.set_step(user_id, topic_name, "fetching")
            fetch_progress["message"] = f"Fetching new Reddit comments since {since_date}..."
            dfs = []
            completed = 0
            with ThreadPoolExecutor(max_workers=3) as ex:
                futures = {
                    ex.submit(get_reddit_comments, topic_name,
                              limit_posts=50, max_comments=COMMENTS_PER_SUB,
                              subreddit_name=sub, topic_name=f"{topic_name}_{sub}",
                              days_back=days_back): sub
                    for sub in subreddits
                }
                for f in as_completed(futures):
                    sub = futures[f]
                    completed += 1
                    fetch_progress["message"] = f"Fetching Reddit... ({completed}/{len(subreddits)} subreddits done)"
                    try:
                        df = f.result()
                        if df is not None and not df.empty:
                            mask = ~df["post_id"].isin(existing_ids)
                            new_df = df[mask]
                            if not new_df.empty:
                                print(f"[incremental] Reddit '{sub}': {len(new_df)} new comments")
                                dfs.append(new_df)
                            else:
                                print(f"[incremental] Reddit '{sub}': 0 new (all seen)")
                    except Exception as e:
                        print(f"[incremental] Error fetching from r/{sub}: {e}")

            # 5. Step: classifying — Reddit
            if dfs:
                df_reddit = pd.concat(dfs).drop_duplicates(subset=["text"]).head(REDDIT_CAP)
                if not df_reddit.empty:
                    job_tracker.set_step(user_id, topic_name, "classifying")
                    fetch_progress["message"] = f"Running AI sentiment on {len(df_reddit)} new Reddit comments..."
                    print(f"[incremental] Classifying & storing {len(df_reddit)} Reddit comments")
                    process_and_store_comments(topic_name, df_reddit, user_id, source_type="reddit", mode="api")

            # 6. Step: fetching — YouTube (search for NEW videos only)
            yt_api_key = os.getenv("YOUTUBE_API_KEY")
            if yt_api_key:
                job_tracker.set_step(user_id, topic_name, "fetching")
                fetch_progress["message"] = "Searching for new YouTube videos..."
                from app.api.gemini import get_sources_for_topic
                try:
                    sources = get_sources_for_topic(topic_name)
                    yt_queries = sources.get("youtube_queries") or []
                except Exception:
                    yt_queries = []

                seen_videos = set()
                yt_count = 0
                for query in yt_queries:
                    try:
                        for video in search_videos(query, yt_api_key, max_results=5, days_back=days_back):
                            vid_id = video["video_id"]
                            if vid_id in yt_vid_set or vid_id in seen_videos:
                                continue  # already scraped in a previous run
                            seen_videos.add(vid_id)
                            per_video = min(YT_PER_VIDEO, YT_CAP - yt_count)
                            if per_video <= 0:
                                break
                            try:
                                fetch_progress["message"] = f"Fetching YouTube: {video['title'][:50]}..."
                                df_yt = get_video_comments(vid_id, video["title"], yt_api_key, max_comments=per_video)
                                if df_yt is not None and not df_yt.empty:
                                    job_tracker.set_step(user_id, topic_name, "classifying")
                                    process_and_store_comments(
                                        topic_name, df_yt, user_id,
                                        source_type="youtube", source_id=vid_id, mode="api"
                                    )
                                    yt_count += len(df_yt)
                                    print(f"[incremental] YouTube '{vid_id}': {len(df_yt)} comments")
                            except Exception as e:
                                print(f"[incremental] Error fetching YT video {vid_id}: {e}")
                    except Exception as e:
                        print(f"[incremental] YouTube search error for '{query}': {e}")

            # 7. Step: storing — snapshot + cache
            job_tracker.set_step(user_id, topic_name, "storing")
            fetch_progress["message"] = "Recomputing sentiment snapshot..."

            all_rows = comment_service.fetch_all_comments(topic_id)
            if all_rows:
                df_all = pd.DataFrame(all_rows)
                print(f"[incremental] Re-computing snapshot from {len(df_all)} total comments")
                comment_service.upsert_snapshot(topic_id, df_all)

            # 8. Rebuild precomputed cache in background (same pattern as run_background_analysis)
            def _precompute(_uid=user_id, _name=topic_name, _tid=topic_id):
                try:
                    with app.app_context():
                        from app.utils.chart_cache import precompute_and_store
                        print(f"[incremental] Rebuilding precomputed cache for topic_id={_tid}")
                        precompute_and_store(_tid, topic_name=_name)
                except Exception as e:
                    print(f"[incremental] Cache rebuild error: {e}")

            import threading as _threading
            _threading.Thread(target=_precompute, daemon=True).start()

            job_tracker.mark_complete(user_id, topic_name)
            print(f"[incremental] Incremental analysis complete for '{topic_name}'")

        except Exception as e:
            import traceback
            print(f"[incremental] Analysis failed for '{topic_name}': {e}")
            traceback.print_exc()
            try:
                from app.utils import job_tracker
                job_tracker.mark_failed(user_id, topic_name)
            except Exception:
                pass
