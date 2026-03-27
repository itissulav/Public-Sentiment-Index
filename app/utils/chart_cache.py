"""
chart_cache.py
==============
Pre-computes and persists the time_windows chart data for predefined topics.

Called from:
  - populate_chart_cache.py  (one-off, reads existing DB data)
  - run_weekly_job.py        (after each weekly refresh)
  - seed_topics.py           (after each topic is seeded)
"""

import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from app.services.supabase_client import admin_supabase

_COMMENT_COLS = (
    "id,topic_id,text,author,score,published_at,"
    "sentiment_label,emotion_label,emotion_scores,confidence_score,"
    "posts(external_post_id,title,topic_sources(source_type,source_id))"
)
_PAGE = 1000


def _fetch_all_comments(topic_id: int) -> list:
    """Paginate through all comments for a topic (mirrors main_controller logic)."""
    rows, offset = [], 0
    while True:
        page = admin_supabase.table("comments") \
            .select(_COMMENT_COLS) \
            .eq("topic_id", topic_id) \
            .order("id").range(offset, offset + _PAGE - 1).execute()
        if not page.data:
            break
        for row in page.data:
            post = row.pop("posts", None) or {}
            ts   = (post.pop("topic_sources", None) or {}) if post else {}
            row["post_id"]     = post.get("external_post_id", "")
            row["post_title"]  = post.get("title", "")
            row["source_type"] = ts.get("source_type", "")
            row["source_id"]   = ts.get("source_id", "")
        rows.extend(page.data)
        if len(page.data) < _PAGE:
            break
        offset += _PAGE
    return rows


def _safe_json(obj) -> object:
    """Convert numpy/non-serialisable types so postgrest-py can store the JSONB."""
    return json.loads(json.dumps(obj, default=str))


def precompute_and_store(topic_id: int, topic_name: str = "") -> bool:
    """
    Fetch all comments for topic_id, compute time_windows for [30, 60, 90] days,
    metadata, and deep_dive analytics, then upsert into precomputed_charts.

    topic_name is required for Gemini deep-dive calls; if omitted Gemini is skipped.
    Returns True on success, False on any error.
    """
    from app.utils.visualizer import ElectionDataVisualizer
    from app.utils.insights import compute_all_insights

    print(f"[chart_cache] Pre-computing charts for topic_id={topic_id}...")
    try:
        rows = _fetch_all_comments(topic_id)
        if not rows:
            print(f"[chart_cache] No comments for topic_id={topic_id} — skipping.")
            return False

        df_all = pd.DataFrame(rows)
        if "published_at" not in df_all.columns:
            df_all["published_at"] = ""
        df_all["published_at"] = df_all["published_at"].fillna("").astype(str)

        now = datetime.now(timezone.utc)
        time_windows: dict = {}

        for win in [30, 60, 90]:
            cutoff = (now - timedelta(days=win)).strftime("%Y-%m-%d")
            _dfa = df_all[df_all["published_at"] >= cutoff].copy()
            _dfr = _dfa[_dfa["source_type"] == "reddit"].copy()  if "source_type" in _dfa.columns else _dfa.copy()
            _dfy = _dfa[_dfa["source_type"] == "youtube"].copy() if "source_type" in _dfa.columns else pd.DataFrame()

            def _sc(df, label_source=None):
                if df is None or df.empty:
                    return {}
                try:
                    vis = ElectionDataVisualizer(df)
                    charts = vis.get_all_charts_data(primary_only=False)
                    if label_source == "youtube":
                        charts["chart17_community_breakdown"] = \
                            vis._get_community_breakdown(label_source="youtube")
                    return charts
                except Exception as e:
                    print(f"[chart_cache]   chart error ({win}D): {e}")
                    return {}

            def _si(df):
                if df is None or df.empty:
                    return {}
                try:
                    return compute_all_insights(df)
                except Exception as e:
                    print(f"[chart_cache]   insight error ({win}D): {e}")
                    return {}

            time_windows[str(win)] = {
                "all":              _sc(_dfa),
                "reddit":           _sc(_dfr),
                "youtube":          _sc(_dfy, label_source="youtube"),
                "insights_all":     _si(_dfa),
                "insights_reddit":  _si(_dfr),
                "insights_youtube": _si(_dfy),
            }
            print(f"[chart_cache]   {win}D window: {len(_dfa)} comments processed")

        # ── Build metadata ────────────────────────────────────────────────────
        df_reddit  = df_all[df_all["source_type"] == "reddit"].copy()  if "source_type" in df_all.columns else df_all.copy()
        df_youtube = df_all[df_all["source_type"] == "youtube"].copy() if "source_type" in df_all.columns else pd.DataFrame()

        reddit_count  = int((df_all["source_type"] == "reddit").sum())  if "source_type" in df_all.columns else 0
        youtube_count = int((df_all["source_type"] == "youtube").sum()) if "source_type" in df_all.columns else 0
        total = reddit_count + youtube_count or 1
        source_split = {
            "reddit_count":  reddit_count,
            "youtube_count": youtube_count,
            "reddit_pct":    round(reddit_count  / total * 100, 1),
            "youtube_pct":   round(youtube_count / total * 100, 1),
        }

        def _top_comment(df, sentiment):
            if df is None or df.empty:
                return None
            mask = df.get("sentiment_label", pd.Series(dtype=str)) == sentiment
            if not mask.any() or "score" not in df.columns:
                return None
            row = df[mask].nlargest(1, "score").iloc[0]
            return {k: (v.item() if hasattr(v, "item") else v) for k, v in row.items()}

        yt_video_cards = []
        if not df_youtube.empty and "source_id" in df_youtube.columns:
            for vid_id, grp in df_youtube.groupby("source_id"):
                title = grp["post_title"].iloc[0] if "post_title" in grp.columns else str(vid_id)
                if len(title) > 80:
                    title = title[:80] + "…"
                dom_emotion = (
                    grp["emotion_label"].mode().iloc[0]
                    if "emotion_label" in grp.columns and not grp["emotion_label"].dropna().empty
                    else "neutral"
                )
                pos_pct = (
                    round((grp["sentiment_label"] == "Positive").sum() / len(grp) * 100, 1)
                    if "sentiment_label" in grp.columns else 0.0
                )
                yt_video_cards.append({
                    "video_id":         str(vid_id),
                    "title":            title,
                    "comment_count":    len(grp),
                    "dominant_emotion": str(dom_emotion),
                    "pos_pct":          pos_pct,
                })
            yt_video_cards.sort(key=lambda x: x["comment_count"], reverse=True)

        from app.utils.comparator import _text_length_by_sentiment, _expand_emotion_scores
        _df_exp = _expand_emotion_scores(df_all.copy())

        metadata = {
            "source_split":    source_split,
            "has_youtube":     youtube_count > 0,
            "top_positive":    _top_comment(df_reddit,  "Positive"),
            "top_negative":    _top_comment(df_reddit,  "Negative"),
            "top_positive_yt": _top_comment(df_youtube, "Positive"),
            "top_negative_yt": _top_comment(df_youtube, "Negative"),
            "yt_video_cards":  yt_video_cards,
            "text_length":     _text_length_by_sentiment(_df_exp),
        }

        # ── Build deep_dive ───────────────────────────────────────────────────
        import random as _random
        from app.utils.gemini_insights import (
            get_deep_dive_insights, get_narrative_report, get_opinion_clusters,
        )

        def _dd_charts(df, label_source=None):
            if df is None or df.empty:
                return {}
            try:
                vis = ElectionDataVisualizer(df)
                charts = vis.get_all_charts_data(primary_only=False)
                if label_source == "youtube":
                    charts["chart17_community_breakdown"] = \
                        vis._get_community_breakdown(label_source="youtube")
                return charts
            except Exception as e:
                print(f"[chart_cache] deep_dive charts error: {e}")
                return {}

        dd_charts_all     = _dd_charts(df_all)
        dd_charts_reddit  = _dd_charts(df_reddit)
        dd_charts_youtube = _dd_charts(df_youtube, label_source="youtube")

        # Reuse _si() defined above (no time-window filter — deep-dive uses all data)
        ins_all    = _si(df_all)
        ins_reddit = _si(df_reddit)
        ins_yt     = _si(df_youtube)

        dd_source_split = None
        if "source_type" in df_all.columns:
            counts = df_all["source_type"].value_counts(normalize=True).to_dict()
            dd_source_split = {k: round(v, 3) for k, v in counts.items()}

        try:
            snap = admin_supabase.table("daily_snapshots") \
                .select("psi_rating").eq("topic_id", topic_id) \
                .order("snapshot_date", desc=True).limit(1).execute()
            psi_rating = float((snap.data or [{}])[0].get("psi_rating") or 0)
        except Exception:
            psi_rating = 0.0

        gemini_all = gemini_reddit = gemini_youtube = {}
        narrative  = ""
        clusters   = []

        if topic_name:
            print(f"[chart_cache] Running Gemini deep-dive for '{topic_name}'...")
            try:
                gemini_all = get_deep_dive_insights(topic_name, ins_all, dd_charts_all)
            except Exception as e:
                print(f"[chart_cache] Gemini all error: {e}")
            try:
                if not df_reddit.empty:
                    gemini_reddit = get_deep_dive_insights(topic_name, ins_reddit, dd_charts_reddit)
            except Exception as e:
                print(f"[chart_cache] Gemini reddit error: {e}")
            try:
                if not df_youtube.empty:
                    gemini_youtube = get_deep_dive_insights(topic_name, ins_yt, dd_charts_youtube)
            except Exception as e:
                print(f"[chart_cache] Gemini youtube error: {e}")
            try:
                narrative = get_narrative_report(topic_name, ins_all, psi_rating, dd_source_split)
            except Exception as e:
                print(f"[chart_cache] Narrative error: {e}")
            try:
                all_rows_list = df_all.to_dict("records")
                pos_rows = [r for r in all_rows_list if r.get("sentiment_label") == "Positive"]
                neg_rows = [r for r in all_rows_list if r.get("sentiment_label") == "Negative"]
                neu_rows = [r for r in all_rows_list if r.get("sentiment_label") == "Neutral"]
                cluster_sample = (
                    _random.sample(pos_rows, min(50, len(pos_rows))) +
                    _random.sample(neg_rows, min(50, len(neg_rows))) +
                    _random.sample(neu_rows, min(50, len(neu_rows)))
                )
                clusters = get_opinion_clusters(topic_name, cluster_sample)
            except Exception as e:
                print(f"[chart_cache] Clusters error: {e}")

        deep_dive = {
            "charts_all":     dd_charts_all,
            "charts_reddit":  dd_charts_reddit,
            "charts_youtube": dd_charts_youtube,
            "gemini":         gemini_all,
            "gemini_all":     gemini_all,
            "gemini_reddit":  gemini_reddit,
            "gemini_youtube": gemini_youtube,
            "narrative":      narrative,
            "clusters":       clusters,
        }

        admin_supabase.table("precomputed_charts").upsert(
            {
                "topic_id":     topic_id,
                "time_windows": _safe_json(time_windows),
                "metadata":     _safe_json(metadata),
                "deep_dive":    _safe_json(deep_dive),
            },
            on_conflict="topic_id",
        ).execute()
        print(f"[chart_cache] Stored precomputed charts for topic_id={topic_id} ✓")
        return True

    except Exception as e:
        import traceback
        print(f"[chart_cache] Error for topic_id={topic_id}: {e}")
        traceback.print_exc()
        return False
