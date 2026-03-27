# app/controllers/main_controller.py

from flask import Blueprint, render_template, session, flash, redirect, url_for, current_app, request, jsonify
import os
import json
import pandas as pd
from app.models.users import User
from app.services.supabase_client import admin_supabase
from app.utils.visualizer import ElectionDataVisualizer
from app.services import admin_service
from app.utils.cache import topic_cache
from app.api.wiki import fetch_wiki_image as _fetch_wiki_image
from app.services.analysis_service import run_background_analysis, purge_user_topic as _purge_user_topic

# This MUST match what you import in __init__.py
main_bp = Blueprint("main", __name__)

# PostgREST nested-select string for fetching comments with their post/source context
_COMMENT_COLS = (
    "id,topic_id,text,author,score,published_at,"
    "sentiment_label,emotion_label,emotion_scores,confidence_score,"
    "posts(external_post_id,title,topic_sources(source_type,source_id))"
)

_PREDEFINED_TOPICS = {
    "Donald Trump", "Elon Musk", "Gaza Conflict", "AI Technology", "Climate Change",
}

def _flatten_comment_row(row: dict) -> dict:
    """Flatten the nested posts → topic_sources join into a flat dict matching
    the old comments_view column names expected by the visualizer."""
    post = row.pop("posts", None) or {}
    ts   = (post.pop("topic_sources", None) or {}) if post else {}
    row["post_id"]    = post.get("external_post_id", "")
    row["post_title"] = post.get("title", "")
    row["source_type"] = ts.get("source_type", "")
    row["source_id"]   = ts.get("source_id", "")
    return row

@main_bp.route("/")
def home():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    trending_topics = []
    try:
        # Fetch topics + their latest daily_snapshot for rating/sentiment display
        res = admin_supabase.table("topics").select("id, name, category").is_("user_id", "null").execute()
        if res.data:
            for t in res.data:
                snap = admin_supabase.table("daily_snapshots") \
                    .select("psi_rating, dominant_emotion, total_comments, snapshot_date") \
                    .eq("topic_id", t["id"]) \
                    .order("snapshot_date", desc=True).limit(1).execute()
                if snap.data:
                    t["rating"]           = snap.data[0].get("psi_rating", 0)
                    t["sentiment"]        = "Positive" if (t["rating"] or 0) > 0 else ("Negative" if (t["rating"] or 0) < 0 else "Neutral")
                    t["dominant_emotion"] = snap.data[0].get("dominant_emotion", "neutral")
                    count_res = admin_supabase.table("comments").select("id", count="exact").eq("topic_id", t["id"]).execute()
                    t["total_comments"]   = count_res.count or 0
                else:
                    t["rating"] = 0
                    t["sentiment"] = "Neutral"
                    t["total_comments"] = 0
                    t["dominant_emotion"] = "neutral"
            trending_topics = sorted(res.data, key=lambda x: x.get("rating", 0), reverse=True)[:5]
    except:
        pass

    saved_scan_count = 0
    if user:
        try:
            res = admin_supabase.table("topics").select("id", count="exact") \
                                .eq("user_id", user.get_user_id()).execute()
            saved_scan_count = res.count or 0
        except:
            pass

    return render_template("home.html", user=user, trending_topics=trending_topics, saved_scan_count=saved_scan_count)

@main_bp.route("/about")
def about():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    return render_template("about.html", user=user)

@main_bp.route("/admin")
def admindashboard():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    
    if not user:
        flash("You must be logged in to view that page.")
        return redirect(url_for("auth.login"))
        
    if str(user.get_role()).lower() != "admin":
        flash("Access Denied. Administrator privileges required.")
        return redirect(url_for("main.home"))
    
    total_users, all_users = admin_service.get_all_users()
    total_topics, predefined_topics = admin_service.get_predefined_topics()
    # Enrich predefined_topics with latest snapshot data for the admin dashboard
    for t in predefined_topics:
        try:
            snap = admin_supabase.table("daily_snapshots") \
                .select("psi_rating, dominant_emotion, total_comments, snapshot_date") \
                .eq("topic_id", t["id"]) \
                .order("snapshot_date", desc=True).limit(1).execute()
            if snap.data:
                t["rating"]    = snap.data[0].get("psi_rating", 0)
                t["sentiment"] = "Positive" if (t.get("rating") or 0) > 0 else ("Negative" if (t.get("rating") or 0) < 0 else "Neutral")
                t["total_comments"] = snap.data[0].get("total_comments", 0)
        except Exception:
            pass

    return render_template("admindashboard.html", user=user, total_users=total_users, all_users=all_users, total_topics=total_topics, predefined_topics=predefined_topics)

@main_bp.route("/trends", methods=["GET", "POST"])
def trends():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    current_user_id = user.get_user_id() if user else None

    current_topic = None
    charts_data   = {}      # kept for backward compat (= charts_reddit below)
    charts_all    = {}
    charts_reddit = {}
    charts_youtube = {}
    insights_data   = {}    # kept for backward compat (= insights_reddit below)
    insights_all    = {}
    insights_reddit = {}
    insights_youtube = {}
    has_youtube  = False
    source_split = {}
    yt_video_cards = []
    top_positive    = None
    top_negative    = None
    top_positive_yt = None
    top_negative_yt = None
    topic_info = None
    job_status = None   # 'processing' | None
    has_data         = False  # True only when comment data was successfully loaded
    current_topic_id = None   # Passed to template for the deep-dive AJAX endpoint
    topic_cover_url  = None   # Wikipedia image URL for custom (non-predefined) topics
    selected_days    = 90     # 30 / 60 / 90 — time window for featured topic charts
    is_featured      = False  # True when showing a predefined topic
    time_windows     = {}     # Pre-computed {30: {...}, 60: {...}, 90: {...}} for client-side switching

    # Build search autocomplete suggestions: predefined + user history
    _TRENDS_IMG = {
        "Donald Trump":  "donaldtrump.png",
        "Elon Musk":     "elonmusk.png",
        "Gaza Conflict": "gazaconflict.png",
        "AI Technology": "aitechnology.png",
        "Climate Change":"climatechange.png",
    }
    suggestions = [{'name': t, 'kind': 'featured', 'img': _TRENDS_IMG.get(t, '')} for t in PREDEFINED_TOPICS]
    if user:
        try:
            _hist = admin_supabase.table("topics").select("name") \
                                  .eq("user_id", current_user_id) \
                                  .order("created_at", desc=True).execute()
            if _hist.data:
                _existing = {s['name'] for s in suggestions}
                for row in _hist.data:
                    if row['name'] not in _existing:
                        suggestions.append({'name': row['name'], 'kind': 'saved', 'img': ''})
        except Exception:
            pass

    if request.method == "POST" or (request.method == "GET" and request.args.get("topic")):
        if request.method == "GET":
            topic_query    = request.args.get("topic")
            topic_id_param = None
            selected_days  = max(30, min(90, int(request.args.get("days", 90))))
        else:
            topic_query    = request.form.get("topic")
            topic_id_param = request.form.get("topic_id")   # set by View Analysis on history page
            selected_days  = max(30, min(90, int(request.form.get("days", 90))))

        if topic_query:
            current_topic = topic_query
            # Normalise against predefined topics (case-insensitive)
            _lower = current_topic.lower()
            _canonical = next((t for t in _PREDEFINED_TOPICS if t.lower() == _lower), None)
            if _canonical:
                current_topic = _canonical
            # Fetch a cover image for non-predefined (custom) topics
            if current_topic not in _PREDEFINED_TOPICS:
                topic_cover_url = _fetch_wiki_image(current_topic)

            # Helper to fetch, split by source, and build above-fold chart/insight bundles.
            # Deep-dive charts and Gemini are deferred to /trends/deep-dive (background AJAX).
            def extract_and_visualize(t_id, days=None):
                nonlocal charts_data, charts_all, charts_reddit, charts_youtube, \
                         insights_data, insights_all, insights_reddit, insights_youtube, \
                         has_youtube, source_split, yt_video_cards, has_data, \
                         top_positive, top_negative, top_positive_yt, top_negative_yt, \
                         current_topic_id

                from app.utils.insights import compute_all_insights

                current_topic_id = t_id

                PAGE = 1000  # Supabase PostgREST hard-caps responses at 1000 rows

                all_rows = topic_cache.get(t_id)
                if all_rows is None:
                    all_rows = []
                    offset = 0
                    try:
                        while True:
                            q = admin_supabase.table("comments").select(_COMMENT_COLS).eq("topic_id", t_id)
                            page = q.order("id").range(offset, offset + PAGE - 1).execute()
                            if not page.data:
                                break
                            all_rows.extend(_flatten_comment_row(r) for r in page.data)
                            if len(page.data) < PAGE:
                                break
                            offset += PAGE
                    except Exception as e:
                        print(f"[extract] Query error: {e}")
                        return False
                    topic_cache.set(t_id, all_rows)
                    print(f"[cache] Fetched and cached {len(all_rows)} rows for topic_id={t_id}")
                else:
                    print(f"[cache] Cache hit — {len(all_rows)} rows for topic_id={t_id}")

                if not all_rows:
                    return False

                # Apply time window filter for featured topics (days=30/60/90)
                if days and days < 90:
                    from datetime import datetime, timedelta, timezone
                    _cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
                    all_rows = [
                        r for r in all_rows
                        if r.get("published_at") and str(r["published_at"]) >= _cutoff
                    ]
                    if not all_rows:
                        return False

                df_all     = pd.DataFrame(all_rows)
                df_reddit  = df_all[df_all["source_type"] == "reddit"].copy() \
                             if "source_type" in df_all.columns \
                             else df_all.copy()
                df_youtube = df_all[df_all["source_type"] == "youtube"].copy() \
                             if "source_type" in df_all.columns \
                             else pd.DataFrame()

                has_youtube = not df_youtube.empty

                def _safe_charts(df, primary_only=False):
                    if df is None or df.empty:
                        return {}
                    try:
                        return ElectionDataVisualizer(df).get_all_charts_data(primary_only=primary_only)
                    except Exception as e:
                        print(f"[charts] Error: {e}")
                        return {}

                def _safe_insights(df):
                    if df is None or df.empty:
                        return {}
                    try:
                        # No topic_name/charts_data → Gemini is skipped; deferred to deep-dive endpoint
                        return compute_all_insights(df)
                    except Exception as e:
                        print(f"[insights] Error: {e}")
                        return {}

                # Only compute above-fold charts (primary_only=True skips the 11 deep-dive charts)
                charts_all     = _safe_charts(df_all,    primary_only=True)
                charts_reddit  = _safe_charts(df_reddit, primary_only=True)
                insights_all    = _safe_insights(df_all)
                insights_reddit = _safe_insights(df_reddit)

                # For backward compat — Reddit tab is the primary detail view
                charts_data   = charts_reddit
                insights_data = insights_reddit

                if has_youtube:
                    charts_youtube   = _safe_charts(df_youtube, primary_only=True)
                    charts_youtube["yt_video_cards"] = _build_yt_video_cards(df_youtube)
                    yt_video_cards   = charts_youtube["yt_video_cards"]
                    insights_youtube = _safe_insights(df_youtube)

                    # YouTube viral comments
                    yt_pos = df_youtube[df_youtube["sentiment_label"] == "Positive"]
                    yt_neg = df_youtube[df_youtube["sentiment_label"] == "Negative"]
                    if not yt_pos.empty:
                        top_positive_yt = yt_pos.nlargest(1, "score").to_dict("records")[0]
                    if not yt_neg.empty:
                        top_negative_yt = yt_neg.nlargest(1, "score").to_dict("records")[0]

                # Source split percentages for overview tab
                total = max(len(df_all), 1)
                source_split = {
                    "reddit_count":  len(df_reddit),
                    "youtube_count": len(df_youtube),
                    "reddit_pct":    round(len(df_reddit)  / total * 100, 1),
                    "youtube_pct":   round(len(df_youtube) / total * 100, 1),
                }

                # Top viral Reddit comments (Reddit tab — Reddit-only data)
                pos_reddit = df_reddit[df_reddit["sentiment_label"] == "Positive"] if not df_reddit.empty else pd.DataFrame()
                neg_reddit = df_reddit[df_reddit["sentiment_label"] == "Negative"] if not df_reddit.empty else pd.DataFrame()
                if not pos_reddit.empty:
                    top_positive = pos_reddit.nlargest(1, "score").to_dict("records")[0]
                if not neg_reddit.empty:
                    top_negative = neg_reddit.nlargest(1, "score").to_dict("records")[0]

                has_data = True
                return True

            # VIEW SAVED SCAN: topic_id provided → load from DB without re-scraping
            if topic_id_param and current_user_id:
                topic_res = admin_supabase.table("topics") \
                                          .select("id, name") \
                                          .eq("id", topic_id_param) \
                                          .eq("user_id", current_user_id) \
                                          .execute()
                if topic_res.data:
                    topic_id      = topic_res.data[0]['id']
                    current_topic = topic_res.data[0]['name']
                    # Load latest snapshot for rating display
                    snap = admin_supabase.table("daily_snapshots") \
                        .select("psi_rating, dominant_emotion, total_comments") \
                        .eq("topic_id", topic_id).order("snapshot_date", desc=True).limit(1).execute()
                    topic_info = snap.data[0] if snap.data else {}
                    topic_info["rating"]    = topic_info.pop("psi_rating", 0)
                    topic_info["name"]      = current_topic
                    topic_info["sentiment"] = "Positive" if (topic_info.get("rating") or 0) > 0 else ("Negative" if (topic_info.get("rating") or 0) < 0 else "Neutral")
                    count_res = admin_supabase.table("comments").select("id", count="exact").eq("topic_id", topic_id).execute()
                    topic_info["total_comments"] = count_res.count or 0
                    _hist_cache_hit = False
                    try:
                        _pc = admin_supabase.table("precomputed_charts") \
                            .select("time_windows, metadata") \
                            .eq("topic_id", topic_id) \
                            .maybe_single().execute()
                        _pc_data = _pc.data or {}
                        _tw = _pc_data.get("time_windows") or {}
                        _md = _pc_data.get("metadata") or {}
                        _win_data = _tw.get("90") or {}
                        if _tw and _win_data:
                            charts_all      = _win_data.get("all",              {})
                            charts_reddit   = _win_data.get("reddit",           {})
                            charts_youtube  = _win_data.get("youtube",          {})
                            charts_data     = charts_reddit
                            insights_all     = _win_data.get("insights_all",     {})
                            insights_reddit  = _win_data.get("insights_reddit",  {})
                            insights_youtube = _win_data.get("insights_youtube", {})
                            insights_data    = insights_reddit
                            source_split    = _md.get("source_split",    {})
                            has_youtube     = _md.get("has_youtube",     False)
                            top_positive    = _md.get("top_positive")
                            top_negative    = _md.get("top_negative")
                            top_positive_yt = _md.get("top_positive_yt")
                            top_negative_yt = _md.get("top_negative_yt")
                            yt_video_cards  = _md.get("yt_video_cards",  [])
                            current_topic_id = topic_id
                            has_data         = True
                            time_windows     = _tw
                            _hist_cache_hit  = True
                            print(f"[trends] History cache hit for topic_id={topic_id}")
                    except Exception as _e:
                        print(f"[trends] History cache miss for topic_id={topic_id}: {_e}")
                    if not _hist_cache_hit:
                        if not extract_and_visualize(topic_id):
                            flash("No comment data found for this saved scan.")
                else:
                    flash("Saved scan not found or access denied.")

            elif current_topic in PREDEFINED_TOPICS:
                # 1. PREDEFINED TOPIC: Pull dynamically from relational DB schema
                is_featured = True
                topic_res = admin_supabase.table("topics") \
                                          .select("id") \
                                          .eq("name", current_topic) \
                                          .is_("user_id", "null") \
                                          .execute()

                if topic_res.data:
                    topic_id = topic_res.data[0]['id']
                    snap = admin_supabase.table("daily_snapshots") \
                        .select("psi_rating, dominant_emotion, total_comments") \
                        .eq("topic_id", topic_id).order("snapshot_date", desc=True).limit(1).execute()
                    topic_info = snap.data[0] if snap.data else {}
                    topic_info["rating"]    = topic_info.pop("psi_rating", 0)
                    topic_info["name"]      = current_topic
                    topic_info["sentiment"] = "Positive" if (topic_info.get("rating") or 0) > 0 else ("Negative" if (topic_info.get("rating") or 0) < 0 else "Neutral")
                    count_res = admin_supabase.table("comments").select("id", count="exact").eq("topic_id", topic_id).execute()
                    topic_info["total_comments"] = count_res.count or 0
                    # ── Try full cache bypass (no extract_and_visualize call) ───────
                    _full_cache_hit = False
                    try:
                        _pc = admin_supabase.table("precomputed_charts") \
                            .select("time_windows, metadata") \
                            .eq("topic_id", topic_id) \
                            .single().execute()
                        _pc_data = _pc.data or {}
                        _tw = _pc_data.get("time_windows") or {}
                        _md = _pc_data.get("metadata") or {}
                        _win_key  = str(selected_days)
                        _win_data = _tw.get(_win_key) or _tw.get("90") or {}

                        if _tw and _md and _win_data:
                            charts_all      = _win_data.get("all",              {})
                            charts_reddit   = _win_data.get("reddit",           {})
                            charts_youtube  = _win_data.get("youtube",          {})
                            charts_data     = charts_reddit
                            insights_all     = _win_data.get("insights_all",     {})
                            insights_reddit  = _win_data.get("insights_reddit",  {})
                            insights_youtube = _win_data.get("insights_youtube", {})
                            insights_data    = insights_reddit

                            source_split    = _md.get("source_split",    {})
                            has_youtube     = _md.get("has_youtube",     False)
                            top_positive    = _md.get("top_positive")
                            top_negative    = _md.get("top_negative")
                            top_positive_yt = _md.get("top_positive_yt")
                            top_negative_yt = _md.get("top_negative_yt")
                            yt_video_cards  = _md.get("yt_video_cards",  [])
                            current_topic_id = topic_id
                            has_data         = True
                            time_windows     = _tw
                            _full_cache_hit  = True
                            print(f"[trends] Full cache hit for topic_id={topic_id} ({_win_key}D)")
                    except Exception as _e:
                        print(f"[trends] precomputed_charts miss for topic_id={topic_id}: {_e}")

                    if not _full_cache_hit:
                        # Fallback: live fetch + compute
                        if not extract_and_visualize(topic_id, days=selected_days):
                            flash(f"No comments found for predefined topic: {current_topic}")
                        else:
                            # Live time_windows computation from in-memory cache
                            from app.utils.insights import compute_all_insights
                            from datetime import datetime, timedelta, timezone
                            _cached_rows = topic_cache.get(topic_id) or []
                            time_windows = {}
                            for _win in [30, 60, 90]:
                                _cutoff_date = (datetime.now(timezone.utc) - timedelta(days=_win)).strftime('%Y-%m-%d')
                                _rows = [r for r in _cached_rows if r.get("published_at") and str(r["published_at"])[:10] >= _cutoff_date]
                                if _rows:
                                    _dfa = pd.DataFrame(_rows)
                                    _dfr = _dfa[_dfa["source_type"] == "reddit"].copy() if "source_type" in _dfa.columns else _dfa.copy()
                                    _dfy = _dfa[_dfa["source_type"] == "youtube"].copy() if "source_type" in _dfa.columns else pd.DataFrame()
                                    def _sc(df, po=True):
                                        if df is None or df.empty: return {}
                                        try: return ElectionDataVisualizer(df).get_all_charts_data(primary_only=po)
                                        except: return {}
                                    def _si(df):
                                        if df is None or df.empty: return {}
                                        try: return compute_all_insights(df)
                                        except: return {}
                                    time_windows[str(_win)] = {
                                        "all":              _sc(_dfa, po=False),
                                        "reddit":           _sc(_dfr, po=False),
                                        "youtube":          _sc(_dfy, po=False),
                                        "insights_all":     _si(_dfa),
                                        "insights_reddit":  _si(_dfr),
                                        "insights_youtube": _si(_dfy),
                                    }
                else:
                    flash(f"No database records found for preset topic: {current_topic}. Please run the seeder script first!")
                    
            else:
                # 2. CUSTOM TOPIC — run in background so the user isn't blocked
                from app.utils import job_tracker

                if not user:
                    flash("You must be logged in to analyse a custom topic.")
                elif not current_user_id:
                    flash("Your session has expired. Please log in again.")
                else:
                    existing_status = job_tracker.get_status(current_user_id, current_topic)

                    if existing_status == "processing":
                        # Already running — just show the processing UI again
                        job_status = "processing"

                    elif existing_status == "complete":
                        # Job finished — load from DB and clear the tracker entry
                        job_tracker.clear(current_user_id, current_topic)
                        topic_res = admin_supabase.table("topics") \
                                                  .select("id") \
                                                  .eq("name", current_topic) \
                                                  .eq("user_id", current_user_id) \
                                                  .execute()
                        if topic_res.data:
                            topic_id = topic_res.data[0]['id']
                            snap = admin_supabase.table("daily_snapshots") \
                                .select("psi_rating, dominant_emotion, total_comments") \
                                .eq("topic_id", topic_id).order("snapshot_date", desc=True).limit(1).execute()
                            topic_info = snap.data[0] if snap.data else {}
                            topic_info["rating"]    = topic_info.pop("psi_rating", 0)
                            topic_info["name"]      = current_topic
                            topic_info["sentiment"] = "Positive" if (topic_info.get("rating") or 0) > 0 else ("Negative" if (topic_info.get("rating") or 0) < 0 else "Neutral")
                            count_res = admin_supabase.table("comments").select("id", count="exact").eq("topic_id", topic_id).execute()
                            topic_info["total_comments"] = count_res.count or 0
                            _custom_cache_hit = False
                            try:
                                _pc = admin_supabase.table("precomputed_charts") \
                                    .select("time_windows, metadata") \
                                    .eq("topic_id", topic_id) \
                                    .maybe_single().execute()
                                _pc_data = _pc.data or {}
                                _tw = _pc_data.get("time_windows") or {}
                                _md = _pc_data.get("metadata") or {}
                                _win_data = _tw.get("90") or {}
                                if _tw and _md and _win_data:
                                    charts_all      = _win_data.get("all",              {})
                                    charts_reddit   = _win_data.get("reddit",           {})
                                    charts_youtube  = _win_data.get("youtube",          {})
                                    charts_data     = charts_reddit
                                    insights_all     = _win_data.get("insights_all",     {})
                                    insights_reddit  = _win_data.get("insights_reddit",  {})
                                    insights_youtube = _win_data.get("insights_youtube", {})
                                    insights_data    = insights_reddit
                                    source_split    = _md.get("source_split",    {})
                                    has_youtube     = _md.get("has_youtube",     False)
                                    top_positive    = _md.get("top_positive")
                                    top_negative    = _md.get("top_negative")
                                    top_positive_yt = _md.get("top_positive_yt")
                                    top_negative_yt = _md.get("top_negative_yt")
                                    yt_video_cards  = _md.get("yt_video_cards",  [])
                                    current_topic_id = topic_id
                                    has_data         = True
                                    time_windows     = _tw
                                    _custom_cache_hit = True
                                    print(f"[trends] Custom topic cache hit for topic_id={topic_id}")
                            except Exception as _e:
                                print(f"[trends] Custom topic cache miss for topic_id={topic_id}: {_e}")
                            if not _custom_cache_hit:
                                extract_and_visualize(topic_id)
                        else:
                            flash(f"Analysis completed but data could not be loaded. Check your History.")

                    elif existing_status == "failed":
                        job_tracker.clear(current_user_id, current_topic)
                        flash(f"The background analysis for '{current_topic}' failed. Please try again.")

                    elif existing_status is None:
                        # No active job — check if topic already exists in DB (user re-searched)
                        topic_res = admin_supabase.table("topics") \
                                                  .select("id") \
                                                  .eq("name", current_topic) \
                                                  .eq("user_id", current_user_id) \
                                                  .execute()
                        if topic_res.data:
                            topic_id = topic_res.data[0]['id']
                            snap = admin_supabase.table("daily_snapshots") \
                                .select("psi_rating, dominant_emotion, total_comments") \
                                .eq("topic_id", topic_id).order("snapshot_date", desc=True).limit(1).execute()
                            topic_info = snap.data[0] if snap.data else {}
                            topic_info["rating"]    = topic_info.pop("psi_rating", 0)
                            topic_info["name"]      = current_topic
                            topic_info["sentiment"] = "Positive" if (topic_info.get("rating") or 0) > 0 else ("Negative" if (topic_info.get("rating") or 0) < 0 else "Neutral")
                            count_res = admin_supabase.table("comments").select("id", count="exact").eq("topic_id", topic_id).execute()
                            topic_info["total_comments"] = count_res.count or 0
                            _custom_cache_hit = False
                            try:
                                _pc = admin_supabase.table("precomputed_charts") \
                                    .select("time_windows, metadata") \
                                    .eq("topic_id", topic_id) \
                                    .maybe_single().execute()
                                _pc_data = _pc.data or {}
                                _tw = _pc_data.get("time_windows") or {}
                                _md = _pc_data.get("metadata") or {}
                                _win_data = _tw.get("90") or {}
                                if _tw and _md and _win_data:
                                    charts_all      = _win_data.get("all",              {})
                                    charts_reddit   = _win_data.get("reddit",           {})
                                    charts_youtube  = _win_data.get("youtube",          {})
                                    charts_data     = charts_reddit
                                    insights_all     = _win_data.get("insights_all",     {})
                                    insights_reddit  = _win_data.get("insights_reddit",  {})
                                    insights_youtube = _win_data.get("insights_youtube", {})
                                    insights_data    = insights_reddit
                                    source_split    = _md.get("source_split",    {})
                                    has_youtube     = _md.get("has_youtube",     False)
                                    top_positive    = _md.get("top_positive")
                                    top_negative    = _md.get("top_negative")
                                    top_positive_yt = _md.get("top_positive_yt")
                                    top_negative_yt = _md.get("top_negative_yt")
                                    yt_video_cards  = _md.get("yt_video_cards",  [])
                                    current_topic_id = topic_id
                                    has_data         = True
                                    time_windows     = _tw
                                    _custom_cache_hit = True
                                    print(f"[trends] Custom topic cache hit for topic_id={topic_id}")
                            except Exception as _e:
                                print(f"[trends] Custom topic cache miss for topic_id={topic_id}: {_e}")
                            if not _custom_cache_hit:
                                extract_and_visualize(topic_id)
                        else:
                            # Cross-user cache: reuse any recent scan of the same topic (within 7 days)
                            from datetime import datetime, timedelta, timezone
                            _week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
                            cached_res = admin_supabase.table("topics") \
                                                       .select("id") \
                                                       .ilike("name", current_topic) \
                                                       .gte("created_at", _week_ago) \
                                                       .limit(1) \
                                                       .execute()
                            if cached_res.data:
                                # Another user's recent scan exists — reuse it instantly
                                topic_id = cached_res.data[0]["id"]
                                snap = admin_supabase.table("daily_snapshots") \
                                    .select("psi_rating, dominant_emotion, total_comments") \
                                    .eq("topic_id", topic_id).order("snapshot_date", desc=True).limit(1).execute()
                                topic_info = snap.data[0] if snap.data else {}
                                topic_info["rating"]    = topic_info.pop("psi_rating", 0)
                                topic_info["name"]      = current_topic
                                topic_info["sentiment"] = "Positive" if (topic_info.get("rating") or 0) > 0 else ("Negative" if (topic_info.get("rating") or 0) < 0 else "Neutral")
                                count_res = admin_supabase.table("comments").select("id", count="exact").eq("topic_id", topic_id).execute()
                                topic_info["total_comments"] = count_res.count or 0
                                _xuser_cache_hit = False
                                try:
                                    _pc = admin_supabase.table("precomputed_charts") \
                                        .select("time_windows, metadata") \
                                        .eq("topic_id", topic_id) \
                                        .maybe_single().execute()
                                    _pc_data = _pc.data or {}
                                    _tw = _pc_data.get("time_windows") or {}
                                    _md = _pc_data.get("metadata") or {}
                                    _win_data = _tw.get("90") or {}
                                    if _tw and _win_data:
                                        charts_all      = _win_data.get("all",              {})
                                        charts_reddit   = _win_data.get("reddit",           {})
                                        charts_youtube  = _win_data.get("youtube",          {})
                                        charts_data     = charts_reddit
                                        insights_all     = _win_data.get("insights_all",     {})
                                        insights_reddit  = _win_data.get("insights_reddit",  {})
                                        insights_youtube = _win_data.get("insights_youtube", {})
                                        insights_data    = insights_reddit
                                        source_split    = _md.get("source_split",    {})
                                        has_youtube     = _md.get("has_youtube",     False)
                                        top_positive    = _md.get("top_positive")
                                        top_negative    = _md.get("top_negative")
                                        top_positive_yt = _md.get("top_positive_yt")
                                        top_negative_yt = _md.get("top_negative_yt")
                                        yt_video_cards  = _md.get("yt_video_cards",  [])
                                        current_topic_id = topic_id
                                        has_data         = True
                                        time_windows     = _tw
                                        _xuser_cache_hit = True
                                        print(f"[trends] Cross-user cache hit for topic_id={topic_id}")
                                except Exception as _e:
                                    print(f"[trends] Cross-user cache miss for topic_id={topic_id}: {_e}")
                                if not _xuser_cache_hit:
                                    extract_and_visualize(topic_id)
                            else:
                                # Truly new topic — kick off a new background thread
                                job_tracker.mark_processing(current_user_id, current_topic)
                                app = current_app._get_current_object()

                                thread = threading.Thread(
                                    target=_background_analyse_user_topic,
                                    args=(app, current_user_id, current_topic),
                                    daemon=True,
                                )
                                thread.start()
                                job_status = "processing"

    else:
        # GET request: Render completely empty to wait for user interaction
        pass
    
    return render_template(
        "trends.html",
        user=user,
        # Overview tab (combined)
        charts_all=json.dumps(charts_all),
        insights_all=insights_all,
        # Reddit tab
        charts_data=json.dumps(charts_reddit),     # backward compat alias
        charts_reddit=json.dumps(charts_reddit),
        insights_data=insights_reddit,              # backward compat alias
        insights_reddit=insights_reddit,
        # YouTube tab
        charts_youtube=json.dumps(charts_youtube),
        insights_youtube=insights_youtube,
        has_youtube=has_youtube,
        source_split=source_split,
        yt_video_cards=yt_video_cards,
        # Viral comments
        top_positive=top_positive,
        top_negative=top_negative,
        top_positive_yt=top_positive_yt,
        top_negative_yt=top_negative_yt,
        # Meta
        current_topic=current_topic,
        topic_info=topic_info,
        job_status=job_status,
        has_data=has_data,
        topic_id_for_deep_dive=current_topic_id,
        topic_cover_url=topic_cover_url,
        suggestions=suggestions,
        selected_days=selected_days,
        is_featured=is_featured,
        time_windows=time_windows,
    )


PREDEFINED_TOPICS = ["Donald Trump", "Elon Musk", "Gaza Conflict", "AI Technology", "Climate Change"]


@main_bp.route("/pulse")
def pulse():
    """Pulse dashboard — multi-topic PSI timeline over the last 90 days."""
    import threading
    from datetime import date, timedelta

    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    cutoff = (date.today() - timedelta(days=90)).isoformat()

    # Fetch predefined topics
    topics_res = admin_supabase.table("topics").select("id,name,category").is_("user_id", "null").execute()
    topics = topics_res.data or []
    topic_ids = [t["id"] for t in topics]

    pulse_data = {}
    movers = []

    if topic_ids:
        snaps_res = admin_supabase.table("daily_snapshots") \
            .select("topic_id,snapshot_date,psi_rating,dominant_emotion,total_comments") \
            .in_("topic_id", topic_ids) \
            .gte("snapshot_date", cutoff) \
            .order("snapshot_date") \
            .execute()
        snaps = snaps_res.data or []

        # Build lookup: topic_id → name
        id_to_name = {t["id"]: t["name"] for t in topics}

        # Group snapshots by topic
        by_topic = {}
        for s in snaps:
            tid = s["topic_id"]
            if tid not in by_topic:
                by_topic[tid] = []
            by_topic[tid].append(s)

        for tid, rows in by_topic.items():
            name = id_to_name.get(tid, str(tid))
            dates   = [r["snapshot_date"] for r in rows]
            psi     = [round(r["psi_rating"] or 0, 1) for r in rows]
            emotions = [r.get("dominant_emotion") or "neutral" for r in rows]
            pulse_data[name] = {"dates": dates, "psi": psi, "emotions": emotions}

            # Mover: latest vs 7 days ago
            if len(psi) >= 2:
                latest = psi[-1]
                week_ago_idx = max(0, len(psi) - 8)
                week_ago = psi[week_ago_idx]
                delta = round(latest - week_ago, 1)
                movers.append({
                    "name": name,
                    "latest_psi": latest,
                    "delta": delta,
                    "direction": "up" if delta > 0 else "down" if delta < 0 else "flat",
                })

        movers.sort(key=lambda m: abs(m["delta"]), reverse=True)
        movers = movers[:3]

        # Shift detection: topics with >15 PSI points overnight
        SHIFT_THRESHOLD = 10
        shifts = []
        for name, t in pulse_data.items():
            psi = t["psi"]
            dates = t["dates"]
            if len(psi) < 2:
                continue
            day_delta = psi[-1] - psi[-2]
            if abs(day_delta) >= SHIFT_THRESHOLD:
                shifts.append({
                    "name": name,
                    "delta": round(day_delta, 1),
                    "direction": "up" if day_delta > 0 else "down",
                    "date": dates[-1],
                    "latest_psi": round(psi[-1], 1),
                })
        shifts.sort(key=lambda x: abs(x["delta"]), reverse=True)
    else:
        shifts = []

    return render_template(
        "pulse.html",
        user=user,
        pulse_data=json.dumps(pulse_data),
        movers=movers,
        shifts=shifts,
        topics=topics,
    )


@main_bp.route("/trends/deep-dive", methods=["POST"])
def trends_deep_dive():
    """Background AJAX endpoint — computes deep-dive charts + Gemini for all sources.

    Called automatically by the browser after the initial trends page renders.
    Returns JSON consumed by initDeepDiveCharts() in the template.
    """
    import pandas as _pd
    from app.utils.insights import compute_all_insights
    from app.utils.gemini_insights import get_deep_dive_insights

    topic_id   = request.form.get("topic_id", "")
    topic_name = request.form.get("topic_name", "")

    if not topic_id:
        return {"error": "missing topic_id"}, 400

    # ── Full cache bypass (any topic with a precomputed row) ─────────────────
    try:
        _dd_row = admin_supabase.table("precomputed_charts") \
            .select("deep_dive") \
            .eq("topic_id", topic_id) \
            .maybe_single().execute()
        _dd = (_dd_row.data or {}).get("deep_dive")
        if _dd:
            print(f"[deep-dive] Full cache hit for topic_id={topic_id}")
            return _dd
    except Exception as _e:
        print(f"[deep-dive] Cache miss for topic_id={topic_id}: {_e}")
    # Falls through to live computation on cache miss

    PAGE = 1000

    all_rows = topic_cache.get(topic_id)
    if all_rows is None:
        all_rows, offset = [], 0
        while True:
            q = admin_supabase.table("comments").select(_COMMENT_COLS).eq("topic_id", topic_id)
            page = q.order("id").range(offset, offset + PAGE - 1).execute()
            if not page.data:
                break
            all_rows.extend(_flatten_comment_row(r) for r in page.data)
            if len(page.data) < PAGE:
                break
            offset += PAGE
        topic_cache.set(topic_id, all_rows)
        print(f"[cache] deep-dive: fetched and cached {len(all_rows)} rows for topic_id={topic_id}")
    else:
        print(f"[cache] deep-dive: cache hit — {len(all_rows)} rows for topic_id={topic_id}")

    if not all_rows:
        return {"error": "no data"}, 404

    df_all     = _pd.DataFrame(all_rows)
    df_reddit  = df_all[df_all["source_type"] == "reddit"].copy() \
                 if "source_type" in df_all.columns else df_all.copy()
    df_youtube = df_all[df_all["source_type"] == "youtube"].copy() \
                 if "source_type" in df_all.columns else _pd.DataFrame()

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
            print(f"[deep-dive] charts error: {e}")
            return {}

    charts_all     = _dd_charts(df_all)
    charts_reddit  = _dd_charts(df_reddit)
    charts_youtube = _dd_charts(df_youtube, label_source="youtube")

    # Compute all-sources insights once (reused by Gemini calls below)
    ins_all = compute_all_insights(df_all)

    # Source split ratio (for narrative)
    source_split = None
    if "source_type" in df_all.columns:
        counts = df_all["source_type"].value_counts(normalize=True).to_dict()
        source_split = {k: round(v, 3) for k, v in counts.items()}

    psi_rating = float(request.form.get("psi_rating", 0) or 0)

    # Gemini: separate call per source so each tab gets unique AI insights
    gemini_all = gemini_reddit = gemini_youtube = {}
    try:
        gemini_all = get_deep_dive_insights(topic_name, ins_all, charts_all)
    except Exception as e:
        print(f"[deep-dive] Gemini all error: {e}")
    try:
        if not df_reddit.empty:
            gemini_reddit = get_deep_dive_insights(topic_name, compute_all_insights(df_reddit), charts_reddit)
    except Exception as e:
        print(f"[deep-dive] Gemini reddit error: {e}")
    try:
        if not df_youtube.empty:
            gemini_youtube = get_deep_dive_insights(topic_name, compute_all_insights(df_youtube), charts_youtube)
    except Exception as e:
        print(f"[deep-dive] Gemini youtube error: {e}")

    # Narrative report (all-sources)
    narrative = ""
    try:
        from app.utils.gemini_insights import get_narrative_report
        narrative = get_narrative_report(topic_name, ins_all, psi_rating, source_split)
    except Exception as e:
        print(f"[deep-dive] Narrative error: {e}")

    # Opinion clusters (all-sources)
    clusters = []
    try:
        import random as _random
        from app.utils.gemini_insights import get_opinion_clusters
        pos_rows = [r for r in all_rows if r.get("sentiment_label") == "Positive"]
        neg_rows = [r for r in all_rows if r.get("sentiment_label") == "Negative"]
        neu_rows = [r for r in all_rows if r.get("sentiment_label") == "Neutral"]
        cluster_sample = (
            _random.sample(pos_rows, min(50, len(pos_rows))) +
            _random.sample(neg_rows, min(50, len(neg_rows))) +
            _random.sample(neu_rows, min(50, len(neu_rows)))
        )
        clusters = get_opinion_clusters(topic_name, cluster_sample)
    except Exception as e:
        print(f"[deep-dive] Clusters error: {e}")

    return {
        "charts_all":     charts_all,
        "charts_reddit":  charts_reddit,
        "charts_youtube": charts_youtube,
        "gemini":         gemini_all,
        "gemini_all":     gemini_all,
        "gemini_reddit":  gemini_reddit,
        "gemini_youtube": gemini_youtube,
        "narrative":      narrative,
        "clusters":       clusters,
    }


@main_bp.route("/trends/ask", methods=["POST"])
def trends_ask():
    """AJAX endpoint — answer a free-form question about a topic using Gemini + real comment data."""
    import pandas as _pd
    import random
    from app.utils.insights import compute_all_insights
    from app.utils.gemini_insights import ask_about_topic

    topic_id   = request.form.get("topic_id", "")
    topic_name = request.form.get("topic_name", "")
    question   = (request.form.get("question") or "").strip()
    psi_rating = float(request.form.get("psi_rating", 0) or 0)

    if not topic_id or not question:
        return {"error": "missing topic_id or question"}, 400

    if len(question) > 500:
        return {"error": "Question too long (max 500 characters)"}, 400

    PAGE = 1000
    all_rows = topic_cache.get(topic_id)
    if all_rows is None:
        all_rows, offset = [], 0
        while True:
            q = admin_supabase.table("comments").select(_COMMENT_COLS).eq("topic_id", topic_id)
            page = q.order("id").range(offset, offset + PAGE - 1).execute()
            if not page.data:
                break
            all_rows.extend(_flatten_comment_row(r) for r in page.data)
            if len(page.data) < PAGE:
                break
            offset += PAGE
        topic_cache.set(topic_id, all_rows)

    if not all_rows:
        return {"error": "no data"}, 404

    df_all = _pd.DataFrame(all_rows)
    insights = compute_all_insights(df_all)

    # Build top positive/negative comment lists (as dicts)
    top_pos = sorted(
        [r for r in all_rows if r.get("sentiment_label") == "Positive"],
        key=lambda r: r.get("score", 0) or 0, reverse=True
    )[:3]
    top_neg = sorted(
        [r for r in all_rows if r.get("sentiment_label") == "Negative"],
        key=lambda r: r.get("score", 0) or 0, reverse=True
    )[:3]

    # Stratified sample: 30 pos, 30 neg, 20 neutral
    def _sample(rows, label, n):
        subset = [r for r in rows if r.get("sentiment_label") == label]
        return random.sample(subset, min(n, len(subset)))

    sampled = _sample(all_rows, "Positive", 30) + \
              _sample(all_rows, "Negative", 30) + \
              _sample(all_rows, "Neutral", 20)
    random.shuffle(sampled)

    # Source split ratio
    source_split = None
    if "source_type" in df_all.columns:
        counts = df_all["source_type"].value_counts(normalize=True).to_dict()
        source_split = {k: round(v, 3) for k, v in counts.items()}

    answer = ask_about_topic(
        question=question,
        topic_name=topic_name,
        insights=insights,
        top_positive=top_pos,
        top_negative=top_neg,
        sampled_comments=sampled,
        psi_rating=psi_rating,
        source_split=source_split,
    )

    return {"answer": answer}


@main_bp.route("/compare", methods=["GET", "POST"])
def compare():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    current_user_id = user.get_user_id() if user else None

    # Build autocomplete suggestions: predefined + user history
    _FEATURED_IMG = {
        "Donald Trump":  "donaldtrump.png",
        "Elon Musk":     "elonmusk.png",
        "Gaza Conflict": "gazaconflict.png",
        "AI Technology": "aitechnology.png",
        "Climate Change":"climatechange.png",
    }
    suggestions = [{'name': t, 'kind': 'featured', 'img': _FEATURED_IMG.get(t, '')} for t in PREDEFINED_TOPICS]
    if user:
        try:
            res = admin_supabase.table("topics") \
                                .select("name") \
                                .eq("user_id", current_user_id) \
                                .order("created_at", desc=True) \
                                .execute()
            if res.data:
                existing_names = {s['name'] for s in suggestions}
                for row in res.data:
                    if row['name'] not in existing_names:
                        suggestions.append({'name': row['name'], 'kind': 'saved'})
        except:
            pass

    comparison_data = None
    gemini_compare  = {}
    topic_a_name = None
    topic_b_name = None

    if request.method == "POST":
        topic_a_name = (request.form.get("topic_a") or "").strip()
        topic_b_name = (request.form.get("topic_b") or "").strip()

        if topic_a_name and topic_b_name:

            def load_topic_for_compare(topic_name):
                """Return (df, info_dict) from DB or by scraping fresh (max 500 comments)."""
                def _snap_to_info(t_id, t_name):
                    snap = admin_supabase.table("daily_snapshots") \
                        .select("psi_rating, total_comments, dominant_emotion") \
                        .eq("topic_id", t_id).order("snapshot_date", desc=True).limit(1).execute()
                    info = snap.data[0] if snap.data else {}
                    info["id"]         = t_id
                    info["name"]       = t_name
                    info["rating"]     = info.pop("psi_rating", 0)
                    info["sentiment"]  = "Positive" if (info.get("rating") or 0) > 0 else ("Negative" if (info.get("rating") or 0) < 0 else "Neutral")
                    return info

                def _fetch_all_comments(tid):
                    PAGE = 1000
                    rows, off = [], 0
                    while True:
                        p = admin_supabase.table("comments").select(_COMMENT_COLS) \
                            .eq("topic_id", tid).order("id").range(off, off + PAGE - 1).execute()
                        if not p.data: break
                        rows.extend(_flatten_comment_row(r) for r in p.data)
                        if len(p.data) < PAGE: break
                        off += PAGE
                    return rows

                # 1. Predefined topic
                if topic_name in PREDEFINED_TOPICS:
                    res = admin_supabase.table("topics").select("id") \
                                        .eq("name", topic_name).is_("user_id", "null").execute()
                    if res.data:
                        t_id = res.data[0]['id']
                        rows = _fetch_all_comments(t_id)
                        if rows:
                            return pd.DataFrame(rows), _snap_to_info(t_id, topic_name)

                # 2. User's saved history
                if current_user_id:
                    res = admin_supabase.table("topics").select("id") \
                                        .eq("name", topic_name).eq("user_id", current_user_id).execute()
                    if res.data:
                        t_id = res.data[0]['id']
                        rows = _fetch_all_comments(t_id)
                        if rows:
                            return pd.DataFrame(rows), _snap_to_info(t_id, topic_name)

                # 3. Topic not found — direct user to Trends page instead of scraping fresh
                flash(f'"{topic_name}" hasn\'t been analysed yet. Run a Custom Analysis from the Trends page first, then come back to compare.')
                return None, None

            # ── Resolve topic IDs and info cheaply (no comment fetch) ──────────
            def _resolve_topic(t_name):
                """Return (topic_id, info_dict) without fetching comments."""
                def _snap(t_id):
                    snap = admin_supabase.table("daily_snapshots") \
                        .select("psi_rating, total_comments, dominant_emotion") \
                        .eq("topic_id", t_id).order("snapshot_date", desc=True).limit(1).execute()
                    info = snap.data[0] if snap.data else {}
                    info["id"]        = t_id
                    info["name"]      = t_name
                    info["rating"]    = info.pop("psi_rating", 0)
                    info["sentiment"] = "Positive" if (info.get("rating") or 0) > 0 else ("Negative" if (info.get("rating") or 0) < 0 else "Neutral")
                    count_res = admin_supabase.table("comments").select("id", count="exact").eq("topic_id", t_id).execute()
                    info["total_comments"] = count_res.count or 0
                    return info
                if t_name in PREDEFINED_TOPICS:
                    res = admin_supabase.table("topics").select("id").eq("name", t_name).is_("user_id", "null").execute()
                    if res.data:
                        return res.data[0]["id"], _snap(res.data[0]["id"])
                if current_user_id:
                    res = admin_supabase.table("topics").select("id").eq("name", t_name).eq("user_id", current_user_id).execute()
                    if res.data:
                        return res.data[0]["id"], _snap(res.data[0]["id"])
                return None, None

            topic_id_a, info_a = _resolve_topic(topic_a_name)
            topic_id_b, info_b = _resolve_topic(topic_b_name)

            if topic_id_a and topic_id_b:
                # ── Try precomputed cache bypass ─────────────────────────────
                _comp_hit = False
                try:
                    from app.utils.comparator import _align_timeseries, EMOTION_LABELS
                    _rows = admin_supabase.table("precomputed_charts") \
                        .select("topic_id, time_windows, metadata") \
                        .in_("topic_id", [topic_id_a, topic_id_b]).execute()
                    _cache = {r["topic_id"]: r for r in (_rows.data or [])}
                    _ra = _cache.get(topic_id_a, {});  _rb = _cache.get(topic_id_b, {})
                    _mda = _ra.get("metadata") or {};  _mdb = _rb.get("metadata") or {}

                    def _build_cmp_from_cache(tw_key):
                        _twa = (_ra.get("time_windows") or {}).get(tw_key, {})
                        _twb = (_rb.get("time_windows") or {}).get(tw_key, {})
                        _ca = _twa.get("all", {});  _cb = _twb.get("all", {})
                        _ia = _twa.get("insights_all", {});  _ib = _twb.get("insights_all", {})
                        if not (_ca and _cb and _ia and _ib):
                            return None
                        def _al(key):
                            a = _ca.get(key, {}); b = _cb.get(key, {})
                            return _align_timeseries(a.get("labels",[]), a.get("values",[]),
                                                     b.get("labels",[]), b.get("values",[]))
                        _tl, _va_tl, _vb_tl = _align_timeseries(
                            _ca.get("chart2_sentiment_timeline",{}).get("labels",[]),
                            _ca.get("chart2_sentiment_timeline",{}).get("datasets",{}).get("Positive",[]),
                            _cb.get("chart2_sentiment_timeline",{}).get("labels",[]),
                            _cb.get("chart2_sentiment_timeline",{}).get("datasets",{}).get("Positive",[]),
                        )
                        _vl, _va_vl, _vb_vl = _al("chart6_sentiment_volatility")
                        _mo, _va_mo, _vb_mo = _align_timeseries(
                            _ia.get("sentiment_momentum",{}).get("labels",[]),
                            _ia.get("sentiment_momentum",{}).get("values",[]),
                            _ib.get("sentiment_momentum",{}).get("labels",[]),
                            _ib.get("sentiment_momentum",{}).get("values",[]),
                        )
                        _cu, _va_cu, _vb_cu = _al("chart14_cumulative_posts")
                        def _weekly_totals(tw_all):
                            ds = tw_all.get("chart15_sentiment_by_day",{}).get("datasets",{})
                            pos = ds.get("Positive",[0]*7); neg = ds.get("Negative",[0]*7); neu = ds.get("Neutral",[0]*7)
                            return [p+n+u for p,n,u in zip(pos,neg,neu)]
                        emo_a = _ia.get("emotion_distribution",{}); emo_b = _ib.get("emotion_distribution",{})
                        map_a = dict(zip(emo_a.get("labels",[]), emo_a.get("values",[])))
                        map_b = dict(zip(emo_b.get("labels",[]), emo_b.get("values",[])))
                        emo_labels_cap = [e.capitalize() for e in EMOTION_LABELS]
                        tka = _ia.get("takeaways",{}); tkb = _ib.get("takeaways",{})
                        hrs_a = _ca.get("chart10_volume_by_hour",{}).get("values",[0]*24)
                        hrs_b = _cb.get("chart10_volume_by_hour",{}).get("values",[0]*24)
                        up_a = (_ca.get("chart3_avg_upvotes_sentiment",{}).get("values") or [0,0])[:2]
                        up_b = (_cb.get("chart3_avg_upvotes_sentiment",{}).get("values") or [0,0])[:2]
                        return {
                            "topic_a": {**info_a},
                            "topic_b": {**info_b},
                            "chart_split":     {"labels":["Positive","Negative"], "topic_a":[tka.get("pos_pct",0),tka.get("neg_pct",0)], "topic_b":[tkb.get("pos_pct",0),tkb.get("neg_pct",0)]},
                            "chart_upvotes":   {"labels":["Positive","Negative"], "topic_a":up_a, "topic_b":up_b},
                            "chart_timeline":  {"labels":_tl, "topic_a":_va_tl, "topic_b":_vb_tl},
                            "chart_volatility":{"labels":_vl, "topic_a":_va_vl, "topic_b":_vb_vl},
                            "chart_hours":     {"labels":[f'{h:02d}:00' for h in range(24)], "topic_a":hrs_a, "topic_b":hrs_b},
                            "chart_weekly":    {"labels":["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], "topic_a":_weekly_totals(_ca), "topic_b":_weekly_totals(_cb)},
                            "keywords_a":      _ia.get("keyword_split",{}),
                            "keywords_b":      _ib.get("keyword_split",{}),
                            "chart_emotions":  {"labels":emo_labels_cap, "topic_a":[round(map_a.get(e.lower(),0.0),4) for e in emo_labels_cap], "topic_b":[round(map_b.get(e.lower(),0.0),4) for e in emo_labels_cap]},
                            "chart_momentum":  {"labels":_mo, "topic_a":_va_mo, "topic_b":_vb_mo},
                            "chart_cumulative":{"labels":_cu, "topic_a":_va_cu, "topic_b":_vb_cu},
                            "chart_text_length":{"labels":["Positive","Negative"], "topic_a":_mda.get("text_length",[0,0]), "topic_b":_mdb.get("text_length",[0,0])},
                        }

                    cmp_overall = _build_cmp_from_cache("90")
                    # Build reddit/youtube variants from their respective source keys
                    def _build_cmp_source(source_key):
                        _twa = (_ra.get("time_windows") or {}).get("90", {})
                        _twb = (_rb.get("time_windows") or {}).get("90", {})
                        _ca = _twa.get(source_key, {}); _cb = _twb.get(source_key, {})
                        _ia = _twa.get(f"insights_{source_key}", {}); _ib = _twb.get(f"insights_{source_key}", {})
                        if not (_ca and _ia):
                            return None
                        def _al(key):
                            a = _ca.get(key, {}); b = _cb.get(key, {})
                            return _align_timeseries(a.get("labels",[]), a.get("values",[]),
                                                     b.get("labels",[]), b.get("values",[]))
                        _tl, _va_tl, _vb_tl = _align_timeseries(
                            _ca.get("chart2_sentiment_timeline",{}).get("labels",[]),
                            _ca.get("chart2_sentiment_timeline",{}).get("datasets",{}).get("Positive",[]),
                            _cb.get("chart2_sentiment_timeline",{}).get("labels",[]),
                            _cb.get("chart2_sentiment_timeline",{}).get("datasets",{}).get("Positive",[]),
                        )
                        _vl, _va_vl, _vb_vl = _al("chart6_sentiment_volatility")
                        _mo, _va_mo, _vb_mo = _align_timeseries(
                            _ia.get("sentiment_momentum",{}).get("labels",[]),
                            _ia.get("sentiment_momentum",{}).get("values",[]),
                            _ib.get("sentiment_momentum",{}).get("labels",[]),
                            _ib.get("sentiment_momentum",{}).get("values",[]),
                        )
                        _cu, _va_cu, _vb_cu = _al("chart14_cumulative_posts")
                        def _weekly_totals(tw_all):
                            ds = tw_all.get("chart15_sentiment_by_day",{}).get("datasets",{})
                            pos = ds.get("Positive",[0]*7); neg = ds.get("Negative",[0]*7); neu = ds.get("Neutral",[0]*7)
                            return [p+n+u for p,n,u in zip(pos,neg,neu)]
                        emo_a = _ia.get("emotion_distribution",{}); emo_b = _ib.get("emotion_distribution",{})
                        map_a = dict(zip(emo_a.get("labels",[]), emo_a.get("values",[])))
                        map_b = dict(zip(emo_b.get("labels",[]), emo_b.get("values",[])))
                        emo_labels_cap = [e.capitalize() for e in EMOTION_LABELS]
                        tka = _ia.get("takeaways",{}); tkb = _ib.get("takeaways",{})
                        hrs_a = _ca.get("chart10_volume_by_hour",{}).get("values",[0]*24)
                        hrs_b = _cb.get("chart10_volume_by_hour",{}).get("values",[0]*24)
                        up_a = (_ca.get("chart3_avg_upvotes_sentiment",{}).get("values") or [0,0])[:2]
                        up_b = (_cb.get("chart3_avg_upvotes_sentiment",{}).get("values") or [0,0])[:2]
                        return {
                            "topic_a": {**info_a},
                            "topic_b": {**info_b},
                            "chart_split":     {"labels":["Positive","Negative"], "topic_a":[tka.get("pos_pct",0),tka.get("neg_pct",0)], "topic_b":[tkb.get("pos_pct",0),tkb.get("neg_pct",0)]},
                            "chart_upvotes":   {"labels":["Positive","Negative"], "topic_a":up_a, "topic_b":up_b},
                            "chart_timeline":  {"labels":_tl, "topic_a":_va_tl, "topic_b":_vb_tl},
                            "chart_volatility":{"labels":_vl, "topic_a":_va_vl, "topic_b":_vb_vl},
                            "chart_hours":     {"labels":[f'{h:02d}:00' for h in range(24)], "topic_a":hrs_a, "topic_b":hrs_b},
                            "chart_weekly":    {"labels":["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], "topic_a":_weekly_totals(_ca), "topic_b":_weekly_totals(_cb)},
                            "keywords_a":      _ia.get("keyword_split",{}),
                            "keywords_b":      _ib.get("keyword_split",{}),
                            "chart_emotions":  {"labels":emo_labels_cap, "topic_a":[round(map_a.get(e.lower(),0.0),4) for e in emo_labels_cap], "topic_b":[round(map_b.get(e.lower(),0.0),4) for e in emo_labels_cap]},
                            "chart_momentum":  {"labels":_mo, "topic_a":_va_mo, "topic_b":_vb_mo},
                            "chart_cumulative":{"labels":_cu, "topic_a":_va_cu, "topic_b":_vb_cu},
                            "chart_text_length":{"labels":["Positive","Negative"], "topic_a":_mda.get("text_length",[0,0]), "topic_b":_mdb.get("text_length",[0,0])},
                        }

                    if cmp_overall:
                        cmp_reddit  = _build_cmp_source("reddit")
                        cmp_youtube = _build_cmp_source("youtube")
                        comparison_data = {"overall": cmp_overall, "reddit": cmp_reddit, "youtube": cmp_youtube}
                        _comp_hit = True
                        print(f"[compare] Cache hit for {topic_a_name} vs {topic_b_name}")
                except Exception as _e:
                    print(f"[compare] Cache miss: {_e}")

                if not _comp_hit:
                    # Fallback: live comment fetch
                    df_a, info_a = load_topic_for_compare(topic_a_name)
                    df_b, info_b = load_topic_for_compare(topic_b_name)
                    if df_a is not None and df_b is not None:
                        from app.utils.comparator import build_comparison_data
                        def _src(df, s):
                            if 'source_type' in df.columns:
                                sub = df[df['source_type'] == s].copy()
                                return sub if not sub.empty else pd.DataFrame(columns=df.columns)
                            return pd.DataFrame()
                        df_a_r, df_b_r = _src(df_a,'reddit'),  _src(df_b,'reddit')
                        df_a_y, df_b_y = _src(df_a,'youtube'), _src(df_b,'youtube')
                        cmp_reddit  = build_comparison_data(df_a_r,info_a,df_b_r,info_b) if not (df_a_r.empty and df_b_r.empty) else None
                        cmp_youtube = build_comparison_data(df_a_y,info_a,df_b_y,info_b) if not (df_a_y.empty and df_b_y.empty) else None
                        cmp_overall = build_comparison_data(df_a,info_a,df_b,info_b)
                        comparison_data = {"overall": cmp_overall, "reddit": cmp_reddit, "youtube": cmp_youtube}

                if comparison_data:
                    # Gemini AI overviews (one call using overall data)
                    try:
                        from app.utils.gemini_insights import get_compare_insights
                        gemini_compare = get_compare_insights(comparison_data.get("overall"), topic_a_name, topic_b_name)
                    except Exception as e:
                        print(f"[compare] Gemini error: {e}")
                        gemini_compare = {}
            else:
                if not topic_id_a:
                    flash(f"Could not load data for '{topic_a_name}'. Run a full analysis first.")
                if not topic_id_b:
                    flash(f"Could not load data for '{topic_b_name}'. Run a full analysis first.")

    return render_template(
        "compare.html",
        user=user,
        suggestions=suggestions,
        topic_a_name=topic_a_name,
        topic_b_name=topic_b_name,
        comparison_json=json.dumps(comparison_data) if comparison_data else None,
        gemini_compare=gemini_compare if comparison_data else None,
    )


@main_bp.route("/profile", methods=["GET", "POST"])
def profile():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    if not user:
        flash("You must be logged in to view your profile.")
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        first_name = request.form.get("first_name", "").strip()
        last_name  = request.form.get("last_name", "").strip()
        username   = request.form.get("username", "").strip()

        if not first_name or not last_name or not username:
            flash("All fields are required.")
        else:
            try:
                admin_supabase.table("Users").update({
                    "First Name": first_name,
                    "Last Name":  last_name,
                    "Username":   username,
                }).eq("UID", user.get_user_id()).execute()

                # Refresh session
                user.set_first_name(first_name)
                user.set_last_name(last_name)
                user.set_username(username)
                session["user"] = user.to_dict()
                flash("Profile updated successfully.")
            except Exception as e:
                flash(f"Could not update profile: {e}")

        return redirect(url_for("main.profile"))

    # Count saved scans for regular users
    scan_count = None
    if user.get_role().lower() != "admin":
        try:
            res = admin_supabase.table("topics").select("id", count="exact") \
                                .eq("user_id", user.get_user_id()).execute()
            scan_count = res.count or 0
        except:
            scan_count = 0

    return render_template("profile.html", user=user, scan_count=scan_count)


@main_bp.route("/notifications")
def notifications():
    """Notifications page — PSI shifts and completed user scans."""
    from datetime import date, timedelta

    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    # ── PSI Shifts (predefined topics, public) ──────────────────────
    SHIFT_THRESHOLD = 10
    shifts = []
    try:
        cutoff = (date.today() - timedelta(days=30)).isoformat()
        topics_res = admin_supabase.table("topics").select("id,name").is_("user_id", "null").execute()
        topics_list = topics_res.data or []
        if topics_list:
            topic_ids = [t["id"] for t in topics_list]
            snaps_res = admin_supabase.table("daily_snapshots") \
                .select("topic_id,snapshot_date,psi_rating") \
                .in_("topic_id", topic_ids) \
                .gte("snapshot_date", cutoff) \
                .order("snapshot_date") \
                .execute()
            id_to_name = {t["id"]: t["name"] for t in topics_list}
            by_topic = {}
            for s in (snaps_res.data or []):
                by_topic.setdefault(s["topic_id"], []).append(s)
            for tid, rows in by_topic.items():
                psi = [round(r["psi_rating"] or 0, 1) for r in rows]
                if len(psi) < 2:
                    continue
                day_delta = psi[-1] - psi[-2]
                if abs(day_delta) >= SHIFT_THRESHOLD:
                    shifts.append({
                        "name": id_to_name.get(tid, ""),
                        "delta": round(day_delta, 1),
                        "direction": "up" if day_delta > 0 else "down",
                        "date": rows[-1]["snapshot_date"],
                        "latest_psi": round(psi[-1], 1),
                    })
        shifts.sort(key=lambda x: abs(x["delta"]), reverse=True)
    except Exception as e:
        print(f"[notifications] shifts error: {e}")

    # ── Completed scans (user-specific) ────────────────────────────
    completed_scans = []
    if user:
        try:
            res = admin_supabase.table("topics") \
                .select("id,name,created_at") \
                .eq("user_id", user.get_user_id()) \
                .order("created_at", desc=True) \
                .execute()
            for t in (res.data or []):
                snap = admin_supabase.table("daily_snapshots") \
                    .select("snapshot_date,psi_rating") \
                    .eq("topic_id", t["id"]) \
                    .order("snapshot_date", desc=True).limit(1).execute()
                if snap.data:
                    t["completed_date"] = snap.data[0]["snapshot_date"]
                    t["psi_rating"] = round(snap.data[0]["psi_rating"] or 0, 1)
                    completed_scans.append(t)
        except Exception as e:
            print(f"[notifications] scans error: {e}")

    return render_template(
        "notifications.html",
        user=user,
        shifts=shifts,
        completed_scans=completed_scans,
    )


@main_bp.route("/api/notifications")
def api_notifications():
    """JSON endpoint for the notification side panel."""
    from datetime import date, timedelta
    from flask import jsonify

    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    SHIFT_THRESHOLD = 10
    shifts = []
    try:
        cutoff = (date.today() - timedelta(days=30)).isoformat()
        topics_res = admin_supabase.table("topics").select("id,name").is_("user_id", "null").execute()
        topics_list = topics_res.data or []
        if topics_list:
            topic_ids = [t["id"] for t in topics_list]
            snaps_res = admin_supabase.table("daily_snapshots") \
                .select("topic_id,snapshot_date,psi_rating") \
                .in_("topic_id", topic_ids) \
                .gte("snapshot_date", cutoff) \
                .order("snapshot_date") \
                .execute()
            id_to_name = {t["id"]: t["name"] for t in topics_list}
            by_topic = {}
            for s in (snaps_res.data or []):
                by_topic.setdefault(s["topic_id"], []).append(s)
            for tid, rows in by_topic.items():
                psi = [round(r["psi_rating"] or 0, 1) for r in rows]
                if len(psi) < 2:
                    continue
                day_delta = psi[-1] - psi[-2]
                if abs(day_delta) >= SHIFT_THRESHOLD:
                    shifts.append({
                        "name": id_to_name.get(tid, ""),
                        "delta": round(day_delta, 1),
                        "direction": "up" if day_delta > 0 else "down",
                        "date": rows[-1]["snapshot_date"],
                        "latest_psi": round(psi[-1], 1),
                    })
        shifts.sort(key=lambda x: abs(x["delta"]), reverse=True)
    except Exception as e:
        print(f"[api/notifications] shifts error: {e}")

    completed_scans = []
    if user:
        try:
            res = admin_supabase.table("topics") \
                .select("id,name,created_at") \
                .eq("user_id", user.get_user_id()) \
                .order("created_at", desc=True) \
                .execute()
            for t in (res.data or []):
                snap = admin_supabase.table("daily_snapshots") \
                    .select("snapshot_date,psi_rating") \
                    .eq("topic_id", t["id"]) \
                    .order("snapshot_date", desc=True).limit(1).execute()
                if snap.data:
                    completed_scans.append({
                        "name": t["name"],
                        "completed_date": snap.data[0]["snapshot_date"],
                        "psi_rating": round(snap.data[0]["psi_rating"] or 0, 1),
                    })
        except Exception as e:
            print(f"[api/notifications] scans error: {e}")

    return jsonify({"shifts": shifts, "completed_scans": completed_scans})


@main_bp.route("/history")
def history():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    if not user:
        flash("You must be logged in to view your scan history.")
        return redirect(url_for("auth.login"))

    saved_scans = []
    try:
        res = admin_supabase.table("topics") \
                            .select("id, name, category, created_at") \
                            .eq("user_id", user.get_user_id()) \
                            .order("created_at", desc=True) \
                            .execute()
        if res.data:
            for t in res.data:
                snap = admin_supabase.table("daily_snapshots") \
                    .select("psi_rating, dominant_emotion, total_comments, snapshot_date") \
                    .eq("topic_id", t["id"]) \
                    .order("snapshot_date", desc=True).limit(1).execute()
                if snap.data:
                    s = snap.data[0]
                    t["rating"]         = s.get("psi_rating", 0)
                    t["total_comments"] = s.get("total_comments", 0)
                    t["dominant_emotion"] = s.get("dominant_emotion", "neutral")
                    t["last_updated"]   = s.get("snapshot_date", t.get("created_at", ""))
                else:
                    t["rating"]         = 0
                    t["total_comments"] = 0
                    t["dominant_emotion"] = "neutral"
                    t["last_updated"]   = t.get("created_at", "")
                r = t["rating"] or 0
                t["sentiment"] = "Positive" if r > 0 else ("Negative" if r < 0 else "Neutral")
            saved_scans = res.data
    except Exception as e:
        flash(f"Could not load scan history: {e}")

    return render_template("history.html", user=user, saved_scans=saved_scans)


# --- PROGRESS API ROUTE ---
@main_bp.route("/api/fetch_progress")
def get_fetch_progress():
    from app.utils.fetcher import fetch_progress
    return jsonify(fetch_progress)


@main_bp.route("/api/topic-image")
def api_topic_image():
    """Return a Wikipedia thumbnail URL for any topic name. Cached in browser sessionStorage."""
    topic = request.args.get("topic", "").strip()
    if not topic:
        return jsonify({"url": None})
    from app.utils.topic_image import get_topic_image_url
    url = get_topic_image_url(topic)
    return jsonify({"url": url})


# --- ADMIN API ROUTES FOR TOPICS ---

@main_bp.route("/admin/api/topics", methods=["POST"])
def api_add_topic():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403
        
    data = request.json
    result = admin_service.add_topic(data)
    if result:
        return jsonify({"success": True, "data": result}), 201
    return jsonify({"error": "Failed to add topic"}), 500

@main_bp.route("/admin/api/topics/<int:topic_id>", methods=["PUT"])
def api_update_topic(topic_id):
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403
        
    data = request.json
    result = admin_service.update_topic(topic_id, data)
    if result:
        return jsonify({"success": True, "data": result}), 200
    return jsonify({"error": "Failed to update topic"}), 500

@main_bp.route("/admin/api/topics/<int:topic_id>", methods=["DELETE"])
def api_delete_topic(topic_id):
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403
        
    success = admin_service.delete_topic(topic_id)
    if success:
        return jsonify({"success": True}), 200
    return jsonify({"error": "Failed to delete topic"}), 500

import threading


def _build_yt_video_cards(df_youtube: pd.DataFrame) -> list:
    """Build per-video summary dicts for the YouTube tab video cards."""
    cards = []
    if df_youtube.empty or "source_id" not in df_youtube.columns:
        return cards
    for video_id, group in df_youtube.groupby("source_id"):
        title = group["post_title"].iloc[0] if "post_title" in group.columns else video_id
        dominant_emotion = (
            group["emotion_label"].mode()[0]
            if "emotion_label" in group.columns and not group.empty
            else "neutral"
        )
        total = len(group)
        pos_pct = round((group["sentiment_label"] == "Positive").sum() / total * 100, 1) if total else 0
        cards.append({
            "video_id":         str(video_id),
            "title":            (title[:80] + "…") if len(title) > 80 else title,
            "comment_count":    total,
            "dominant_emotion": dominant_emotion,
            "pos_pct":          pos_pct,
        })
    return sorted(cards, key=lambda x: x["comment_count"], reverse=True)


def _background_analyse_user_topic(app, user_id, topic_name):
    """Delegates to app.services.analysis_service.run_background_analysis."""
    run_background_analysis(app, user_id, topic_name)


@main_bp.route("/api/topic_status")
def api_topic_status():
    """Polling endpoint: returns the background job state for a given topic."""
    from app.utils import job_tracker

    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user:
        return jsonify({"status": "unauthenticated"}), 401

    topic = request.args.get("topic", "").strip()
    if not topic:
        return jsonify({"status": "missing_topic"}), 400

    user_id = user.get_user_id()
    status = job_tracker.get_status(user_id, topic)
    step   = job_tracker.get_step(user_id, topic)

    if status in (None, "not_found"):
        # DB fallback: if analysis completed on a different instance or after a restart,
        # check whether the topic already has data in the database.
        try:
            from app.services.supabase_client import admin_supabase
            topic_res = admin_supabase.table("topics").select("id") \
                .eq("name", topic).eq("user_id", str(user_id)).limit(1).execute()
            if topic_res.data:
                tid = topic_res.data[0]["id"]
                cnt = admin_supabase.table("comments").select("id", count="exact") \
                    .eq("topic_id", tid).limit(1).execute()
                if cnt.count and cnt.count > 0:
                    return jsonify({"status": "complete", "step": None})
        except Exception:
            pass

    return jsonify({"status": status or "not_found", "step": step})


def background_run_topic(topic_name):
    from app.utils.fetcher import get_reddit_comments
    from app.utils.hf_analyzer import process_and_store_comments

    try:
        print(f"Starting Background Analysis Thread for: {topic_name}")
        df = get_reddit_comments(topic_name, limit_posts=100, max_comments=5000, subreddit_name="all", topic_name=topic_name)
        if not df.empty:
            # Admin rerun is always for predefined topics — no user ownership
            process_and_store_comments(topic_name, df, user_id=None, mode="local")
        print(f"Background thread finished efficiently for: {topic_name}")
    except Exception as e:
        print(f"Background rerun failed for {topic_name}: {e}")

@main_bp.route("/admin/api/predefined-topics/<topic_id>", methods=["DELETE"])
def api_delete_predefined_topic(topic_id):
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    try:
        # Safety: only delete rows that are predefined (user_id IS NULL)
        res = admin_supabase.table("topics") \
                            .delete() \
                            .eq("id", topic_id) \
                            .is_("user_id", "null") \
                            .execute()
        return jsonify({"success": True}), 200
    except Exception as e:
        print(f"Error deleting predefined topic {topic_id}: {e}")
        return jsonify({"error": str(e)}), 500


@main_bp.route("/admin/api/topics/rerun", methods=["POST"])
def api_rerun_topic():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403
        
    topic_name = request.json.get("topicName")
    if not topic_name:
         return jsonify({"error": "No topic name provided"}), 400
         
    # Launch background thread to prevent API timeout
    thread = threading.Thread(target=background_run_topic, args=(topic_name,))
    thread.daemon = True # allow flask to close without locking this thread
    thread.start()
    
    return jsonify({"success": True, "message": "Analysis started safely in background"}), 200
