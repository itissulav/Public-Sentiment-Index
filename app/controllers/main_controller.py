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
    "Donald Trump", "The Boys", "Avengers Doomsday", "Macbook Neo", "America vs Iran",
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

    # Build search autocomplete suggestions: predefined + user history
    _TRENDS_IMG = {
        "Donald Trump":      "donaldtrump.png",
        "The Boys":          "theboys.png",
        "Avengers Doomsday": "avengersdoomsday.png",
        "Macbook Neo":       "macbookneo.png",
        "America vs Iran":   "americavsiran.png",
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

    if request.method == "POST":
        topic_query    = request.form.get("topic")
        topic_id_param = request.form.get("topic_id")   # set by View Analysis on history page

        if topic_query:
            current_topic = topic_query
            # Fetch a cover image for non-predefined (custom) topics
            if current_topic not in _PREDEFINED_TOPICS:
                topic_cover_url = _fetch_wiki_image(current_topic)

            # Helper to fetch, split by source, and build above-fold chart/insight bundles.
            # Deep-dive charts and Gemini are deferred to /trends/deep-dive (background AJAX).
            def extract_and_visualize(t_id):
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
                    if not extract_and_visualize(topic_id):
                        flash("No comment data found for this saved scan.")
                else:
                    flash("Saved scan not found or access denied.")

            elif current_topic in PREDEFINED_TOPICS:
                # 1. PREDEFINED TOPIC: Pull dynamically from relational DB schema
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
                    if not extract_and_visualize(topic_id):
                        flash(f"No comments found for predefined topic: {current_topic}")
                else:
                    flash(f"No database records found for preset topic: {current_topic}. Please run the seeder script first!")
                    
            else:
                # 2. CUSTOM TOPIC — run in background so the user isn't blocked
                from app.utils import job_tracker

                if not user:
                    flash("You must be logged in to analyse a custom topic.")
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
                            extract_and_visualize(topic_id)
                        else:
                            # Truly new topic — kick off a new background thread
                            job_tracker.mark_processing(current_user_id, current_topic)
                            app = current_app._get_current_object()
                            user_email     = user.get_email()
                            user_firstname = user.get_first_name()

                            thread = threading.Thread(
                                target=_background_analyse_user_topic,
                                args=(app, current_user_id, user_email, user_firstname, current_topic),
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
    )


PREDEFINED_TOPICS = ["Donald Trump", "The Boys", "Avengers Doomsday", "Macbook Neo", "America vs Iran"]


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
        "Donald Trump":      "donaldtrump.png",
        "The Boys":          "theboys.png",
        "Avengers Doomsday": "avengersdoomsday.png",
        "Macbook Neo":       "macbookneo.png",
        "America vs Iran":   "americavsiran.png",
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

                # 3. New topic — requires login to save
                if not current_user_id:
                    flash(f"Please log in to analyse a new topic: '{topic_name}'")
                    return None, None

                from app.utils.fetcher import get_reddit_comments
                from app.utils.hf_analyzer import process_and_store_comments

                df = get_reddit_comments(topic_name, limit_posts=100, max_comments=500,
                                         subreddit_name="all", topic_name=topic_name)
                if df.empty:
                    return None, None

                process_and_store_comments(topic_name, df, current_user_id, mode="local")

                res = admin_supabase.table("topics").select("id") \
                                    .eq("name", topic_name).eq("user_id", current_user_id).execute()
                if res.data:
                    t_id = res.data[0]['id']
                    rows = _fetch_all_comments(t_id)
                    if rows:
                        return pd.DataFrame(rows), _snap_to_info(t_id, topic_name)

                return None, None

            df_a, info_a = load_topic_for_compare(topic_a_name)
            df_b, info_b = load_topic_for_compare(topic_b_name)

            if df_a is not None and df_b is not None:
                from app.utils.comparator import build_comparison_data

                def _src(df, s):
                    if 'source_type' in df.columns:
                        sub = df[df['source_type'] == s].copy()
                        return sub if not sub.empty else pd.DataFrame(columns=df.columns)
                    return pd.DataFrame()

                df_a_r, df_b_r = _src(df_a, 'reddit'),  _src(df_b, 'reddit')
                df_a_y, df_b_y = _src(df_a, 'youtube'), _src(df_b, 'youtube')
                cmp_reddit  = build_comparison_data(df_a_r, info_a, df_b_r, info_b) if not (df_a_r.empty and df_b_r.empty) else None
                cmp_youtube = build_comparison_data(df_a_y, info_a, df_b_y, info_b) if not (df_a_y.empty and df_b_y.empty) else None
                cmp_overall = build_comparison_data(df_a, info_a, df_b, info_b)
                comparison_data = {
                    'overall': cmp_overall,
                    'reddit':  cmp_reddit,
                    'youtube': cmp_youtube,
                }

                # Gemini AI overviews (one call using overall data)
                try:
                    from app.utils.gemini_insights import get_compare_insights
                    gemini_compare = get_compare_insights(cmp_overall, topic_a_name, topic_b_name)
                except Exception as e:
                    print(f"[compare] Gemini error: {e}")
                    gemini_compare = {}
            else:
                if df_a is None:
                    flash(f"Could not load data for '{topic_a_name}'. Run a full analysis first.")
                if df_b is None:
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


def _background_analyse_user_topic(app, user_id, user_email, first_name, topic_name):
    """Delegates to app.services.analysis_service.run_background_analysis."""
    run_background_analysis(app, user_id, user_email, first_name, topic_name)


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

    status = job_tracker.get_status(user.get_user_id(), topic)
    return jsonify({"status": status or "not_found"})


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
