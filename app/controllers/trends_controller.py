"""
app/controllers/trends_controller.py
======================================
Routes: GET|POST /trends, POST /trends/deep-dive, POST /trends/ask
"""

import json
import threading
import pandas as pd
from flask import Blueprint, render_template, session, flash, redirect, url_for, request, current_app

from app.models.users import User
from app.services import topic_service, comment_service
from app.utils.cache import topic_cache
from app.utils.topic_image import get_topic_image_filename
from app.utils.visualizer import ElectionDataVisualizer
from app.services.analysis_service import run_background_analysis

trends_bp = Blueprint("trends", __name__)


# ── YouTube video-card helper ─────────────────────────────────────────────────

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
        total   = len(group)
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
    """Delegates to analysis_service.run_background_analysis."""
    run_background_analysis(app, user_id, topic_name)


# ── Cache extraction helper ───────────────────────────────────────────────────

def _unpack_cache(pc: dict, win_key: str = "90") -> dict | None:
    """
    Unpack a precomputed_charts row into the flat chart/insight variables
    expected by the template. Returns None if the cache entry is incomplete.
    """
    _tw = pc.get("time_windows") or {}
    _md = pc.get("metadata") or {}
    _win_data = _tw.get(win_key) or _tw.get("90") or {}
    if not (_tw and _win_data):
        return None
    return {
        "charts_all":      _win_data.get("all",              {}),
        "charts_reddit":   _win_data.get("reddit",           {}),
        "charts_youtube":  _win_data.get("youtube",          {}),
        "insights_all":    _win_data.get("insights_all",     {}),
        "insights_reddit": _win_data.get("insights_reddit",  {}),
        "insights_youtube":_win_data.get("insights_youtube", {}),
        "source_split":    _md.get("source_split",    {}),
        "has_youtube":     _md.get("has_youtube",     False),
        "top_positive":    _md.get("top_positive"),
        "top_negative":    _md.get("top_negative"),
        "top_positive_yt": _md.get("top_positive_yt"),
        "top_negative_yt": _md.get("top_negative_yt"),
        "yt_video_cards":  _md.get("yt_video_cards",  []),
        "time_windows":    _tw,
    }


# ── Main trends route ─────────────────────────────────────────────────────────

@trends_bp.route("/trends", methods=["GET", "POST"])
def trends():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    current_user_id = user.get_user_id() if user else None

    # Template variables with defaults
    current_topic    = None
    charts_all       = {}
    charts_reddit    = {}
    charts_youtube   = {}
    insights_all     = {}
    insights_reddit  = {}
    insights_youtube = {}
    has_youtube      = False
    source_split     = {}
    yt_video_cards   = []
    top_positive     = None
    top_negative     = None
    top_positive_yt  = None
    top_negative_yt  = None
    topic_info       = None
    job_status       = None
    has_data         = False
    current_topic_id = None
    topic_cover_url  = None
    selected_days    = 90
    is_featured      = False
    time_windows     = {}

    # Build autocomplete suggestions — predefined topics pulled from DB (cached 60 s)
    predefined_rows = topic_service.list_predefined_topics()
    predefined_set  = {r["name"] for r in predefined_rows}
    suggestions = [
        {"name": r["name"], "kind": "featured", "img": get_topic_image_filename(r["name"])}
        for r in predefined_rows
    ]
    featured_topics = [
        {"name": r["name"], "img": get_topic_image_filename(r["name"])}
        for r in predefined_rows
    ]
    if user:
        existing = {s["name"] for s in suggestions}
        for name in topic_service.get_user_topic_suggestions(current_user_id):
            if name not in existing:
                suggestions.append({"name": name, "kind": "saved", "img": ""})

    # ── Helper: fetch comments (with in-memory cache) ─────────────────────────
    def _get_cached_rows(t_id):
        rows = topic_cache.get(t_id)
        if rows is None:
            rows = comment_service.fetch_all_comments(t_id)
            topic_cache.set(t_id, rows)
            print(f"[cache] Fetched and cached {len(rows)} rows for topic_id={t_id}")
        else:
            print(f"[cache] Cache hit — {len(rows)} rows for topic_id={t_id}")
        return rows

    # ── Helper: build above-fold charts/insights from comment rows ────────────
    def extract_and_visualize(t_id, days=None):
        nonlocal charts_all, charts_reddit, charts_youtube, \
                 insights_all, insights_reddit, insights_youtube, \
                 has_youtube, source_split, yt_video_cards, has_data, \
                 top_positive, top_negative, top_positive_yt, top_negative_yt, \
                 current_topic_id

        from app.utils.insights import compute_all_insights

        current_topic_id = t_id
        all_rows = _get_cached_rows(t_id)
        if not all_rows:
            return False

        if days and days < 90:
            from datetime import datetime, timedelta, timezone
            _cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            all_rows = [r for r in all_rows if r.get("published_at") and str(r["published_at"]) >= _cutoff]
            if not all_rows:
                return False

        df_all    = pd.DataFrame(all_rows)
        df_reddit = df_all[df_all["source_type"] == "reddit"].copy() \
                    if "source_type" in df_all.columns else df_all.copy()
        df_youtube_df = df_all[df_all["source_type"] == "youtube"].copy() \
                    if "source_type" in df_all.columns else pd.DataFrame()

        has_youtube = not df_youtube_df.empty

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
                return compute_all_insights(df)
            except Exception as e:
                print(f"[insights] Error: {e}")
                return {}

        charts_all      = _safe_charts(df_all,    primary_only=True)
        charts_reddit   = _safe_charts(df_reddit, primary_only=True)
        insights_all    = _safe_insights(df_all)
        insights_reddit = _safe_insights(df_reddit)

        if has_youtube:
            charts_youtube                        = _safe_charts(df_youtube_df, primary_only=True)
            charts_youtube["yt_video_cards"]      = _build_yt_video_cards(df_youtube_df)
            yt_video_cards                        = charts_youtube["yt_video_cards"]
            insights_youtube                      = _safe_insights(df_youtube_df)
            yt_pos = df_youtube_df[df_youtube_df["sentiment_label"] == "Positive"]
            yt_neg = df_youtube_df[df_youtube_df["sentiment_label"] == "Negative"]
            if not yt_pos.empty:
                top_positive_yt = yt_pos.nlargest(1, "score").to_dict("records")[0]
            if not yt_neg.empty:
                top_negative_yt = yt_neg.nlargest(1, "score").to_dict("records")[0]

        total = max(len(df_all), 1)
        source_split = {
            "reddit_count":  len(df_reddit),
            "youtube_count": len(df_youtube_df),
            "reddit_pct":    round(len(df_reddit)      / total * 100, 1),
            "youtube_pct":   round(len(df_youtube_df)  / total * 100, 1),
        }

        pos_reddit = df_reddit[df_reddit["sentiment_label"] == "Positive"] if not df_reddit.empty else pd.DataFrame()
        neg_reddit = df_reddit[df_reddit["sentiment_label"] == "Negative"] if not df_reddit.empty else pd.DataFrame()
        if not pos_reddit.empty:
            top_positive = pos_reddit.nlargest(1, "score").to_dict("records")[0]
        if not neg_reddit.empty:
            top_negative = neg_reddit.nlargest(1, "score").to_dict("records")[0]

        has_data = True
        return True

    # ── Helper: apply precomputed cache to template vars ──────────────────────
    def _apply_cache(unpacked: dict):
        nonlocal charts_all, charts_reddit, charts_youtube, \
                 insights_all, insights_reddit, insights_youtube, \
                 source_split, has_youtube, top_positive, top_negative, \
                 top_positive_yt, top_negative_yt, yt_video_cards, \
                 current_topic_id, has_data, time_windows
        charts_all       = unpacked["charts_all"]
        charts_reddit    = unpacked["charts_reddit"]
        charts_youtube   = unpacked["charts_youtube"]
        insights_all     = unpacked["insights_all"]
        insights_reddit  = unpacked["insights_reddit"]
        insights_youtube = unpacked["insights_youtube"]
        source_split     = unpacked["source_split"]
        has_youtube      = unpacked["has_youtube"]
        top_positive     = unpacked["top_positive"]
        top_negative     = unpacked["top_negative"]
        top_positive_yt  = unpacked["top_positive_yt"]
        top_negative_yt  = unpacked["top_negative_yt"]
        yt_video_cards   = unpacked["yt_video_cards"]
        time_windows     = unpacked["time_windows"]
        current_topic_id = topic_id
        has_data         = True

    if request.method == "POST" or (request.method == "GET" and request.args.get("topic")):
        if request.method == "GET":
            topic_query    = request.args.get("topic")
            topic_id_param = None
            selected_days  = max(30, min(90, int(request.args.get("days", 90))))
        else:
            topic_query    = request.form.get("topic")
            topic_id_param = request.form.get("topic_id")
            selected_days  = max(30, min(90, int(request.form.get("days", 90))))

        if topic_query:
            current_topic = topic_query
            _lower = current_topic.lower()
            _canonical = next((t for t in predefined_set if t.lower() == _lower), None)
            if _canonical:
                current_topic = _canonical

            if current_topic not in predefined_set:
                if not user:
                    flash("You must be logged in to analyse a custom topic.")
                    return redirect(url_for("auth.login"))
                from app.api.wiki import fetch_wiki_image as _fetch_wiki_image
                topic_cover_url = _fetch_wiki_image(current_topic)

            # ── INCREMENTAL RE-ANALYSIS (from History page) ─────────────────────
            is_reanalyse = request.form.get("reanalyse") == "1" if request.method == "POST" else False
            if topic_id_param and is_reanalyse and current_user_id:
                row = topic_service.get_user_topic_by_id(int(topic_id_param), str(current_user_id))
                if row:
                    from app.utils import job_tracker
                    from app.services.analysis_service import run_incremental_analysis

                    current_topic = row["name"]
                    topic_id = row["id"]
                    since_date = topic_service.get_latest_snapshot_date(topic_id)
                    if not since_date:
                        since_date = row.get("created_at", "")[:10]

                    existing_status = job_tracker.get_status(current_user_id, current_topic)
                    if existing_status == "processing":
                        job_status = "processing"
                        print(f"[trends] Re-analysis already processing for '{current_topic}'")
                    else:
                        job_tracker.mark_processing(current_user_id, current_topic)
                        _app_obj = current_app._get_current_object()
                        threading.Thread(
                            target=run_incremental_analysis,
                            args=(_app_obj, current_user_id, current_topic, topic_id, since_date),
                            daemon=True,
                        ).start()
                        job_status = "processing"
                        print(f"[trends] Started incremental re-analysis for '{current_topic}' (topic_id={topic_id}, since={since_date})")
                # fall through to template render (shows spinner)

            # ── VIEW SAVED SCAN ───────────────────────────────────────────────
            elif topic_id_param and current_user_id:
                row = topic_service.get_user_topic_by_id(int(topic_id_param), str(current_user_id))
                if row:
                    topic_id         = row["id"]
                    current_topic    = row["name"]
                    current_topic_id = topic_id
                    info             = topic_service.get_topic_info(topic_id)
                    topic_info    = {"name": current_topic, **info}

                    pc = comment_service.load_precomputed_cache(topic_id)
                    if pc:
                        unpacked = _unpack_cache(pc, "90")
                        if unpacked:
                            _apply_cache(unpacked)
                            print(f"[trends] History cache hit for topic_id={topic_id}")
                    if not has_data:
                        if not extract_and_visualize(topic_id):
                            flash("No comment data found for this saved scan.")
                else:
                    flash("Saved scan not found or access denied.")

            # ── PREDEFINED TOPIC ──────────────────────────────────────────────
            elif current_topic in predefined_set:
                is_featured = True
                topic_id = topic_service.get_predefined_topic_id(current_topic)
                if topic_id:
                    current_topic_id = topic_id
                    info             = topic_service.get_topic_info(topic_id)
                    topic_info = {"name": current_topic, **info}

                    pc = comment_service.load_precomputed_cache(topic_id)
                    if pc:
                        unpacked = _unpack_cache(pc, str(selected_days))
                        if unpacked:
                            _apply_cache(unpacked)
                            print(f"[trends] Full cache hit for topic_id={topic_id} ({selected_days}D)")

                    if not has_data:
                        if not extract_and_visualize(topic_id, days=selected_days):
                            flash(f"No comments found for predefined topic: {current_topic}")
                        else:
                            # Build live time_windows from in-memory cache
                            from app.utils.insights import compute_all_insights
                            from datetime import datetime, timedelta, timezone
                            _cached_rows = topic_cache.get(topic_id) or []
                            time_windows = {}
                            for _win in [30, 60, 90]:
                                _cutoff_date = (
                                    datetime.now(timezone.utc) - timedelta(days=_win)
                                ).strftime("%Y-%m-%d")
                                _rows = [
                                    r for r in _cached_rows
                                    if r.get("published_at") and str(r["published_at"])[:10] >= _cutoff_date
                                ]
                                if _rows:
                                    _dfa = pd.DataFrame(_rows)
                                    _dfr = _dfa[_dfa["source_type"] == "reddit"].copy() \
                                           if "source_type" in _dfa.columns else _dfa.copy()
                                    _dfy = _dfa[_dfa["source_type"] == "youtube"].copy() \
                                           if "source_type" in _dfa.columns else pd.DataFrame()

                                    def _sc(df, po=True):
                                        if df is None or df.empty:
                                            return {}
                                        try:
                                            return ElectionDataVisualizer(df).get_all_charts_data(primary_only=po)
                                        except Exception:
                                            return {}

                                    def _si(df):
                                        if df is None or df.empty:
                                            return {}
                                        try:
                                            return compute_all_insights(df)
                                        except Exception:
                                            return {}

                                    time_windows[str(_win)] = {
                                        "all":               _sc(_dfa, po=False),
                                        "reddit":            _sc(_dfr, po=False),
                                        "youtube":           _sc(_dfy, po=False),
                                        "insights_all":      _si(_dfa),
                                        "insights_reddit":   _si(_dfr),
                                        "insights_youtube":  _si(_dfy),
                                    }
                else:
                    flash(f"No database records found for preset topic: {current_topic}. Please run the seeder script first!")

            # ── CUSTOM TOPIC ──────────────────────────────────────────────────
            else:
                from app.utils import job_tracker

                if not user:
                    flash("You must be logged in to analyse a custom topic.")
                    return redirect(url_for("auth.login"))
                if not current_user_id:
                    flash("Your session has expired. Please log in again.")
                    return redirect(url_for("auth.login"))

                existing_status = job_tracker.get_status(current_user_id, current_topic)

                if existing_status == "processing":
                    job_status = "processing"

                elif existing_status == "complete":
                    job_tracker.clear(current_user_id, current_topic)
                    topic_id = topic_service.get_user_topic_id(current_topic, str(current_user_id))
                    if topic_id:
                        current_topic_id = topic_id
                        info             = topic_service.get_topic_info(topic_id)
                        topic_info = {"name": current_topic, **info}
                        pc = comment_service.load_precomputed_cache(topic_id)
                        if pc:
                            unpacked = _unpack_cache(pc, "90")
                            if unpacked:
                                _apply_cache(unpacked)
                                print(f"[trends] Custom topic cache hit for topic_id={topic_id}")
                        if not has_data:
                            extract_and_visualize(topic_id)
                    else:
                        flash("Analysis completed but data could not be loaded. Check your History.")

                elif existing_status == "failed":
                    job_tracker.clear(current_user_id, current_topic)
                    flash(f"The background analysis for '{current_topic}' failed. Please try again.")

                elif existing_status is None:
                    topic_id = topic_service.get_user_topic_id(current_topic, str(current_user_id))
                    if topic_id:
                        current_topic_id = topic_id
                        info             = topic_service.get_topic_info(topic_id)
                        topic_info = {"name": current_topic, **info}
                        pc = comment_service.load_precomputed_cache(topic_id)
                        if pc:
                            unpacked = _unpack_cache(pc, "90")
                            if unpacked:
                                _apply_cache(unpacked)
                                print(f"[trends] Custom topic cache hit for topic_id={topic_id}")
                        if not has_data:
                            extract_and_visualize(topic_id)
                    else:
                        # Cross-user cache: reuse any recent scan of the same topic
                        xuser_id = topic_service.get_cross_user_topic(current_topic, days_back=7)
                        if xuser_id:
                            topic_id         = xuser_id
                            current_topic_id = topic_id
                            info             = topic_service.get_topic_info(topic_id)
                            topic_info = {"name": current_topic, **info}
                            pc = comment_service.load_precomputed_cache(topic_id)
                            if pc:
                                unpacked = _unpack_cache(pc, "90")
                                if unpacked:
                                    _apply_cache(unpacked)
                                    print(f"[trends] Cross-user cache hit for topic_id={topic_id}")
                            if not has_data:
                                extract_and_visualize(topic_id)
                        else:
                            # Truly new topic — kick off a background thread
                            job_tracker.mark_processing(current_user_id, current_topic)
                            app_obj = current_app._get_current_object()
                            threading.Thread(
                                target=_background_analyse_user_topic,
                                args=(app_obj, current_user_id, current_topic),
                                daemon=True,
                            ).start()
                            job_status = "processing"

    return render_template(
        "trends.html",
        user=user,
        charts_all=json.dumps(charts_all),
        insights_all=insights_all,
        charts_data=json.dumps(charts_reddit),
        charts_reddit=json.dumps(charts_reddit),
        insights_data=insights_reddit,
        insights_reddit=insights_reddit,
        charts_youtube=json.dumps(charts_youtube),
        insights_youtube=insights_youtube,
        has_youtube=has_youtube,
        source_split=source_split,
        yt_video_cards=yt_video_cards,
        top_positive=top_positive,
        top_negative=top_negative,
        top_positive_yt=top_positive_yt,
        top_negative_yt=top_negative_yt,
        current_topic=current_topic,
        topic_info=topic_info,
        job_status=job_status,
        has_data=has_data,
        topic_id_for_deep_dive=current_topic_id,
        topic_cover_url=topic_cover_url,
        suggestions=suggestions,
        featured_topics=featured_topics,
        selected_days=selected_days,
        is_featured=is_featured,
        time_windows=time_windows,
    )


# ── Deep-dive AJAX endpoint ───────────────────────────────────────────────────

@trends_bp.route("/trends/deep-dive", methods=["POST"])
def trends_deep_dive():
    """Background AJAX — computes deep-dive charts only (fast <1 s).
    Gemini AI text is fetched separately via /trends/deep-dive/gemini."""
    import pandas as _pd
    from app.utils.insights import compute_all_insights

    topic_id   = request.form.get("topic_id", "")
    topic_name = request.form.get("topic_name", "")

    if not topic_id:
        return {"error": "missing topic_id"}, 400

    # Full cache bypass — if precomputed cache has deep_dive, return everything
    pc = comment_service.load_precomputed_cache(int(topic_id))
    if pc and pc.get("deep_dive"):
        print(f"[deep-dive] Full cache hit for topic_id={topic_id}")
        return pc["deep_dive"]

    all_rows = topic_cache.get(topic_id)
    if all_rows is None:
        all_rows = comment_service.fetch_all_comments(int(topic_id))
        topic_cache.set(topic_id, all_rows)
        print(f"[cache] deep-dive: fetched and cached {len(all_rows)} rows")
    else:
        print(f"[cache] deep-dive: cache hit — {len(all_rows)} rows")

    if not all_rows:
        return {"error": "no data"}, 404

    df_all    = _pd.DataFrame(all_rows)
    df_reddit = df_all[df_all["source_type"] == "reddit"].copy() \
                if "source_type" in df_all.columns else df_all.copy()
    df_youtube = df_all[df_all["source_type"] == "youtube"].copy() \
                 if "source_type" in df_all.columns else _pd.DataFrame()

    def _dd_charts(df, label_source=None):
        if df is None or df.empty:
            return {}
        try:
            vis    = ElectionDataVisualizer(df)
            charts = vis.get_all_charts_data(primary_only=False)
            if label_source == "youtube":
                charts["chart17_community_breakdown"] = vis._get_community_breakdown(label_source="youtube")
            return charts
        except Exception as e:
            print(f"[deep-dive] charts error: {e}")
            return {}

    charts_all     = _dd_charts(df_all)
    charts_reddit  = _dd_charts(df_reddit)
    charts_youtube = _dd_charts(df_youtube, label_source="youtube")

    ins_all = compute_all_insights(df_all)

    source_split = None
    if "source_type" in df_all.columns:
        counts = df_all["source_type"].value_counts(normalize=True).to_dict()
        source_split = {k: round(v, 3) for k, v in counts.items()}

    return {
        "charts_all":     charts_all,
        "charts_reddit":  charts_reddit,
        "charts_youtube": charts_youtube,
        "gemini":         {},
        "gemini_all":     {},
        "gemini_reddit":  {},
        "gemini_youtube": {},
        "narrative":      "",
        "clusters":       [],
    }


@trends_bp.route("/trends/deep-dive/gemini", methods=["POST"])
def trends_deep_dive_gemini():
    """AJAX — returns Gemini AI insights for the deep-dive panel.
    Runs all 5 Gemini calls in parallel for speed."""
    import random as _random
    import pandas as _pd
    from concurrent.futures import ThreadPoolExecutor
    from app.utils.insights import compute_all_insights
    from app.api.gemini import get_deep_dive_insights, get_narrative_report, get_opinion_clusters

    topic_id   = request.form.get("topic_id", "")
    topic_name = request.form.get("topic_name", "")
    psi_rating = float(request.form.get("psi_rating", 0) or 0)

    if not topic_id:
        return {"error": "missing topic_id"}, 400

    # Cache check — if precomputed cache has Gemini data, return it
    pc = comment_service.load_precomputed_cache(int(topic_id))
    if pc and pc.get("deep_dive"):
        dd = pc["deep_dive"]
        cached_gemini = {
            "gemini_all":     dd.get("gemini_all") or dd.get("gemini") or {},
            "gemini_reddit":  dd.get("gemini_reddit", {}),
            "gemini_youtube": dd.get("gemini_youtube", {}),
            "narrative":      dd.get("narrative", ""),
            "clusters":       dd.get("clusters", []),
        }
        if any(v for v in cached_gemini.values()):
            print(f"[deep-dive/gemini] Cache hit for topic_id={topic_id}")
            return cached_gemini

    # Load comments
    all_rows = topic_cache.get(topic_id)
    if all_rows is None:
        all_rows = comment_service.fetch_all_comments(int(topic_id))
        topic_cache.set(topic_id, all_rows)
    if not all_rows:
        return {"error": "no data"}, 404

    df_all     = _pd.DataFrame(all_rows)
    df_reddit  = df_all[df_all["source_type"] == "reddit"].copy() \
                 if "source_type" in df_all.columns else df_all.copy()
    df_youtube = df_all[df_all["source_type"] == "youtube"].copy() \
                 if "source_type" in df_all.columns else _pd.DataFrame()

    ins_all = compute_all_insights(df_all)

    source_split = None
    if "source_type" in df_all.columns:
        counts = df_all["source_type"].value_counts(normalize=True).to_dict()
        source_split = {k: round(v, 3) for k, v in counts.items()}

    # Build chart data for Gemini context (lightweight — no full viz)
    def _dd_charts(df, label_source=None):
        if df is None or df.empty:
            return {}
        try:
            vis    = ElectionDataVisualizer(df)
            charts = vis.get_all_charts_data(primary_only=False)
            if label_source == "youtube":
                charts["chart17_community_breakdown"] = vis._get_community_breakdown(label_source="youtube")
            return charts
        except Exception:
            return {}

    charts_all     = _dd_charts(df_all)
    charts_reddit  = _dd_charts(df_reddit)
    charts_youtube = _dd_charts(df_youtube, label_source="youtube")

    ins_reddit  = compute_all_insights(df_reddit) if not df_reddit.empty else {}
    ins_youtube = compute_all_insights(df_youtube) if not df_youtube.empty else {}

    # Build cluster sample
    pos_rows = [r for r in all_rows if r.get("sentiment_label") == "Positive"]
    neg_rows = [r for r in all_rows if r.get("sentiment_label") == "Negative"]
    neu_rows = [r for r in all_rows if r.get("sentiment_label") == "Neutral"]
    cluster_sample = (
        _random.sample(pos_rows, min(50, len(pos_rows))) +
        _random.sample(neg_rows, min(50, len(neg_rows))) +
        _random.sample(neu_rows, min(50, len(neu_rows)))
    )

    # Run all 5 Gemini calls IN PARALLEL
    print(f"[deep-dive/gemini] Launching 5 parallel Gemini calls for '{topic_name}'")

    def _safe(fn, *args, default=None):
        try:
            return fn(*args)
        except Exception as e:
            print(f"[deep-dive/gemini] Error in {fn.__name__}: {e}")
            return default if default is not None else {}

    with ThreadPoolExecutor(max_workers=5) as ex:
        fut_all       = ex.submit(_safe, get_deep_dive_insights, topic_name, ins_all, charts_all)
        fut_reddit    = ex.submit(_safe, get_deep_dive_insights, topic_name, ins_reddit, charts_reddit) \
                        if not df_reddit.empty else None
        fut_youtube   = ex.submit(_safe, get_deep_dive_insights, topic_name, ins_youtube, charts_youtube) \
                        if not df_youtube.empty else None
        fut_narrative = ex.submit(_safe, get_narrative_report, topic_name, ins_all, psi_rating, source_split, default="")
        fut_clusters  = ex.submit(_safe, get_opinion_clusters, topic_name, cluster_sample, default=[])

    result = {
        "gemini_all":     fut_all.result(),
        "gemini_reddit":  fut_reddit.result() if fut_reddit else {},
        "gemini_youtube": fut_youtube.result() if fut_youtube else {},
        "narrative":      fut_narrative.result(),
        "clusters":       fut_clusters.result(),
    }
    print(f"[deep-dive/gemini] All Gemini calls complete for '{topic_name}'")
    return result


# ── Ask AJAX endpoint ─────────────────────────────────────────────────────────

@trends_bp.route("/trends/ask", methods=["POST"])
def trends_ask():
    """AJAX — answer a free-form question about a topic using Gemini + comment data."""
    import random
    import pandas as _pd
    from app.utils.insights import compute_all_insights
    from app.api.gemini import ask_about_topic

    topic_id   = request.form.get("topic_id", "")
    topic_name = request.form.get("topic_name", "")
    question   = (request.form.get("question") or "").strip()
    psi_rating = float(request.form.get("psi_rating", 0) or 0)

    if not topic_id or not question:
        return {"error": "missing topic_id or question"}, 400
    if len(question) > 500:
        return {"error": "Question too long (max 500 characters)"}, 400

    all_rows = topic_cache.get(topic_id)
    if all_rows is None:
        all_rows = comment_service.fetch_all_comments(int(topic_id))
        topic_cache.set(topic_id, all_rows)

    if not all_rows:
        return {"error": "no data"}, 404

    df_all   = _pd.DataFrame(all_rows)
    insights = compute_all_insights(df_all)

    top_pos = sorted(
        [r for r in all_rows if r.get("sentiment_label") == "Positive"],
        key=lambda r: r.get("score", 0) or 0, reverse=True,
    )[:3]
    top_neg = sorted(
        [r for r in all_rows if r.get("sentiment_label") == "Negative"],
        key=lambda r: r.get("score", 0) or 0, reverse=True,
    )[:3]

    def _sample(rows, label, n):
        subset = [r for r in rows if r.get("sentiment_label") == label]
        return random.sample(subset, min(n, len(subset)))

    sampled = _sample(all_rows, "Positive", 30) + \
              _sample(all_rows, "Negative", 30) + \
              _sample(all_rows, "Neutral",  20)
    random.shuffle(sampled)

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
