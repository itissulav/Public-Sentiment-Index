"""
app/controllers/compare_controller.py
=======================================
Routes: GET|POST /compare
"""

import json
import pandas as pd
from flask import Blueprint, render_template, session, flash, request
from app.models.users import User
from app.services import topic_service, comment_service
from app.utils.topic_image import get_topic_image_filename

compare_bp = Blueprint("compare", __name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _snap_to_info(topic_id: int, topic_name: str) -> dict:
    """Return snapshot info dict for a topic."""
    info = topic_service.get_topic_info(topic_id)
    info["name"] = topic_name
    return info


def _resolve_topic(topic_name: str, current_user_id) -> tuple:
    """Return (topic_id, info_dict) without fetching comments. Returns (None, None) if not found."""
    if topic_name in topic_service.get_predefined_names_set():
        tid = topic_service.get_predefined_topic_id(topic_name)
        if tid:
            return tid, _snap_to_info(tid, topic_name)
    if current_user_id:
        tid = topic_service.get_user_topic_id(topic_name, str(current_user_id))
        if tid:
            return tid, _snap_to_info(tid, topic_name)
    return None, None


def _load_topic_for_compare(topic_name: str, current_user_id) -> tuple:
    """Return (df, info_dict) by fetching all comments. Returns (None, None) on miss."""
    if topic_name in topic_service.get_predefined_names_set():
        tid = topic_service.get_predefined_topic_id(topic_name)
        if tid:
            rows = comment_service.fetch_all_comments(tid)
            if rows:
                return pd.DataFrame(rows), _snap_to_info(tid, topic_name)

    if current_user_id:
        tid = topic_service.get_user_topic_id(topic_name, str(current_user_id))
        if tid:
            rows = comment_service.fetch_all_comments(tid)
            if rows:
                return pd.DataFrame(rows), _snap_to_info(tid, topic_name)

    flash(
        f'"{topic_name}" hasn\'t been analysed yet. '
        "Run a Custom Analysis from the Trends page first, then come back to compare."
    )
    return None, None


def _build_cmp_dict(ca, cb, ia, ib, mda, mdb, info_a, info_b):
    """
    Assemble the comparison dict from precomputed chart/insight dicts.
    Returns None if required data is missing.
    """
    if not (ca and cb and ia and ib):
        return None

    from app.utils.comparator import _align_timeseries, EMOTION_LABELS

    def _al(key):
        a = ca.get(key, {}); b = cb.get(key, {})
        return _align_timeseries(
            a.get("labels", []), a.get("values", []),
            b.get("labels", []), b.get("values", []),
        )

    _tl, _va_tl, _vb_tl = _align_timeseries(
        ca.get("chart2_sentiment_timeline", {}).get("labels", []),
        ca.get("chart2_sentiment_timeline", {}).get("datasets", {}).get("Positive", []),
        cb.get("chart2_sentiment_timeline", {}).get("labels", []),
        cb.get("chart2_sentiment_timeline", {}).get("datasets", {}).get("Positive", []),
    )
    _vl,  _va_vl,  _vb_vl  = _al("chart6_sentiment_volatility")
    _mo,  _va_mo,  _vb_mo  = _align_timeseries(
        ia.get("sentiment_momentum", {}).get("labels", []),
        ia.get("sentiment_momentum", {}).get("values", []),
        ib.get("sentiment_momentum", {}).get("labels", []),
        ib.get("sentiment_momentum", {}).get("values", []),
    )
    _cu,  _va_cu,  _vb_cu  = _al("chart14_cumulative_posts")

    def _weekly_totals(tw_all):
        ds  = tw_all.get("chart15_sentiment_by_day", {}).get("datasets", {})
        pos = ds.get("Positive", [0] * 7)
        neg = ds.get("Negative", [0] * 7)
        neu = ds.get("Neutral",  [0] * 7)
        return [p + n + u for p, n, u in zip(pos, neg, neu)]

    emo_a = ia.get("emotion_distribution", {}); emo_b = ib.get("emotion_distribution", {})
    map_a = dict(zip(emo_a.get("labels", []), emo_a.get("values", [])))
    map_b = dict(zip(emo_b.get("labels", []), emo_b.get("values", [])))
    emo_labels_cap = [e.capitalize() for e in EMOTION_LABELS]

    tka = ia.get("takeaways", {}); tkb = ib.get("takeaways", {})
    hrs_a = ca.get("chart10_volume_by_hour", {}).get("values", [0] * 24)
    hrs_b = cb.get("chart10_volume_by_hour", {}).get("values", [0] * 24)
    up_a  = (ca.get("chart3_avg_upvotes_sentiment", {}).get("values") or [0, 0])[:2]
    up_b  = (cb.get("chart3_avg_upvotes_sentiment", {}).get("values") or [0, 0])[:2]

    return {
        "topic_a": {**info_a},
        "topic_b": {**info_b},
        "chart_split":      {"labels": ["Positive", "Negative"],
                             "topic_a": [tka.get("pos_pct", 0), tka.get("neg_pct", 0)],
                             "topic_b": [tkb.get("pos_pct", 0), tkb.get("neg_pct", 0)]},
        "chart_upvotes":    {"labels": ["Positive", "Negative"], "topic_a": up_a, "topic_b": up_b},
        "chart_timeline":   {"labels": _tl, "topic_a": _va_tl, "topic_b": _vb_tl},
        "chart_volatility": {"labels": _vl, "topic_a": _va_vl, "topic_b": _vb_vl},
        "chart_hours":      {"labels": [f"{h:02d}:00" for h in range(24)], "topic_a": hrs_a, "topic_b": hrs_b},
        "chart_weekly":     {"labels": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
                             "topic_a": _weekly_totals(ca), "topic_b": _weekly_totals(cb)},
        "keywords_a":       ia.get("keyword_split", {}),
        "keywords_b":       ib.get("keyword_split", {}),
        "chart_emotions":   {"labels": emo_labels_cap,
                             "topic_a": [round(map_a.get(e.lower(), 0.0), 4) for e in emo_labels_cap],
                             "topic_b": [round(map_b.get(e.lower(), 0.0), 4) for e in emo_labels_cap]},
        "chart_momentum":   {"labels": _mo, "topic_a": _va_mo, "topic_b": _vb_mo},
        "chart_cumulative": {"labels": _cu, "topic_a": _va_cu, "topic_b": _vb_cu},
        "chart_text_length":{"labels": ["Positive", "Negative"],
                             "topic_a": mda.get("text_length", [0, 0]),
                             "topic_b": mdb.get("text_length", [0, 0])},
    }


# ── Route ─────────────────────────────────────────────────────────────────────

@compare_bp.route("/compare", methods=["GET", "POST"])
def compare():
    from flask import redirect, url_for, flash

    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    # Compare page requires login - redirect guests to login
    if not user:
        flash("You must be logged in to compare topics.")
        return redirect(url_for("auth.login"))

    current_user_id = user.get_user_id() if user else None

    predefined_rows = topic_service.list_predefined_topics()
    suggestions = [
        {"name": r["name"], "kind": "featured", "img": get_topic_image_filename(r["name"])}
        for r in predefined_rows
    ]
    if user:
        existing = {s["name"] for s in suggestions}
        for name in topic_service.get_user_topic_suggestions(current_user_id):
            if name not in existing:
                suggestions.append({"name": name, "kind": "saved"})

    comparison_data = None
    gemini_compare  = {}
    topic_a_name    = None
    topic_b_name    = None

    if request.method == "POST":
        topic_a_name = (request.form.get("topic_a") or "").strip()
        topic_b_name = (request.form.get("topic_b") or "").strip()

        if topic_a_name.lower() == topic_b_name.lower() and topic_a_name:
            flash("Please select two different topics to compare.")
            return render_template(
                "compare.html",
                user=user,
                suggestions=suggestions,
                topic_a_name=topic_a_name,
                topic_b_name=topic_b_name,
                comparison_json=None,
                gemini_compare={},
            )

        if topic_a_name and topic_b_name:
            topic_id_a, info_a = _resolve_topic(topic_a_name, current_user_id)
            topic_id_b, info_b = _resolve_topic(topic_b_name, current_user_id)

            if topic_id_a and topic_id_b:
                # Try precomputed cache bypass
                _comp_hit = False
                try:
                    from app.utils.comparator import EMOTION_LABELS  # noqa: F401 (side effect: import check)
                    pc_a = comment_service.load_precomputed_cache(topic_id_a)
                    pc_b = comment_service.load_precomputed_cache(topic_id_b)

                    if pc_a and pc_b:
                        _mda = pc_a.get("metadata") or {}
                        _mdb = pc_b.get("metadata") or {}

                        def _build_from_cache(tw_key, src_key="all", ins_key="insights_all"):
                            _twa = (pc_a.get("time_windows") or {}).get(tw_key, {})
                            _twb = (pc_b.get("time_windows") or {}).get(tw_key, {})
                            _ca  = _twa.get(src_key, {})
                            _cb  = _twb.get(src_key, {})
                            _ia  = _twa.get(ins_key, {})
                            _ib  = _twb.get(ins_key, {})
                            return _build_cmp_dict(_ca, _cb, _ia, _ib, _mda, _mdb, info_a, info_b)

                        cmp_overall = _build_from_cache("90", "all", "insights_all")
                        if cmp_overall:
                            cmp_reddit  = _build_from_cache("90", "reddit",  "insights_reddit")
                            cmp_youtube = _build_from_cache("90", "youtube", "insights_youtube")
                            comparison_data = {
                                "overall": cmp_overall,
                                "reddit":  cmp_reddit,
                                "youtube": cmp_youtube,
                            }
                            _comp_hit = True
                            print(f"[compare] Cache hit for {topic_a_name} vs {topic_b_name}")
                except Exception as _e:
                    print(f"[compare] Cache miss: {_e}")

                if not _comp_hit:
                    df_a, info_a = _load_topic_for_compare(topic_a_name, current_user_id)
                    df_b, info_b = _load_topic_for_compare(topic_b_name, current_user_id)
                    if df_a is not None and df_b is not None:
                        from app.utils.comparator import build_comparison_data

                        def _src(df, s):
                            if "source_type" in df.columns:
                                sub = df[df["source_type"] == s].copy()
                                return sub if not sub.empty else pd.DataFrame(columns=df.columns)
                            return pd.DataFrame()

                        df_a_r, df_b_r = _src(df_a, "reddit"),  _src(df_b, "reddit")
                        df_a_y, df_b_y = _src(df_a, "youtube"), _src(df_b, "youtube")
                        cmp_reddit  = build_comparison_data(df_a_r, info_a, df_b_r, info_b) \
                                      if not (df_a_r.empty and df_b_r.empty) else None
                        cmp_youtube = build_comparison_data(df_a_y, info_a, df_b_y, info_b) \
                                      if not (df_a_y.empty and df_b_y.empty) else None
                        cmp_overall = build_comparison_data(df_a, info_a, df_b, info_b)
                        comparison_data = {
                            "overall": cmp_overall,
                            "reddit":  cmp_reddit,
                            "youtube": cmp_youtube,
                        }

                if comparison_data:
                    try:
                        from app.api.gemini import get_compare_insights
                        gemini_compare = get_compare_insights(
                            comparison_data.get("overall"), topic_a_name, topic_b_name
                        )
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
