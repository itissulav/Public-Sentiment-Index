"""
app/services/comment_service.py
================================
All Supabase DB operations for comments, posts, topic_sources, daily_snapshots,
and precomputed_charts.

Centralises the paginated fetch loop and DB write logic previously scattered
across api/huggingface.py, utils/chart_cache.py, and main_controller.py.
"""

import json
import pandas as pd
from datetime import date
from app.services.supabase_client import admin_supabase

# PostgREST nested-select string — same across all callers
COMMENT_COLS = (
    "id,topic_id,text,author,score,published_at,"
    "sentiment_label,emotion_label,emotion_scores,confidence_score,"
    "posts(external_post_id,title,topic_sources(source_type,source_id))"
)
_PAGE = 1000


def flatten_comment_row(row: dict) -> dict:
    """Flatten nested posts → topic_sources join into a flat dict."""
    post = row.pop("posts", None) or {}
    ts   = (post.pop("topic_sources", None) or {}) if post else {}
    row["post_id"]     = post.get("external_post_id", "")
    row["post_title"]  = post.get("title", "")
    row["source_type"] = ts.get("source_type", "")
    row["source_id"]   = ts.get("source_id", "")
    return row


def fetch_all_comments(topic_id: int) -> list:
    """
    Paginate through all comments for a topic, returning flat dicts.
    Works around Supabase's 1000-row hard cap.
    """
    rows, offset = [], 0
    try:
        while True:
            page = admin_supabase.table("comments") \
                .select(COMMENT_COLS) \
                .eq("topic_id", topic_id) \
                .order("id").range(offset, offset + _PAGE - 1).execute()
            if not page.data:
                break
            rows.extend(flatten_comment_row(r) for r in page.data)
            if len(page.data) < _PAGE:
                break
            offset += _PAGE
    except Exception as e:
        print(f"[comment_service] fetch_all_comments error: {e}")
    return rows


def get_comment_count(topic_id: int) -> int:
    """Return exact comment count for a topic."""
    try:
        res = admin_supabase.table("comments").select("id", count="exact") \
            .eq("topic_id", topic_id).execute()
        return res.count or 0
    except Exception:
        return 0


def load_precomputed_cache(topic_id: int) -> dict | None:
    """
    Load the full precomputed_charts row for a topic.
    Returns the row dict {time_windows, metadata, deep_dive} or None.
    """
    try:
        res = admin_supabase.table("precomputed_charts") \
            .select("time_windows, metadata, deep_dive") \
            .eq("topic_id", topic_id).maybe_single().execute()
        return res.data or None
    except Exception:
        return None


def store_precomputed_cache(topic_id: int, time_windows: dict, metadata: dict, deep_dive: dict):
    """Upsert the precomputed_charts row for a topic."""
    def _safe_json(obj):
        return json.loads(json.dumps(obj, default=str))

    admin_supabase.table("precomputed_charts").upsert(
        {
            "topic_id":     topic_id,
            "time_windows": _safe_json(time_windows),
            "metadata":     _safe_json(metadata),
            "deep_dive":    _safe_json(deep_dive),
        },
        on_conflict="topic_id",
    ).execute()


# ──────────────────────────────────────────────────────────────────────────────
# Topic / source / post upserts (moved from api/huggingface.py)
# ──────────────────────────────────────────────────────────────────────────────

def get_or_create_topic(topic_name: str, user_id=None) -> int:
    """Return the topic id, creating the row if it doesn't exist."""
    if user_id is None:
        res = admin_supabase.table("topics").select("id") \
                      .eq("name", topic_name).is_("user_id", "null").execute()
    else:
        res = admin_supabase.table("topics").select("id") \
                      .eq("name", topic_name).eq("user_id", user_id).execute()

    if res.data:
        return res.data[0]["id"]

    insert = admin_supabase.table("topics").insert({
        "name": topic_name,
        "user_id": user_id,
    }).execute()
    return insert.data[0]["id"]


def get_or_create_topic_source(topic_id: int, source_type: str, source_id_val: str) -> int:
    """Upsert topic_source row and return its id."""
    res = admin_supabase.table("topic_sources").upsert(
        {"topic_id": topic_id, "source_type": source_type, "source_id": source_id_val},
        on_conflict="topic_id,source_type,source_id",
    ).execute()
    return res.data[0]["id"]


def get_existing_external_post_ids(topic_id: int) -> set:
    """Return set of external_post_id values already stored for this topic."""
    try:
        res = admin_supabase.table("topic_sources") \
            .select("id") \
            .eq("topic_id", topic_id).execute()
        source_ids = [r["id"] for r in (res.data or [])]
        if not source_ids:
            return set()
        res2 = admin_supabase.table("posts") \
            .select("external_post_id") \
            .in_("topic_source_id", source_ids).execute()
        return {r["external_post_id"] for r in (res2.data or [])}
    except Exception as e:
        print(f"[comment_service] get_existing_external_post_ids error: {e}")
        return set()


def bulk_upsert_posts(topic_source_id: int, post_rows: list) -> dict:
    """
    Upsert posts records and return {external_post_id: posts.id} mapping.
    post_rows: [{"external_post_id": str, "title": str}, ...]
    """
    if not post_rows:
        return {}
    records = [
        {"topic_source_id": topic_source_id,
         "external_post_id": r["external_post_id"],
         "title": r["title"]}
        for r in post_rows
    ]
    res = admin_supabase.table("posts").upsert(
        records, on_conflict="topic_source_id,external_post_id",
    ).execute()
    return {r["external_post_id"]: r["id"] for r in res.data}


def store_classified_comments(db_records: list, fetch_progress: dict = None):
    """
    Batch-insert classified comment records into the comments table.
    Optionally updates fetch_progress dict for progress tracking.
    """
    total_records = len(db_records)
    batch_size = 500
    inserted = 0

    for i in range(0, total_records, batch_size):
        try:
            admin_supabase.table("comments").insert(db_records[i:i + batch_size]).execute()
            inserted += len(db_records[i:i + batch_size])
            if fetch_progress is not None:
                fetch_progress["current"] = inserted
                fetch_progress["message"] = f"Saving results... ({inserted}/{total_records} comments)"
        except Exception as e:
            print(f"[comment_service] Supabase insert error (batch {i}): {e}")

    print(f"[comment_service] Inserted {inserted}/{total_records} records.")
    return inserted


def upsert_snapshot(topic_id: int, df_classified: pd.DataFrame):
    """
    Compute daily PSI snapshot from classified DataFrame and upsert to daily_snapshots.
    """
    today = date.today().isoformat()
    if df_classified.empty:
        return

    try:
        total_res = admin_supabase.table("comments").select("id", count="exact") \
            .eq("topic_id", topic_id).execute()
        pos_res   = admin_supabase.table("comments").select("id", count="exact") \
            .eq("topic_id", topic_id).eq("sentiment_label", "Positive").execute()
        neg_res   = admin_supabase.table("comments").select("id", count="exact") \
            .eq("topic_id", topic_id).eq("sentiment_label", "Negative").execute()
        actual_total = total_res.count or 0
        actual_pos   = pos_res.count   or 0
        actual_neg   = neg_res.count   or 0
    except Exception:
        actual_total = len(df_classified)
        actual_pos   = int((df_classified["sentiment_label"] == "Positive").sum())
        actual_neg   = int((df_classified["sentiment_label"] == "Negative").sum())

    if actual_total == 0:
        return

    emotion_cols = ["positive", "negative", "neutral"]
    emotion_dist = {}
    for col in emotion_cols:
        if col in df_classified.columns:
            emotion_dist[col] = round(float(df_classified[col].mean()), 4)

    dominant_emotion = max(emotion_dist, key=emotion_dist.get) if emotion_dist else "neutral"

    volume_score = (actual_pos - actual_neg) / actual_total

    df_c = df_classified.copy()
    df_c["score"] = df_c.get("score", pd.Series(0, index=df_c.index)).fillna(0).astype(int)
    df_c["confidence_score"] = df_c.get("confidence_score", pd.Series(0.5, index=df_c.index)).fillna(0.5).astype(float)
    df_c["direction"] = df_c["sentiment_label"].map({"Positive": 1, "Negative": -1, "Neutral": 0}).fillna(0).astype(int)

    abs_votes = df_c["score"].abs() + 1
    eng_weights = abs_votes * df_c["confidence_score"] * df_c["direction"]
    total_abs_w = (abs_votes * df_c["confidence_score"]).sum()
    engagement_score = float(eng_weights.sum() / total_abs_w) if total_abs_w > 0 else volume_score

    pos_mask = df_c["sentiment_label"] == "Positive"
    neg_mask = df_c["sentiment_label"] == "Negative"
    has_emo  = "positive" in df_c.columns and "negative" in df_c.columns
    if has_emo and pos_mask.any() and neg_mask.any():
        avg_pos_conf = float(df_c.loc[pos_mask, "positive"].mean())
        avg_neg_conf = float(df_c.loc[neg_mask, "negative"].mean())
        intensity_score = avg_pos_conf - avg_neg_conf
    else:
        intensity_score = volume_score

    psi_rating = round((volume_score * 40) + (engagement_score * 40) + (intensity_score * 20), 2)
    psi_rating = max(-100.0, min(100.0, psi_rating))

    admin_supabase.table("daily_snapshots").upsert({
        "topic_id":             topic_id,
        "snapshot_date":        today,
        "total_comments":       actual_total,
        "positive_pct":         round(actual_pos / actual_total * 100, 2),
        "negative_pct":         round(actual_neg / actual_total * 100, 2),
        "neutral_pct":          round((actual_total - actual_pos - actual_neg) / actual_total * 100, 2),
        "dominant_emotion":     dominant_emotion,
        "emotion_distribution": emotion_dist,
        "psi_rating":           psi_rating,
    }, on_conflict="topic_id,snapshot_date").execute()
