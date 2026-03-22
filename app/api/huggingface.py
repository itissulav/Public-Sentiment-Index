"""
app/api/huggingface.py
======================
Takes a DataFrame of comments, classifies each with the emotion model,
and stores results in the 'comments' table (v2 schema).

Two classification modes:
  - local  : uses transformers pipeline (seeding — no quota)
  - api    : uses HF Inference API (daily cron — free tier)

Topic lookup uses the 'topics' table.
"""

import os
import json
from datetime import datetime, timezone, date
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()


class HuggingFaceError(Exception):
    """Raised when the HuggingFace Inference API is unavailable or rate-limited."""


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def _get_or_create_topic(topic_name: str, user_id=None) -> int:
    """Return the topic id, creating the row if it doesn't exist."""
    if user_id is None:
        res = supabase.table("topics").select("id") \
                      .eq("name", topic_name).is_("user_id", "null").execute()
    else:
        res = supabase.table("topics").select("id") \
                      .eq("name", topic_name).eq("user_id", user_id).execute()

    if res.data:
        return res.data[0]["id"]

    insert = supabase.table("topics").insert({
        "name": topic_name,
        "user_id": user_id,
    }).execute()
    return insert.data[0]["id"]


def _compute_and_upsert_snapshot(topic_id: int, df_classified: pd.DataFrame):
    """Compute today's daily_snapshot and upsert it."""
    from app.utils.analyzer import calculate_psi_from_df

    today = date.today().isoformat()
    if df_classified.empty:
        return

    try:
        total_res = supabase.table("comments").select("id", count="exact") \
            .eq("topic_id", topic_id).execute()
        pos_res   = supabase.table("comments").select("id", count="exact") \
            .eq("topic_id", topic_id).eq("sentiment_label", "Positive").execute()
        neg_res   = supabase.table("comments").select("id", count="exact") \
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

    supabase.table("daily_snapshots").upsert({
        "topic_id":            topic_id,
        "snapshot_date":       today,
        "total_comments":      actual_total,
        "positive_pct":        round(actual_pos / actual_total * 100, 2),
        "negative_pct":        round(actual_neg / actual_total * 100, 2),
        "neutral_pct":         round((actual_total - actual_pos - actual_neg) / actual_total * 100, 2),
        "dominant_emotion":    dominant_emotion,
        "emotion_distribution": emotion_dist,
        "psi_rating":          psi_rating,
    }, on_conflict="topic_id,snapshot_date").execute()


def _get_or_create_topic_source(topic_id: int, source_type: str, source_id_val: str) -> int:
    """Upsert topic_source row and return its id."""
    res = supabase.table("topic_sources").upsert(
        {"topic_id": topic_id, "source_type": source_type, "source_id": source_id_val},
        on_conflict="topic_id,source_type,source_id",
    ).execute()
    return res.data[0]["id"]


def _bulk_upsert_posts(topic_source_id: int, post_rows: list) -> dict:
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
    res = supabase.table("posts").upsert(
        records, on_conflict="topic_source_id,external_post_id",
    ).execute()
    return {r["external_post_id"]: r["id"] for r in res.data}


def process_and_store_comments(
    topic_name: str,
    df: pd.DataFrame,
    user_id=None,
    source_type: str = "reddit",
    source_id: str = "all",
    mode: str = "api",   # "local" or "api"
):
    """
    Classify df comments with the emotion model and insert into 'comments' table.

    Parameters
    ----------
    topic_name  : human-readable topic name
    df          : DataFrame with columns: text, author, score, timestamp, post_id, post_title
    user_id     : UUID string for custom user topics; None for predefined topics
    source_type : 'reddit' | 'youtube' | 'news'
    source_id   : subreddit name / YT channel / 'guardian'
    mode        : 'local' (transformers) or 'api' (HF Inference API)
    """
    from app.api.reddit import fetch_progress

    if df.empty:
        print(f"[hf_analyzer] Empty DataFrame for '{topic_name}' — nothing to store.")
        return

    print(f"[hf_analyzer] Classifying {len(df)} comments for '{topic_name}' via {mode}...")
    fetch_progress["status"] = "analyzing"
    fetch_progress["message"] = f"Classifying {len(df)} comments via emotion model..."

    from app.utils.emotion_classifier import classify_local, classify_api

    texts = df["text"].tolist()
    if mode == "local":
        emotion_results = classify_local(texts)
    else:
        emotion_results = classify_api(texts)

    topic_id = _get_or_create_topic(topic_name, user_id)
    today = date.today().isoformat()

    topic_source_id = _get_or_create_topic_source(topic_id, source_type, source_id)
    df_posts = df.copy()
    if "post_id" not in df_posts.columns:
        df_posts["post_id"] = source_id
    if "post_title" not in df_posts.columns:
        df_posts["post_title"] = ""
    unique_posts = df_posts[["post_id", "post_title"]].drop_duplicates("post_id").fillna("")
    post_rows = [
        {"external_post_id": str(r["post_id"]) or source_id,
         "title": str(r["post_title"])[:500]}
        for _, r in unique_posts.iterrows()
    ]
    post_id_map = _bulk_upsert_posts(topic_source_id, post_rows)

    db_records = []
    classified_rows = []

    for idx, result in enumerate(emotion_results):
        row = df.iloc[idx]

        published_at = None
        try:
            ts = str(row.get("timestamp", "") or row.get("published_at", ""))
            if ts:
                published_at = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").isoformat()
        except Exception:
            pass

        emotion_scores_dict = {
            k: result[k]
            for k in ["positive", "negative", "neutral"]
            if k in result
        }

        ext_post_id = str(row.get("post_id", "") or source_id)
        post_fk = post_id_map.get(ext_post_id)
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
            "sentiment_label": result["sentiment_label"],
            "emotion_label": result["emotion_label"],
            "confidence_score": result["confidence_score"],
            "score": int(row.get("score", 0)),
        })

    fetch_progress["message"] = f"Saving {len(db_records)} records to database..."
    batch_size = 50
    inserted = 0

    for i in range(0, len(db_records), batch_size):
        try:
            supabase.table("comments").insert(db_records[i:i + batch_size]).execute()
            inserted += len(db_records[i:i + batch_size])
            fetch_progress["current"] = inserted
        except Exception as e:
            print(f"[hf_analyzer] Supabase insert error (batch {i}): {e}")

    print(f"[hf_analyzer] Inserted {inserted}/{len(db_records)} records for '{topic_name}'.")

    try:
        df_classified = pd.DataFrame(classified_rows)
        _compute_and_upsert_snapshot(topic_id, df_classified)
        print(f"[hf_analyzer] Daily snapshot upserted for topic_id={topic_id}.")
    except Exception as e:
        print(f"[hf_analyzer] Snapshot error: {e}")

    fetch_progress["status"] = "complete"
    fetch_progress["message"] = "Analysis complete!"
    return True
