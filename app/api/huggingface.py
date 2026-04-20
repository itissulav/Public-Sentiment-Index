"""
app/api/huggingface.py
======================
Classifies comments with the emotion model.

Two classification modes:
  - local  : uses transformers pipeline (seeding — no quota)
  - api    : uses HF Inference API (daily cron — free tier)

All DB writes are handled by services/comment_service.py.
"""

import json
from datetime import date
import pandas as pd


class HuggingFaceError(Exception):
    """Raised when the HuggingFace Inference API is unavailable or rate-limited."""


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
    from app.utils.emotion_classifier import classify_local, classify_api
    from app.services import comment_service

    if df.empty:
        print(f"[hf_analyzer] Empty DataFrame for '{topic_name}' — nothing to store.")
        return

    total_comments = len(df)
    total_batches  = (total_comments + 99) // 100
    print(f"[hf_analyzer] Classifying {total_comments} comments for '{topic_name}' via {mode} ({total_batches} batches)...")
    fetch_progress["status"]  = "analyzing"
    fetch_progress["message"] = f"Analysing sentiment... (0/{total_batches} batches)"

    texts = df["text"].tolist()
    if mode == "local":
        emotion_results = classify_local(texts)
    else:
        emotion_results = classify_api(texts)

    topic_id = comment_service.get_or_create_topic(topic_name, user_id)
    today = date.today().isoformat()

    topic_source_id = comment_service.get_or_create_topic_source(topic_id, source_type, source_id)
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
    post_id_map = comment_service.bulk_upsert_posts(topic_source_id, post_rows)

    db_records = []
    classified_rows = []

    for (idx, row), result in zip(df.iterrows(), emotion_results):

        published_at = None
        try:
            from datetime import datetime
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

    fetch_progress["message"] = f"Saving results... (0/{len(db_records)} comments)"
    comment_service.store_classified_comments(db_records, fetch_progress)

    try:
        df_classified = pd.DataFrame(classified_rows)
        comment_service.upsert_snapshot(topic_id, df_classified)
        print(f"[hf_analyzer] Daily snapshot upserted for topic_id={topic_id}.")
    except Exception as e:
        print(f"[hf_analyzer] Snapshot error: {e}")

    fetch_progress["status"] = "complete"
    fetch_progress["message"] = "Analysis complete!"
    return True
