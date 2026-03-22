"""
analyzer.py
===========
PSI (Public Sentiment Index) rating calculation — v2 schema.

Reads from the 'comments' table (with emotion_scores).
Writes the result to daily_snapshots.

Rating formula — range -100 to +100:

  Component 1 — Volume Score (40%):
    (positive_count - negative_count) / total_comments  → -1 to +1

  Component 2 — Engagement-Weighted Score (40%):
    Each comment weighted by abs(upvotes)+1 × confidence × direction
    Normalised by total absolute weight

  Component 3 — Emotion Intensity (20%):
    avg(joy + optimism scores for positive comments)
    - avg(anger + sadness scores for negative comments)
    → -1 to +1

Final = (vol × 40) + (eng × 40) + (intensity × 20)
"""

import os
import json
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if SUPABASE_URL and SUPABASE_SERVICE_KEY:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
else:
    supabase = None


def calculate_psi_from_df(df: pd.DataFrame) -> float:
    """
    Compute PSI rating from a classified DataFrame.
    df must have: sentiment_label, confidence_score, score (upvotes),
    and optionally the 4 emotion columns (anger, joy, optimism, sadness).
    Returns a float in range -100 to +100.
    """
    total = len(df)
    if total == 0:
        return 0.0

    df = df.copy()
    df["score"] = df["score"].fillna(0).astype(int)
    df["confidence_score"] = df["confidence_score"].fillna(0.5).astype(float)
    df["sentiment_label"] = df["sentiment_label"].fillna("Neutral")
    df["direction"] = df["sentiment_label"].map(
        {"Positive": 1, "Negative": -1, "Neutral": 0}
    ).fillna(0).astype(int)

    # Component 1: Volume
    pos_count = int((df["sentiment_label"] == "Positive").sum())
    neg_count = int((df["sentiment_label"] == "Negative").sum())
    volume_score = (pos_count - neg_count) / total

    # Component 2: Engagement-weighted
    abs_votes = df["score"].abs() + 1
    eng_weights = abs_votes * df["confidence_score"] * df["direction"]
    total_abs = (abs_votes * df["confidence_score"]).sum()
    engagement_score = float(eng_weights.sum() / total_abs) if total_abs > 0 else 0.0

    # Component 3: Emotion intensity (uses raw emotion scores if available)
    pos_mask = df["sentiment_label"] == "Positive"
    neg_mask = df["sentiment_label"] == "Negative"

    has_sentiment_cols = "positive" in df.columns and "negative" in df.columns

    if has_sentiment_cols and pos_mask.any() and neg_mask.any():
        avg_pos_conf = float(df.loc[pos_mask, "positive"].mean())
        avg_neg_conf = float(df.loc[neg_mask, "negative"].mean())
        intensity_score = avg_pos_conf - avg_neg_conf
    else:
        # Fallback: confidence_score difference
        avg_pos_conf = df.loc[pos_mask, "confidence_score"].mean() if pos_mask.any() else 0.0
        avg_neg_conf = df.loc[neg_mask, "confidence_score"].mean() if neg_mask.any() else 0.0
        intensity_score = float(avg_pos_conf - avg_neg_conf)

    final = round((volume_score * 40) + (engagement_score * 40) + (intensity_score * 20), 2)
    return max(-100.0, min(100.0, final))


def calculate_topic_rating(topic_id: int):
    """
    Load all comments for a topic from DB, calculate PSI rating,
    and return (rating, sentiment_label).
    """
    if not supabase:
        return 0.0, "Neutral"

    try:
        res = supabase.table("comments") \
                      .select("score, confidence_score, sentiment_label, emotion_scores") \
                      .eq("topic_id", topic_id) \
                      .execute()

        if not res.data:
            return 0.0, "Neutral"

        df = pd.DataFrame(res.data)

        # Expand emotion_scores JSONB into columns
        if "emotion_scores" in df.columns:
            def _parse_scores(x):
                if isinstance(x, dict):
                    return x
                if isinstance(x, str):
                    try:
                        return json.loads(x)
                    except Exception:
                        return {}
                return {}

            scores_expanded = df["emotion_scores"].apply(_parse_scores).apply(pd.Series)
            df = pd.concat([df.drop(columns=["emotion_scores"]), scores_expanded], axis=1)

        rating = calculate_psi_from_df(df)
        sentiment = "Positive" if rating > 0 else ("Negative" if rating < 0 else "Neutral")
        return rating, sentiment

    except Exception as e:
        print(f"[analyzer] Error calculating rating for topic {topic_id}: {e}")
        return 0.0, "Neutral"


def update_all_topic_ratings():
    """Recalculate PSI for all predefined topics."""
    if not supabase:
        return

    res = supabase.table("topics").select("id, name").is_("user_id", "null").execute()
    if res.data:
        for topic in res.data:
            rating, sentiment = calculate_topic_rating(topic["id"])
            print(f"[{topic['name']}] PSI: {rating} | {sentiment}")
