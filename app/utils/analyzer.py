import os
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

def calculate_topic_rating(topic_id):
    """
    Calculates a composite sentiment rating for a topic using three components:

    Component 1 — Volume Score (40%):
        (positive_count - negative_count) / total_comments  → range -1 to +1

    Component 2 — Engagement-Weighted Score (40%):
        Each comment: weight = (abs(upvotes)+1) * confidence * direction
        Normalised by total absolute weight → range -1 to +1

    Component 3 — Emotion Intensity (20%):
        avg_confidence(positives) - avg_confidence(negatives) → range -1 to +1

    Final Rating = (vol × 40) + (eng × 40) + (intensity × 20)
    Range: -100 to +100  |  > 0 = Positive, < 0 = Negative, = 0 = Neutral
    """
    if not supabase:
        return 0.0, "Neutral"

    try:
        res = supabase.table("reddit_comments") \
                      .select("score, confidence_score, sentiment_label") \
                      .eq("topic_id", topic_id) \
                      .execute()
        if not res.data:
            return 0.0, "Neutral"

        df = pd.DataFrame(res.data)
        total_comments = len(df)

        if total_comments == 0:
            return 0.0, "Neutral"

        # Fill nulls with safe defaults
        df['score']            = df['score'].fillna(0).astype(int)
        df['confidence_score'] = df['confidence_score'].fillna(0.5).astype(float)
        df['sentiment_label']  = df['sentiment_label'].fillna('Neutral')

        df['direction'] = df['sentiment_label'].map(
            {'Positive': 1, 'Negative': -1, 'Neutral': 0}
        ).fillna(0).astype(int)

        # --- Component 1: Volume Score (40%) ---
        positive_count = (df['sentiment_label'] == 'Positive').sum()
        negative_count = (df['sentiment_label'] == 'Negative').sum()
        volume_score   = (positive_count - negative_count) / total_comments

        # --- Component 2: Engagement-Weighted Score (40%) ---
        abs_upvotes = df['score'].abs() + 1
        df['eng_weight']    = abs_upvotes * df['confidence_score'] * df['direction']
        total_abs_weight    = (abs_upvotes * df['confidence_score']).sum()
        engagement_score    = df['eng_weight'].sum() / total_abs_weight if total_abs_weight > 0 else 0.0

        # --- Component 3: Emotion Intensity (20%) ---
        pos_mask     = df['sentiment_label'] == 'Positive'
        neg_mask     = df['sentiment_label'] == 'Negative'
        avg_conf_pos = df.loc[pos_mask, 'confidence_score'].mean() if pos_mask.any() else 0.0
        avg_conf_neg = df.loc[neg_mask, 'confidence_score'].mean() if neg_mask.any() else 0.0
        intensity_score = avg_conf_pos - avg_conf_neg

        # --- Final Composite Rating ---
        final_rating      = round((volume_score * 40) + (engagement_score * 40) + (intensity_score * 20), 2)
        overall_sentiment = "Positive" if final_rating > 0 else ("Negative" if final_rating < 0 else "Neutral")

        try:
            supabase.table("search_topics").update({
                "rating":    final_rating,
                "sentiment": overall_sentiment
            }).eq("id", topic_id).execute()
        except Exception as e:
            print(f"Warning: Could not update search_topics rating: {e}")

        return final_rating, overall_sentiment

    except Exception as e:
        print(f"Error calculating topic rating: {e}")
        return 0.0, "Neutral"

def update_all_topic_ratings():
    """Runs the calculation for all topics in the database."""
    if not supabase: return
    
    res = supabase.table("search_topics").select("id, name").execute()
    if res.data:
        for topic in res.data:
            rating, sentiment = calculate_topic_rating(topic['id'])
            print(f"[{topic['name']}] Rating: {rating} | Sentiment: {sentiment}")
