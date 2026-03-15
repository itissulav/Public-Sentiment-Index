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
    Calculates the weighted sentiment rating for a topic based on engagement (score) and AI confidence.
    Weighted Rating = Sum(Weight)
    Weight = (abs(score) + 1) * confidence_score * direction
    """
    if not supabase:
        return 0, "Neutral"
        
    try:
        # Fetch all comments for the topic
        res = supabase.table("reddit_comments").select("score, confidence_score, sentiment_label").eq("topic_id", topic_id).execute()
        if not res.data:
            return 0, "Neutral"
            
        df = pd.DataFrame(res.data)
        
        total_rating = 0.0
        
        for _, row in df.iterrows():
            score = int(row['score']) if pd.notnull(row['score']) else 0
            confidence = float(row['confidence_score']) if pd.notnull(row['confidence_score']) else 0.5
            sentiment = row['sentiment_label']
            
            direction = 0
            if sentiment == 'Positive':
                direction = 1
            elif sentiment == 'Negative':
                direction = -1
                
            weight = (abs(score) + 1) * confidence * direction
            total_rating += weight
            
        overall_sentiment = "Positive" if total_rating > 0 else ("Negative" if total_rating < 0 else "Neutral")
        
        # We round the rating for cleaner UI display
        final_rating = round(total_rating, 2)
        
        # Update the search_topics table cache so the Home page can fetch it instantly
        try:
            supabase.table("search_topics").update({
                "rating": final_rating, 
                "sentiment": overall_sentiment
            }).eq("id", topic_id).execute()
        except Exception as e:
            print(f"Warning: Could not update search_topics rating automatically: {e}")
            
        return final_rating, overall_sentiment
        
    except Exception as e:
        print(f"Error calculating topic rating: {e}")
        return 0, "Neutral"

def update_all_topic_ratings():
    """Runs the calculation for all topics in the database."""
    if not supabase: return
    
    res = supabase.table("search_topics").select("id, name").execute()
    if res.data:
        for topic in res.data:
            rating, sentiment = calculate_topic_rating(topic['id'])
            print(f"[{topic['name']}] Rating: {rating} | Sentiment: {sentiment}")
