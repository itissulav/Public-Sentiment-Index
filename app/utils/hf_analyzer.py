import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

HUGGINGFACE_API_KEY = os.getenv("HUGGINGFACE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# You will eventually change this URL to your own custom fine-tuned model endpoint!
MODEL_ID = "cardiffnlp/twitter-roberta-base-sentiment"
API_URL = f"https://router.huggingface.co/hf-inference/models/{MODEL_ID}"
HEADERS = {"Authorization": f"Bearer {HUGGINGFACE_API_KEY}"} if HUGGINGFACE_API_KEY else {}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

def query_huggingface(texts):
    if not HUGGINGFACE_API_KEY:
        print("Missing HF API Key. Returning neutral labels.")
        return [[{"label": "neutral", "score": 1.0}] for _ in texts]
        
    # Prevent urllib3 socket drops by sending tiny request bodies to the free tier
    safe_texts = [t[:200] if isinstance(t, str) else "" for t in texts]
        
    payload = {"inputs": safe_texts}
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=20)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 503:
                print(f"HF Model loading... sleeping 15s (Attempt {attempt+1}/{max_retries})")
                time.sleep(15)
            elif response.status_code == 429:
                print(f"HF Rate limited! Sleeping 30s (Attempt {attempt+1}/{max_retries})")
                time.sleep(30)
            else:
                print(f"HF Error {response.status_code}: {response.text}")
                break
        except Exception as e:
            print(f"Network error querying HF: {e}")
            time.sleep(5)
            
    return None

def process_and_store_comments(topic_name, df, user_id=None):
    """
    Takes a DataFrame of comments, batches them to Hugging Face for sentiment,
    and inserts the complete records into the Supabase database.

    user_id=None  → predefined/shared topic (user_id IS NULL in DB)
    user_id=<uid> → user's personal saved search (private to that user)
    """
    from app.utils.fetcher import fetch_progress # Import shared progress state

    if df.empty:
        return

    print(f"hf_analyzer received DataFrame with {len(df)} rows for {topic_name}")
    fetch_progress["status"] = "analyzing"

    # RELATIONAL DB: Ensure topic exists and get ID, scoped by user_id
    if user_id is None:
        topic_res = supabase.table("search_topics").select("id") \
                            .eq("name", topic_name) \
                            .is_("user_id", "null") \
                            .execute()
    else:
        topic_res = supabase.table("search_topics").select("id") \
                            .eq("name", topic_name) \
                            .eq("user_id", user_id) \
                            .execute()

    if not topic_res.data:
        insert_res = supabase.table("search_topics") \
                             .insert({"name": topic_name, "user_id": user_id}) \
                             .execute()
        topic_id = insert_res.data[0]['id']
    else:
        topic_id = topic_res.data[0]['id']
        # Clear old comments for this topic to refresh data cleanly
        supabase.table("reddit_comments").delete().eq("topic_id", topic_id).execute()
    
    comments_list = df['text'].tolist()
    total_comments = len(comments_list)
    
    fetch_progress["total"] = total_comments
    fetch_progress["current"] = 0
    fetch_progress["message"] = f"Analyzing {total_comments} comments via HF AI..."
    
    # Small batch size to prevent `urllib3` Dropouts on HF Free Tier
    batch_size = 25
    db_records = []
    
    for i in range(0, total_comments, batch_size):
        batch_texts = comments_list[i:i + batch_size]
        batch_df = df.iloc[i:i + batch_size]
        
        results = query_huggingface(batch_texts)
        fetch_progress["current"] = min(i + batch_size, total_comments)
        
        # Unpack Hugging Face API array structure quirks. 
        # For batch inputs, HF might return [[pred1, pred2, ..., predN]]!
        predictions_list = []
        if not results:
            predictions_list = None
        elif len(results) == len(batch_texts):
            # E.g., [[{label, score}], [{label, score}]]
            predictions_list = results
        elif len(results) == 1 and isinstance(results[0], list) and len(results[0]) == len(batch_texts):
            # Model packaged the entire batch inside a single 2D array!
            predictions_list = [[p] for p in results[0]]
            
        # Safe fallback
        if not predictions_list:
            print(f"[!] HF API Parsing Failed! Sent {len(batch_texts)}, but couldn't parse structure. Spoofing Neutral.")
            predictions_list = [[{"label": "neutral", "score": 0.5}]] * len(batch_df)
            
        for idx, prediction in enumerate(predictions_list):
            if not isinstance(prediction, list):
                continue
                
            top_prediction = max(prediction, key=lambda x: x['score'])
            raw_label = top_prediction['label'].lower()
            confidence = top_prediction['score']
            
            if raw_label in ['label_0', 'negative']:
                sentiment = 'Negative'
            elif raw_label in ['label_2', 'positive']:
                sentiment = 'Positive'
            else:
                sentiment = 'Neutral'
                
            # If not confident in positive/negative, assume it's neutral text
            if sentiment != 'Neutral' and confidence < 0.60:
                sentiment = 'Neutral'
                
            row = batch_df.iloc[idx]
            db_records.append({
                "topic_id": topic_id,
                "post_id": str(row['post_id']),
                "text": str(row['text']),
                "author": str(row['author']),
                "score": int(row['score']),
                "sentiment_label": sentiment,
                "confidence_score": float(confidence),
                "timestamp": row['timestamp']
            })
            
        time.sleep(2.5) # Force throttle payload speed to keep free tier HTTP sockets open
        
    if db_records:
        fetch_progress["message"] = f"Saving {len(db_records)} results to DB..."
        db_batch_size = 50
        inserted_count = 0
        for i in range(0, len(db_records), db_batch_size):
            try:
                batch = db_records[i:i + db_batch_size]
                supabase.table("reddit_comments").insert(batch).execute()
                inserted_count += len(batch)
            except Exception as e:
                print(f"Supabase Insert Error: {e}")
                
        print(f"Successfully saved {inserted_count} out of {len(db_records)} records.")
        # Update comment count and last_updated timestamp (needed for freshness checks)
        try:
            supabase.table("search_topics").update({
                "total_comments": inserted_count,
                "last_updated": datetime.now(timezone.utc).isoformat()
            }).eq("id", topic_id).execute()
        except:
            pass
                
    fetch_progress["status"] = "complete"
    fetch_progress["message"] = "Analysis complete!"
    return True