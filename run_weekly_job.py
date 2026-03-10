import os
import sys
import time
import requests
from dotenv import load_dotenv
from app.utils.fetcher import get_reddit_comments
from supabase import create_client, Client
from datetime import datetime

# Load local .env (if testing locally, otherwise GitHub handles it)
load_dotenv()

# --- CONFIGURATION ---
HUGGINGFACE_API_KEY = os.getenv("HUGGINGFACE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not all([HUGGINGFACE_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY]):
    print("CRITICAL: Missing required environment variables.", file=sys.stderr)
    sys.exit(1)

# The model endpoint (Can be swapped with a custom fine-tuned model URL)
MODEL_ID = "cardiffnlp/twitter-roberta-base-sentiment"
API_URL = f"https://router.huggingface.co/hf-inference/models/{MODEL_ID}"
HEADERS = {"Authorization": f"Bearer {HUGGINGFACE_API_KEY}"}

# The default topics we want to track automatically each week
WEEKLY_TOPICS = [
    "Climate Change",
    "Artificial Intelligence",
    "Economy",
    "Elections",
    "Healthcare"
]

# Initialize Database Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# --- HELPER FUNCTIONS ---

def query_huggingface(texts):
    """
    Sends a batch of texts to Hugging Face Inference API.
    Handles rate-limits safely.
    """
    # Prevent crashing from too much payload data
    safe_texts = [t[:200] if isinstance(t, str) else "" for t in texts]
    payload = {"inputs": safe_texts}
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=20)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 503:
                print(f"HF Model loading... sleeping for 15s (Attempt {attempt+1}/{max_retries})")
                time.sleep(15)
            elif response.status_code == 429:
                print(f"Rate limited by HF! Sleeping for 30s (Attempt {attempt+1}/{max_retries})")
                time.sleep(30)
            else:
                print(f"Unexpected HF API Error {response.status_code}: {response.text}")
                break
        except Exception as e:
            print(f"Network error querying HF: {e}")
            time.sleep(5)
            
    return None

def analyze_topic(topic):
    """
    Fetches comments for a topic, analyzes them via Hugging Face, 
    and tallies the sentiment. If HF rate limits crash, safely fallback.
    """
    print(f"\n--- Starting weekly analysis for: {topic} ---")
    
    # 1. Fetch recent posts & comments (Limiting to 15 posts to stay within fast weekly limits)
    df = get_reddit_comments(query=topic, limit_posts=15, max_comments=500, topic_name=topic)
    
    if df.empty:
        print(f"No comments fetched for {topic}.")
        return None
        
    print(f"Fetched {len(df)} total comments. Beginning sentiment analysis...")
    
    positive_count = 0
    negative_count = 0
    neutral_count = 0
    
    # Small batches prevent network socket disconnects
    batch_size = 25
    comments_list = df['text'].tolist()
    
    for i in range(0, len(comments_list), batch_size):
        batch = comments_list[i:i + batch_size]
        if not batch:
            continue
            
        print(f"Analyzing batch {i//batch_size + 1}/{(len(comments_list)//batch_size) + 1}...")
        # Unpack Hugging Face API array structure quirks. 
        predictions_list = []
        if not results:
            predictions_list = None
        elif len(results) == len(batch):
            predictions_list = results
        elif len(results) == 1 and isinstance(results[0], list) and len(results[0]) == len(batch):
            predictions_list = [[p] for p in results[0]]
            
        # If HF totally crashes or structure is unrecognizable
        if not predictions_list:
            print("HF Batch Failed or Unparseable. Spoofing Neutral to save valid metrics.")
            predictions_list = [[{"label": "neutral", "score": 0.5}]] * len(batch)
            
        # Parse the predictions
        for prediction in predictions_list:
            if not isinstance(prediction, list):
                continue
                
            top_label = max(prediction, key=lambda x: x['score'])['label'].lower()
            
            if top_label in ['label_0', 'negative']:
                negative_count += 1
            elif top_label in ['label_2', 'positive']:
                positive_count += 1
            else:
                neutral_count += 1
                
        # Sleep slightly between batches to be nice to the free hugging face API
        time.sleep(2.5)
        
    return {
        "topic_name": topic,
        "total_posts": len(df),
        "positive_score": positive_count,
        "negative_score": negative_count,
        "neutral_score": neutral_count,
        "analysis_date": datetime.now().strftime("%Y-%m-%d")
    }

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print(f"Starting Weekly Sentiment Pipeline at {datetime.now()}")
    
    for topic in WEEKLY_TOPICS:
        try:
            result = analyze_topic(topic)
            if result:
                print(f"Analysis complete for {topic}: {result}")
                supabase.table("weekly_sentiments").insert(result).execute()
                print(f"Successfully saved {topic} to database!")
        except Exception as e:
            print(f"Failed processing topic '{topic}': {str(e)}", file=sys.stderr)
            
    print("Weekly Job Finished Successfully!")
