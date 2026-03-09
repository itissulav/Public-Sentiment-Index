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
API_URL = f"https://api-inference.huggingface.co/models/{MODEL_ID}"
HEADERS = {"Authorization": f"Bearer {HUGGINGFACE_API_KEY}"}

# The default topics we want to track automatically each week
# You can change these anytime!
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
    payload = {"inputs": texts}
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(API_URL, headers=HEADERS, json=payload)
            
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
    and tallies the sentiment.
    """
    print(f"\n--- Starting weekly analysis for: {topic} ---")
    
    # 1. Fetch recent posts & comments (Limiting to 15 posts to stay within fast weekly limits)
    # Using the PRAW fetcher we already built
    df = get_reddit_comments(query=topic, limit_posts=15, topic_name=topic)
    
    if df.empty:
        print(f"No comments fetched for {topic}.")
        return None
        
    print(f"Fetched {len(df)} total comments. Beginning sentiment analysis...")
    
    positive_count = 0
    negative_count = 0
    neutral_count = 0
    
    # HF Inference API prefers batches. Let's send in batches of 50.
    batch_size = 50
    comments_list = df['text'].tolist()
    
    for i in range(0, len(comments_list), batch_size):
        batch = comments_list[i:i + batch_size]
        
        if not batch:
            continue
            
        print(f"Analyzing batch {i//batch_size + 1}/{(len(comments_list)//batch_size) + 1}...")
        results = query_huggingface(batch)
        
        if not results:
            continue
            
        # Parse the predictions
        for prediction in results:
            if not isinstance(prediction, list):
                # Sometimes API returns a dict error
                continue
                
            # RoBERTa usually returns: [{"label": "LABEL_0", "score": 0.9}, {"label": "LABEL_1", "score": 0.05}, {"label": "LABEL_2", "score": 0.05}]
            # LABEL_0 -> Negative, LABEL_1 -> Neutral, LABEL_2 -> Positive
            
            # Find the highest scoring label
            top_label = max(prediction, key=lambda x: x['score'])['label']
            
            if top_label == 'LABEL_0' or top_label == 'negative':
                negative_count += 1
            elif top_label == 'LABEL_2' or top_label == 'positive':
                positive_count += 1
            else:
                neutral_count += 1
                
        # Sleep slightly between batches to be nice to the free hugging face API
        time.sleep(2)
        
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
                
                # Push the data to the Supabase database
                db_response = supabase.table("weekly_sentiments").insert(result).execute()
                print(f"Successfully saved {topic} to database!")
                
        except Exception as e:
            print(f"Failed processing topic '{topic}': {str(e)}", file=sys.stderr)
            
    print("Weekly Job Finished Successfully!")
