import os
import sys
from app.utils.fetcher import get_reddit_comments
from app.utils.hf_analyzer import process_and_store_comments

# The 5 predefined topics
PREDEFINED_TOPICS = [
    "Donald Trump",
    "The Boys",
    "Avengers Doomsday",
    "Macbook Neo",
    "America vs Iran"
]

def seed_database():
    print("--- Starting Predefined Topic Seeder ---")
    print("This will fetch 100 posts and all their comments for 5 topics.")
    print("It will then send them sequentially to Hugging Face and save to Supabase.")
    print("WARNING: This may take a long time due to Reddit API limits and Hugging Face free tier rate limits.\n")
    
    for topic in PREDEFINED_TOPICS:
        print(f"\n==============================")
        print(f"Processing Topic: {topic}")
        print(f"==============================")
        
        # 1. Fetch from Reddit
        # Pushing limits hard to ensure we grab 2000+ data points
        print(f"Fetching posts and max 2000 comments for '{topic}'...")
        df = get_reddit_comments(query=topic, limit_posts=500, max_comments=2000, topic_name=topic)
        
        if df.empty:
            print(f"No comments found for '{topic}'. Skipping analysis.")
            continue
            
        print(f"Successfully fetched {len(df)} comments for '{topic}'.")
        
        # 2. Analyze and Store
        print(f"Sending comments to Hugging Face and storing in database...")
        process_and_store_comments(topic, df)
        
        print(f"Finished processing '{topic}'!\n")
        
    print("All predefined topics have been seeded successfully!")

if __name__ == "__main__":
    seed_database()
