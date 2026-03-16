from app.utils.fetcher import get_reddit_comments
from app.utils.hf_analyzer import process_and_store_comments
from app.utils.analyzer import update_all_topic_ratings

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
    print("Fetches up to 500 posts / 2000 comments per topic via Reddit,")
    print("runs Hugging Face sentiment analysis, and saves everything to Supabase.\n")

    for topic in PREDEFINED_TOPICS:
        print(f"\n==============================")
        print(f"Processing Topic: {topic}")
        print(f"==============================")

        print(f"  Fetching up to 2000 comments for '{topic}'...")
        df = get_reddit_comments(query=topic, limit_posts=500, max_comments=2000, topic_name=topic)

        if df.empty:
            print(f"  No comments found for '{topic}'. Skipping analysis.")
            continue

        print(f"  Successfully fetched {len(df)} comments.")
        print(f"  Sending comments to Hugging Face and storing in database...")

        process_and_store_comments(topic, df, user_id=None)

        print(f"  Finished processing '{topic}'.")

    print("\n--- Computing composite ratings for all topics ---")
    update_all_topic_ratings()
    print("\n--- Seeding complete ---")


if __name__ == "__main__":
    seed_database()
