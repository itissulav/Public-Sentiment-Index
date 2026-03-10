import pandas as pd
from app.utils.hf_analyzer import supabase

# Load pre-scraped 2000 rows
df = pd.read_csv("static/analysed/America vs Iran.csv")

# 1. Grab topic ID
topic_name = "America vs Iran"
topic_res = supabase.table("search_topics").select("id").eq("name", topic_name).execute()
if not topic_res.data:
    insert_res = supabase.table("search_topics").insert({"name": topic_name}).execute()
    topic_id = insert_res.data[0]['id']
else:
    topic_id = topic_res.data[0]['id']

print(f"Loaded Topic ID: {topic_id}")
print(f"CSV Rows to Insert: {len(df)}")

# Generate dummy DB records fast (skip HF AI for speed)
db_records = []
for idx, row in df.iterrows():
    db_records.append({
        "topic_id": topic_id,
        "post_id": str(row['post_id']),
        "text": str(row['text'])[:500],
        "author": str(row['author']),
        "score": int(row['score']),
        "sentiment_label": "Neutral",
        "confidence_score": 0.5,
        "timestamp": row['timestamp']
    })

print("Attempting to insert into Supabase...")

# Test the 50-chunk limit
db_batch_size = 50
inserted_count = 0
for i in range(0, len(db_records), db_batch_size):
    try:
        batch = db_records[i:i + db_batch_size]
        response = supabase.table("reddit_comments").insert(batch).execute()
        inserted_count += len(batch)
    except Exception as e:
        print(f"\n[!] SUPABASE INSERT ERROR at Row {i}:")
        print(str(e))
        break

print(f"\nFinal Result: Successfully saved {inserted_count} out of {len(db_records)} records.")
