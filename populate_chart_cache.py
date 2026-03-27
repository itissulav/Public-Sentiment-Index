"""
populate_chart_cache.py
=======================
One-off script to populate precomputed_charts for topics from data already
in the Supabase database. No reseeding required.

Usage:
    python populate_chart_cache.py                      # all topics (predefined + custom)
    python populate_chart_cache.py --predefined-only    # only predefined topics
    python populate_chart_cache.py "Donald Trump"       # single topic by name
"""

import sys
from dotenv import load_dotenv
load_dotenv()

from app.services.supabase_client import admin_supabase
from app.utils.chart_cache import precompute_and_store


def main():
    args = sys.argv[1:]
    predefined_only = "--predefined-only" in args
    target = next((a for a in args if not a.startswith("--")), None)
    if target:
        target = target.strip()

    query = admin_supabase.table("topics").select("id, name")
    if predefined_only:
        query = query.is_("user_id", "null")
    res = query.execute()
    topics = res.data or []

    if target:
        topics = [t for t in topics if t["name"].lower() == target.lower()]
        if not topics:
            print(f"Topic '{target}' not found in the database.")
            print(f"Available: {[t['name'] for t in (res.data or [])]}")
            sys.exit(1)

    scope = "predefined" if predefined_only else "all"
    print(f"Pre-computing charts for {len(topics)} {scope} topic(s)...")
    failed = []
    for t in topics:
        ok = precompute_and_store(t["id"], topic_name=t["name"])
        if not ok:
            failed.append(t["name"])

    print(f"\nDone. {len(topics) - len(failed)}/{len(topics)} succeeded.")
    if failed:
        print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
