"""
app/api/reddit.py
=================
Reddit data fetching via PRAW.
"""

import os
import praw
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm

load_dotenv()


class RedditAPIError(Exception):
    """Raised when the Reddit API is unreachable or returns an unexpected error."""


reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    user_agent=os.getenv("USER_AGENT")
)

# Global dictionary to track progress across requests
fetch_progress = {
    "status": "idle",
    "current": 0,
    "total": 0,
    "message": ""
}

DAYS_BACK = 90  # collect comments up to 90 days old


def get_reddit_comments(query, limit_posts=100, max_comments=5000, subreddit_name="all", topic_name="topic", days_back=90):
    global fetch_progress

    fetch_progress["status"] = "fetching"
    fetch_progress["current"] = 0
    fetch_progress["total"] = limit_posts
    fetch_progress["message"] = f"Fetching posts for '{query}'"

    try:
        subreddit = reddit.subreddit(subreddit_name)
    except Exception as e:
        raise RedditAPIError(f"Could not connect to Reddit: {e}") from e

    comment_data = []
    seen_texts = set()
    post_count = 0

    # Map days_back → PRAW time_filter bucket (server-side prefilter)
    if days_back <= 1:
        time_filter = "day"
    elif days_back <= 7:
        time_filter = "week"
    elif days_back <= 31:
        time_filter = "month"
    elif days_back <= 365:
        time_filter = "year"
    else:
        time_filter = "all"

    # Incremental runs want newest-first so we can short-circuit on old posts;
    # fresh 90-day scans stick with relevance ranking.
    sort_mode = "new" if days_back < 90 else "relevance"

    print(f"Fetching up to {limit_posts} posts for '{query}' from r/{subreddit_name} "
          f"(sort={sort_mode}, time_filter={time_filter}, days_back={days_back})...")
    try:
        posts = list(subreddit.search(
            query,
            sort=sort_mode,
            time_filter=time_filter,
            limit=limit_posts,
        ))
    except Exception as e:
        raise RedditAPIError(f"Reddit search failed for '{query}': {e}") from e

    actual_posts = len(posts)
    fetch_progress["total"] = actual_posts
    fetch_progress["message"] = "Processing comments from posts..."

    cutoff = datetime.utcnow()
    submission_cutoff = cutoff - timedelta(days=days_back)

    for submission in tqdm(posts, desc="Processing posts"):
        if len(comment_data) >= max_comments:
            break

        # Skip posts created before the cutoff. In sort="new" mode we can
        # short-circuit — everything after is only going to be older.
        submission_date = datetime.utcfromtimestamp(submission.created_utc)
        if submission_date < submission_cutoff:
            if sort_mode == "new":
                break
            continue

        post_count += 1
        fetch_progress["current"] = post_count
        fetch_progress["message"] = f"Fetching Reddit posts... (post {post_count}/{actual_posts}, {len(comment_data)} comments)"

        # Expand "load more" links — limit=10 fetches up to 10 "more" stubs
        # giving a good breadth of replies without blowing up on huge threads
        try:
            submission.comments.replace_more(limit=10)
        except Exception:
            submission.comments.replace_more(limit=0)

        for top_comment in submission.comments:
            if len(comment_data) >= max_comments:
                break

            # ── Top-level comment ─────────────────────────────────
            entry = _make_entry(top_comment, submission, cutoff, days_back)
            if entry and entry["text"] not in seen_texts:
                seen_texts.add(entry["text"])
                comment_data.append(entry)

                if len(comment_data) % 500 == 0:
                    print(f"   ...{len(comment_data)} comments so far...")

            if len(comment_data) >= max_comments:
                break

            # ── Level-2 replies ───────────────────────────────────
            try:
                for reply in top_comment.replies:
                    if len(comment_data) >= max_comments:
                        break
                    rep_entry = _make_entry(reply, submission, cutoff, days_back)
                    if rep_entry and rep_entry["text"] not in seen_texts:
                        seen_texts.add(rep_entry["text"])
                        comment_data.append(rep_entry)

                    if len(comment_data) % 500 == 0 and len(comment_data) > 0:
                        print(f"   ...{len(comment_data)} comments so far...")
            except Exception:
                pass

    fetch_progress["status"] = "analyzing"
    fetch_progress["message"] = "Analyzing and saving data..."
    print(f"Collected {len(comment_data)} comments from {post_count} posts")

    df = pd.DataFrame(comment_data) if comment_data else pd.DataFrame(
        columns=["post_id", "post_title", "text", "author", "score", "timestamp"]
    )

    output_dir = os.path.join("_dev", "analysed")
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(os.path.join(output_dir, f"{topic_name}.csv"), index=False)

    fetch_progress["status"] = "complete"
    fetch_progress["message"] = "Analysis complete!"

    return df


def _make_entry(comment, submission, cutoff, days_back=90) -> dict | None:
    """Return a comment entry dict, or None if it should be skipped."""
    try:
        body = comment.body.replace("\n", " ").strip()
        if not body or body.lower() in ["[deleted]", "[removed]"]:
            return None

        comment_date = datetime.utcfromtimestamp(comment.created_utc)
        if (cutoff - comment_date).days > days_back:
            return None

        return {
            "post_id":    submission.id,
            "post_title": submission.title,
            "text":       body,
            "author":     comment.author.name if comment.author else "[deleted]",
            "score":      comment.score,
            "timestamp":  comment_date.strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception:
        return None
