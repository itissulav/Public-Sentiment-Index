# Re-export shim — module has moved to app/api/reddit.py
from app.api.reddit import get_reddit_comments, fetch_progress, RedditAPIError  # noqa: F401
