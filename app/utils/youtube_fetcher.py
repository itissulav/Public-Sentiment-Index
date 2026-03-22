# Re-export shim — module has moved to app/api/youtube.py
from app.api.youtube import (  # noqa: F401
    search_videos, get_video_info, get_video_comments, YouTubeQuotaError,
)
