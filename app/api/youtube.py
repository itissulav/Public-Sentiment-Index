"""
app/api/youtube.py
==================
Fetches YouTube video metadata and comments using the YouTube Data API v3.

Quota cost (free tier: 10,000 units/day):
  search.list         = 100 units per call
  videos.list         =   1 unit per call
  channels.list       =   1 unit per call
  commentThreads.list =   1 unit per page (100 comments/page)

Per-topic budget: 5 queries × 102 (search+videos+channels) + ~2 pages × 9 videos = ~528 units.
Fits ~18 full topic scans per day on the free tier.
"""

import os
from datetime import datetime, timedelta, timezone
import pandas as pd


class YouTubeQuotaError(Exception):
    """Raised when the YouTube Data API quota is exceeded (HTTP 403)."""


def _build_client(api_key: str):
    from googleapiclient.discovery import build
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


def search_videos(query: str, api_key: str, max_results: int = 5, days_back: int = 90) -> list:
    """
    Search YouTube for informative/opinionated videos matching `query`.

    Filters applied:
    - No Shorts (videoDuration=medium excludes < 4 min)
    - No live streams (liveStreamingDetails presence check)
    - Channels with 100k+ subscribers only
    - Published within `days_back` days (default 90)

    Returns list of dicts sorted by views then comment count (highest first).
    Quota cost: 100 (search.list) + 1 (videos.list) + 1 (channels.list) = 102 units per call.
    """
    try:
        from googleapiclient.errors import HttpError
        youtube = _build_client(api_key)

        published_after = (datetime.now(timezone.utc) - timedelta(days=days_back)) \
                          .strftime("%Y-%m-%dT%H:%M:%SZ")
        search_response = youtube.search().list(
            q=query,
            part="id,snippet",
            type="video",
            order="relevance",
            maxResults=max_results,
            relevanceLanguage="en",
            safeSearch="none",
            publishedAfter=published_after,
            videoDuration="medium",  # excludes Shorts (< 4 min)
        ).execute()

        video_ids = [
            item["id"]["videoId"]
            for item in search_response.get("items", [])
            if item["id"].get("kind") == "youtube#video"
        ]

        if not video_ids:
            return []

        # Fetch stats + live stream info (liveStreamingDetails) + channel ID
        stats_response = youtube.videos().list(
            part="snippet,statistics,liveStreamingDetails",
            id=",".join(video_ids),
        ).execute()

        # Filter out live streams and collect channel IDs
        filtered_items = []
        channel_ids = []
        for item in stats_response.get("items", []):
            # Skip live streams (ongoing or completed broadcasts)
            if item.get("liveStreamingDetails"):
                continue
            # Skip if title contains #shorts indicator
            title = item.get("snippet", {}).get("title", "").lower()
            if "#shorts" in title or " shorts" in title:
                continue
            filtered_items.append(item)
            channel_ids.append(item["snippet"]["channelId"])

        if not filtered_items:
            return []

        # Fetch subscriber counts for all channels in one call
        unique_channel_ids = list(dict.fromkeys(channel_ids))
        channel_subs = {}
        try:
            ch_response = youtube.channels().list(
                part="statistics",
                id=",".join(unique_channel_ids),
            ).execute()
            for ch in ch_response.get("items", []):
                channel_subs[ch["id"]] = int(ch.get("statistics", {}).get("subscriberCount", 0))
        except Exception as e:
            print(f"[youtube] Could not fetch channel stats: {e}")

        results = []
        for item in filtered_items:
            channel_id  = item["snippet"]["channelId"]
            sub_count   = channel_subs.get(channel_id, 0)
            # Skip channels with fewer than 100k subscribers
            if channel_subs and sub_count < 100_000:
                continue
            stats   = item.get("statistics", {})
            snippet = item.get("snippet", {})
            results.append({
                "video_id":         item["id"],
                "title":            snippet.get("title", ""),
                "channel":          snippet.get("channelTitle", ""),
                "subscriber_count": sub_count,
                "view_count":       int(stats.get("viewCount", 0)),
                "like_count":       int(stats.get("likeCount", 0)),
                "comment_count":    int(stats.get("commentCount", 0)),
                "published_at":     snippet.get("publishedAt", ""),
            })

        # Sort by views first, then comment count as tiebreaker
        results.sort(key=lambda v: (v["view_count"], v["comment_count"]), reverse=True)
        return results

    except Exception as e:
        err_str = str(e)
        if "quotaExceeded" in err_str or "403" in err_str:
            raise YouTubeQuotaError(f"YouTube API quota exceeded: {e}") from e
        print(f"[youtube] Error in search_videos('{query}'): {e}")
        return []


def get_video_info(video_ids: list, api_key: str) -> list:
    """
    Fetch metadata for a list of video IDs using videos.list.
    1 quota unit per call (up to 50 IDs per call).
    Returns list of dicts: video_id, title, channel, view_count, like_count, comment_count, published_at
    """
    if not video_ids:
        return []
    try:
        youtube = _build_client(api_key)
        stats_response = youtube.videos().list(
            part="snippet,statistics",
            id=",".join(video_ids[:50]),
        ).execute()
        results = []
        for item in stats_response.get("items", []):
            stats   = item.get("statistics", {})
            snippet = item.get("snippet", {})
            results.append({
                "video_id":      item["id"],
                "title":         snippet.get("title", ""),
                "channel":       snippet.get("channelTitle", ""),
                "view_count":    int(stats.get("viewCount", 0)),
                "like_count":    int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
                "published_at":  snippet.get("publishedAt", ""),
            })
        return results
    except Exception as e:
        print(f"[youtube] Error in get_video_info: {e}")
        return []


def get_video_comments(video_id: str, video_title: str, api_key: str, max_comments: int = 200) -> pd.DataFrame:
    """
    Fetch top-level comments for a YouTube video.

    Returns DataFrame with columns: text, author, score, post_id, post_title, published_at, source_id
    Returns empty DataFrame if comments are disabled or on error.

    Quota cost: 1 unit per page (100 comments/page).
    """
    COLUMNS = ["text", "author", "score", "post_id", "post_title", "published_at", "source_id"]

    try:
        youtube = _build_client(api_key)
        comment_data = []
        next_page_token = None

        while len(comment_data) < max_comments:
            batch_size = min(100, max_comments - len(comment_data))

            kwargs = {
                "part":        "snippet",
                "videoId":     video_id,
                "maxResults":  batch_size,
                "order":       "relevance",
                "textFormat":  "plainText",
            }
            if next_page_token:
                kwargs["pageToken"] = next_page_token

            try:
                response = youtube.commentThreads().list(**kwargs).execute()
            except Exception as e:
                err_str = str(e)
                if "commentsDisabled" in err_str or "403" in err_str:
                    print(f"[youtube] Comments disabled or forbidden for video {video_id}")
                else:
                    print(f"[youtube] Error fetching comments for {video_id}: {e}")
                break

            for item in response.get("items", []):
                top = item["snippet"]["topLevelComment"]["snippet"]

                raw_text = top.get("textDisplay", "").replace("\n", " ").strip()
                if not raw_text:
                    continue

                published_raw = top.get("publishedAt", "")
                try:
                    dt = datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
                    published_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    published_str = ""

                comment_data.append({
                    "text":         raw_text[:2000],
                    "author":       top.get("authorDisplayName", "[anonymous]"),
                    "score":        int(top.get("likeCount", 0)),
                    "post_id":      video_id,
                    "post_title":   video_title[:500],
                    "published_at": published_str,
                    "source_id":    video_id,
                })

                if len(comment_data) >= max_comments:
                    break

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        if not comment_data:
            return pd.DataFrame(columns=COLUMNS)

        return pd.DataFrame(comment_data)

    except Exception as e:
        print(f"[youtube] Error in get_video_comments({video_id}): {e}")
        return pd.DataFrame(columns=COLUMNS)
