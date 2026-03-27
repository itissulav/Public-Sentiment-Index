"""
topic_image.py
==============
Fetches a representative image URL for any topic via the Wikipedia REST API.

Free, no API key required. The Wikipedia summary endpoint returns a thumbnail
URL for virtually any notable person, event, or concept.

Used by /api/topic-image to power dynamic topic imagery in the frontend.
"""

import json
import urllib.request
import urllib.parse


def get_topic_image_url(topic_name: str) -> str | None:
    """
    Return the best available Wikipedia thumbnail URL for `topic_name`.
    Returns None if the topic isn't found or on any network error.

    Wikipedia thumbnail URLs are hotlink-friendly and serve directly from
    Wikimedia CDN — no proxy needed.
    """
    try:
        encoded = urllib.parse.quote(topic_name, safe="")
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}"
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "PublicSentimentIndex/1.0 (educational project)"},
        )
        with urllib.request.urlopen(req, timeout=4) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            # Prefer originalimage (higher res) then fall back to thumbnail
            img = data.get("originalimage") or data.get("thumbnail")
            if img and img.get("source"):
                return img["source"]
    except Exception:
        pass
    return None
