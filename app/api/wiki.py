"""
app/api/wiki.py
===============
Wikipedia image fetching — two approaches, both free, no auth required.

  fetch_wiki_image(topic)       — uses Wikipedia query API (requests library)
  get_topic_image_url(topic)    — uses Wikipedia REST summary API (urllib)
"""

import json
import urllib.request
import urllib.parse


def fetch_wiki_image(topic: str) -> str | None:
    """Fetch a Wikipedia page thumbnail URL for a given topic name.
    Returns the image URL string or None if not found."""
    import requests as _req
    try:
        r = _req.get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "titles":  topic,
                "prop":    "pageimages",
                "format":  "json",
                "pithumbsize": 1200,
                "redirects": 1,
            },
            timeout=4,
            headers={"User-Agent": "PSI-FYP/1.0"},
        )
        pages = r.json().get("query", {}).get("pages", {})
        for page in pages.values():
            url = page.get("thumbnail", {}).get("source")
            if url:
                return url
    except Exception:
        pass
    return None


def get_topic_image_url(topic_name: str) -> str | None:
    """
    Return the best available Wikipedia thumbnail URL for `topic_name`.
    Prefers originalimage (higher res), falls back to thumbnail.
    Returns None if the topic isn't found or on any network error.
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
            img = data.get("originalimage") or data.get("thumbnail")
            if img and img.get("source"):
                return img["source"]
    except Exception:
        pass
    return None
