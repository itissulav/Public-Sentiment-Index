"""
app/api/wiki.py
===============
Wikipedia thumbnail fetching.
"""


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
