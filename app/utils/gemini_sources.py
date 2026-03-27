"""
gemini_sources.py
=================
Uses Gemini to discover the best data sources for a given topic.

Returns a structured dict with:
  - category      : topic category (politics, entertainment, tech, ...)
  - subreddits    : list of relevant subreddit names (without r/)
  - youtube_queries : 5 search queries to find dedicated review/opinion videos
  - news_keywords : list of Guardian API search keywords

Called once when a topic is first created. Result is stored in topic_sources table.
"""

import os
import json
import re
from dotenv import load_dotenv

load_dotenv()

PROMPT_TEMPLATE = """
You are a data source analyst for a public sentiment platform. Your job is to find the BEST YouTube videos that contain meaningful audience discussion about "{topic}".

CRITICAL RULES — read carefully before generating queries:
1. NEVER use "unboxing" unless the topic is a physical consumer product (smartphone, laptop, gadget, headphones).
2. NEVER use "hands on" unless the topic is a physical product you can hold.
3. For TV shows, movies, anime, or any entertainment: use "review", "analysis", "explained", "reaction", "discussion", "season [X]", "episode breakdown".
4. For politicians, political events, or international figures: ALWAYS prefer established news channels (CNN, BBC News, Sky News, Al Jazeera, MSNBC, Fox News) and political analysis channels. Use queries like "{topic} news", "{topic} analysis BBC", "{topic} CNN report".
5. For sports: use "highlights", "analysis", "match review", "breakdown".
6. Every single query MUST contain the exact topic name "{topic}" so YouTube results are exclusively about this topic — not toys, not unrelated products, not similar names.
7. Be specific enough that an ambiguous name (e.g. "The Boys") cannot match unrelated content (toys, unboxing channels, etc.).
8. ALL queries must target informative or opinionated content only — reviews, analysis, debates, documentaries, news reports. NEVER generate queries that could return toy unboxings, product demonstrations for unrelated products, or entertainment content unrelated to the topic.
9. Do NOT generate queries that could return YouTube Shorts or live stream results.

First, determine the topic type for "{topic}":
- Is it a TV show? (e.g. The Boys, House of the Dragon, Severance)
- Is it a movie? (e.g. Avengers Doomsday, Interstellar)
- Is it a politician, political figure, or international personality? (e.g. Donald Trump, Joe Biden, Elon Musk)
- Is it a physical tech product? (e.g. MacBook, iPhone, GPU)
- Is it a geopolitical event? (e.g. America vs Iran, Russia Ukraine, Gaza Conflict)
- Is it a sports event or team?
- Other?

Then return a JSON object with exactly these fields:
{{
  "category": "one of: politics | entertainment | tech | business | sport | general",
  "subreddits": ["subredditname1", "subredditname2", "subredditname3"],
  "youtube_queries": ["query1", "query2", "query3", "query4", "query5"],
  "news_keywords": ["keyword1", "keyword2", "keyword3"]
}}

Rules:
- subreddits: 3 to 5 most relevant communities (e.g. for The Boys: ["TheBoys", "television", "Superhero_Shows"]), no 'r/' prefix, no duplicates
- youtube_queries: exactly 5 search queries, each MUST contain "{topic}".
    Choose query templates based on topic type:
    * TV shows / movies / anime → "{topic} review", "{topic} season analysis", "{topic} explained", "{topic} reaction", "{topic} finale discussion"
    * Politicians / political figures / international personalities → "{topic} BBC News", "{topic} CNN analysis", "{topic} Sky News", "{topic} speech reaction", "{topic} Al Jazeera"
    * Geopolitical events → "{topic} BBC News", "{topic} Al Jazeera", "{topic} analysis", "{topic} explained", "{topic} documentary"
    * Physical tech products → "{topic} review", "{topic} unboxing", "{topic} hands on"
    * Sports → "{topic} highlights", "{topic} match analysis", "{topic} breakdown"
    * Creator-specific (queries 3-5 for non-political topics): combine topic with the 2-3 most relevant YouTubers who cover this topic type.
      For tech products: MKBHD, Dave2D, Linus Tech Tips.
      For movies/TV: Chris Stuckmann, YMS, Screen Junkies, Pitch Meeting.
      For sports: ESPN, SkySports.
      Format: "MKBHD {topic}" — always include the topic name first.
- news_keywords: 2 to 4 keywords for searching news articles about this topic
- Return ONLY valid JSON. No markdown, no code blocks, no explanation.
"""

FALLBACK = {
    "category": "general",
    "subreddits": ["all"],
    "youtube_queries": [],
    "news_keywords": [],
}


def _basic_fallback(topic_name: str) -> dict:
    """Derive minimal sources from the topic name when Gemini is unavailable."""
    words = [w.lower() for w in topic_name.split() if len(w) >= 3]
    # Use topic words as subreddit guesses + general fallbacks
    subs = list(dict.fromkeys(words + ["all"]))[:4]
    return {
        "category": "general",
        "subreddits": subs,
        "youtube_queries": [
            f"{topic_name} review",
            f"{topic_name} discussion",
        ],
        "news_keywords": [topic_name],
        "youtube_relevant": False,
    }


def _make_gemini_client():
    """Return (client, model_id) — Vertex AI only."""
    project = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project:
        print("[gemini] GOOGLE_CLOUD_PROJECT not set — Gemini unavailable")
        return None, None
    try:
        from google import genai as _genai
        client = _genai.Client(
            vertexai=True,
            project=project,
            location=os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1"),
        )
        return client, "gemini-2.5-flash-lite"
    except Exception as e:
        print(f"[gemini] Vertex AI client init failed: {e}")
        return None, None


def get_sources_for_topic(topic_name: str) -> dict:
    """
    Query Gemini to get the best sources for a topic.
    Returns a dict with category, subreddits, youtube_queries, news_keywords.
    Falls back to safe defaults on any error.
    Uses Vertex AI (high rate limits) with AI Studio as fallback.
    """
    client, model_id = _make_gemini_client()
    if client is None:
        print(f"[gemini] No credentials found — using fallback sources for '{topic_name}'")
        return _basic_fallback(topic_name)

    try:
        import time

        prompt = PROMPT_TEMPLATE.format(topic=topic_name)
        response = None
        for attempt in range(3):
            try:
                response = client.models.generate_content(
                    model=model_id,
                    contents=prompt,
                )
                break
            except Exception as e:
                err_str = str(e)
                # Daily quota exhausted — retrying won't help, fall back immediately
                if "GenerateRequestsPerDayPerProjectPerModel" in err_str or \
                   ("429" in err_str and "day" in err_str.lower()):
                    print(f"[gemini] Daily quota exhausted for '{topic_name}' — using fallback sources")
                    break
                # Per-minute rate limit — short retry is worth it
                elif "429" in err_str and attempt < 2:
                    wait = 15 * (attempt + 1)
                    print(f"[gemini] Rate limited, retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"[gemini] API error for '{topic_name}': {e}")
                    break

        if response is None:
            return _basic_fallback(topic_name)

        raw = response.text.strip()

        # Strip markdown code fences if Gemini wraps in ```json ... ```
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        raw = raw.strip()

        result = json.loads(raw)

        # Validate required keys
        required = {"category", "subreddits", "youtube_queries", "news_keywords"}
        if not required.issubset(result.keys()):
            raise ValueError(f"Missing keys in Gemini response: {required - result.keys()}")

        # Ensure subreddits don't have r/ prefix
        result["subreddits"] = [s.lstrip("r/").strip() for s in result["subreddits"]]

        print(f"[gemini] Sources for '{topic_name}': {result}")
        return result

    except json.JSONDecodeError as e:
        print(f"[gemini] JSON parse error for '{topic_name}': {e} — raw: {raw[:200]}")
    except Exception as e:
        print(f"[gemini] Error getting sources for '{topic_name}': {e}")

    return _basic_fallback(topic_name)
