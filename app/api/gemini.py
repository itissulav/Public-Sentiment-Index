"""
gemini.py
=========
All Gemini API calls for the PSI platform.

Provides:
  Source discovery:
    get_sources_for_topic(topic_name)  → {category, subreddits, youtube_queries, news_keywords}

  Insight generation:
    get_deep_dive_insights(topic_name, insights, charts_data)  → dict
    get_compare_insights(cmp_data, topic_a_name, topic_b_name) → dict
    get_narrative_report(topic_name, insights, psi_rating, source_split) → str
    get_opinion_clusters(topic_name, sampled_comments)          → list
    ask_about_topic(question, topic_name, ...)                  → str
"""

import os
import json
import re
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# Shared client
# ──────────────────────────────────────────────────────────────────────────────

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


def _json_config():
    """
    Build the GenerateContentConfig that forces Gemini to emit valid JSON.
    Imported lazily so a missing SDK doesn't break module import.
    """
    from google.genai import types as _genai_types
    return _genai_types.GenerateContentConfig(response_mime_type="application/json")


def _parse_gemini_json(raw: str, *, context: str):
    """
    Parse a Gemini response that should be JSON.

    Strips optional markdown fences, then json.loads. On failure, logs the
    raw text (truncated) under `context` and re-raises json.JSONDecodeError
    so the caller's existing except-branch fires.
    """
    if raw is None:
        raise json.JSONDecodeError("empty response", "", 0)
    txt = raw.strip()
    txt = re.sub(r"^```(?:json)?\s*", "", txt)
    txt = re.sub(r"\s*```$", "", txt).strip()
    try:
        return json.loads(txt)
    except json.JSONDecodeError as e:
        print(f"[gemini] JSON parse error ({context}): {e} — raw: {txt[:400]}")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Source discovery
# ──────────────────────────────────────────────────────────────────────────────

SOURCES_PROMPT = """
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


def _basic_fallback(topic_name: str) -> dict:
    """Derive minimal sources from the topic name when Gemini is unavailable."""
    words = [w.lower() for w in topic_name.split() if len(w) >= 3]
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


def get_sources_for_topic(topic_name: str) -> dict:
    """
    Query Gemini to get the best sources for a topic.
    Returns a dict with category, subreddits, youtube_queries, news_keywords.
    Falls back to safe defaults on any error.
    """
    client, model_id = _make_gemini_client()
    if client is None:
        print(f"[gemini] No credentials found — using fallback sources for '{topic_name}'")
        return _basic_fallback(topic_name)

    try:
        import time

        prompt = SOURCES_PROMPT.format(topic=topic_name)
        response = None
        for attempt in range(3):
            try:
                response = client.models.generate_content(
                    model=model_id,
                    contents=prompt,
                    config=_json_config(),
                )
                break
            except Exception as e:
                err_str = str(e)
                if "GenerateRequestsPerDayPerProjectPerModel" in err_str or \
                   ("429" in err_str and "day" in err_str.lower()):
                    print(f"[gemini] Daily quota exhausted for '{topic_name}' — using fallback sources")
                    break
                elif "429" in err_str and attempt < 2:
                    wait = 15 * (attempt + 1)
                    print(f"[gemini] Rate limited, retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"[gemini] API error for '{topic_name}': {e}")
                    break

        if response is None:
            return _basic_fallback(topic_name)

        result = _parse_gemini_json(response.text, context=f"sources for '{topic_name}'")

        required = {"category", "subreddits", "youtube_queries", "news_keywords"}
        if not required.issubset(result.keys()):
            raise ValueError(f"Missing keys in Gemini response: {required - result.keys()}")

        result["subreddits"] = [s.lstrip("r/").strip() for s in result["subreddits"]]

        print(f"[gemini] Sources for '{topic_name}': {result}")
        return result

    except json.JSONDecodeError:
        pass  # already logged by _parse_gemini_json
    except Exception as e:
        print(f"[gemini] Error getting sources for '{topic_name}': {e}")

    return _basic_fallback(topic_name)


# ──────────────────────────────────────────────────────────────────────────────
# Deep-dive chart insights
# ──────────────────────────────────────────────────────────────────────────────

DEEP_DIVE_PROMPT = """
You are a data analyst writing plain-English explanations for a public sentiment dashboard.
The audience is non-technical: explain what the data means, not what the chart is.

Topic: "{topic}"
Total comments analyzed: {total_comments}
Date range: {date_range}
Overall sentiment: {positive_pct}% positive, {negative_pct}% negative, {neutral_pct}% neutral

--- WHAT PEOPLE LOVE (top phrases from positive comments, with real quotes) ---
{positive_keywords_json}

--- WHAT PEOPLE CRITICISE (top phrases from negative comments, with real quotes) ---
{negative_keywords_json}

--- CHART DATA ---
{charts_json}

Instructions:
- For "summary": 2 plain-English sentences. Start with the headline sentiment finding. Second sentence gives the most interesting nuance. Max 50 words.
- For "topic_context": 2-3 sentences. Explain what "{topic}" is (as if the reader may not know), why it is in public conversation right now, and any key recent developments driving discussion. Don't reference chart data — this is background context. Max 70 words.
- For "psi_meaning": 1-2 sentences. Explain what the PSI score of {positive_pct}% positive means in plain English — is this high, low, or polarised for this type of topic? Compare to what "average" public opinion looks like. Max 40 words.
- For "keyword_love": Read the actual quotes from positive commenters above and write 2-3 plain-English sentences synthesising what people genuinely love about this topic. Mention the specific themes that appear (e.g. design, performance, price). Quote or paraphrase real phrases where they add colour. Do NOT use "the chart shows" or "this visualization". Max 60 words.
- For "keyword_criticise": Same as above but for negative comments — what specific aspects do people criticise? Be concrete about the complaints. Max 60 words.
- For "viral_posts": 2 sentences. Why did the highest-upvoted posts resonate so strongly? What emotion or argument drove people to engage? Base this on the sentiment split and keyword data. Max 40 words.
- For "debate_summary": 2-3 sentences. What are the 2-3 main camps or arguments in the debate about this topic? Who is on each side and what is the core disagreement? Max 60 words.
- For all other keys: write exactly 2 plain-English sentences. Start with a concrete observation from the numbers. Second sentence explains what it means or why it matters. Max 40 words each.

Return ONLY valid JSON with exactly these keys:
{{
  "summary": "...",
  "topic_context": "...",
  "psi_meaning": "...",
  "keyword_love": "...",
  "keyword_criticise": "...",
  "viral_posts": "...",
  "debate_summary": "...",
  "engagement": "...",
  "scatter": "...",
  "volatility": "...",
  "pos_intensity": "...",
  "neg_intensity": "...",
  "score_dist": "...",
  "text_length": "...",
  "peak_hours": "...",
  "weekly": "...",
  "cumulative": "...",
  "community": "..."
}}
No markdown, no code blocks, no explanation outside JSON.
"""

REQUIRED_KEYS = {
    "summary", "topic_context", "psi_meaning",
    "keyword_love", "keyword_criticise",
    "viral_posts", "debate_summary",
    "engagement", "scatter", "volatility",
    "pos_intensity", "neg_intensity", "score_dist",
    "text_length", "peak_hours", "weekly", "cumulative", "community",
}

EMPTY_INSIGHTS = {k: "" for k in REQUIRED_KEYS}


def _build_keyword_context(keyword_split: dict, sentiment: str, max_items: int = 5) -> list:
    """Extract top phrases + quotes from keyword_split for a given sentiment side."""
    side = keyword_split.get(sentiment.lower(), {})
    labels = side.get("labels", [])
    quotes = side.get("quotes", [])
    scores = side.get("scores", [])
    result = []
    for i, phrase in enumerate(labels[:max_items]):
        entry = {"phrase": phrase}
        if i < len(quotes) and quotes[i]:
            entry["example_quote"] = quotes[i][:200]
        if i < len(scores) and scores[i] is not None:
            entry["upvotes"] = scores[i]
        result.append(entry)
    return result


def _build_charts_summary(charts_data: dict, insights: dict) -> dict:
    """Extract the most useful numbers from chart dicts for Gemini context."""
    summary = {}

    c3 = charts_data.get("chart3_avg_upvotes_sentiment") or {}
    if c3.get("labels") and c3.get("values"):
        summary["engagement_avg_upvotes"] = dict(zip(c3["labels"], c3["values"]))

    c4 = charts_data.get("chart4_sentiment_vs_engagement") or {}
    summary["scatter_sample_size"] = len(c4.get("data", []))

    c6 = charts_data.get("chart6_sentiment_volatility") or {}
    if c6.get("values"):
        vals = [v for v in c6["values"] if v is not None]
        if vals:
            peak_idx = vals.index(max(vals))
            summary["volatility_peak_date"] = (c6.get("labels") or [""])[peak_idx]
            summary["volatility_peak"] = round(max(vals), 3)
            summary["volatility_avg"]  = round(sum(vals) / len(vals), 3)

    for key, chart_key in [("pos_intensity", "chart8_positive_intensity"),
                            ("neg_intensity", "chart7_negative_intensity")]:
        cx = charts_data.get(chart_key) or {}
        if cx.get("values"):
            total = sum(cx["values"]) or 1
            high_bin = cx["values"][-1]
            summary[f"{key}_high_bin_pct"] = round(high_bin / total * 100, 1)

    c13 = charts_data.get("chart13_score_distribution") or {}
    if c13.get("labels") and c13.get("values"):
        total = sum(c13["values"]) or 1
        first = c13["values"][0] if c13["values"] else 0
        summary["score_dist_low_bin_pct"] = round(first / total * 100, 1)
        summary["score_dist"] = dict(zip(c13["labels"], c13["values"]))

    c12 = charts_data.get("chart12_text_length_vs_sentiment") or {}
    summary["text_length_sample"] = len(c12.get("data", []))

    c10 = charts_data.get("chart10_volume_by_hour") or {}
    if c10.get("values"):
        peak_idx = c10["values"].index(max(c10["values"]))
        summary["peak_hour"] = (c10.get("labels") or [""])[peak_idx]
        summary["peak_hour_count"] = max(c10["values"])

    c15 = charts_data.get("chart15_sentiment_by_day") or {}
    if c15.get("labels") and c15.get("datasets"):
        pos_per_day = c15["datasets"].get("Positive", [])
        if pos_per_day and c15["labels"]:
            peak_idx = pos_per_day.index(max(pos_per_day)) if max(pos_per_day) > 0 else 0
            summary["best_day"] = c15["labels"][peak_idx]

    c14 = charts_data.get("chart14_cumulative_posts") or {}
    if c14.get("values") and len(c14["values"]) >= 2:
        summary["total_comments_tracked"] = c14["values"][-1]
        summary["first_day"] = (c14.get("labels") or [""])[0]

    c17 = charts_data.get("chart17_community_breakdown") or {}
    if c17.get("labels") and c17.get("datasets", {}).get("Positive"):
        pos_arr = c17["datasets"]["Positive"]
        labels = c17["labels"]
        if pos_arr:
            max_idx = pos_arr.index(max(pos_arr))
            min_idx = pos_arr.index(min(pos_arr))
            summary["most_positive_community"]  = labels[max_idx]
            summary["most_positive_pct"]        = pos_arr[max_idx]
            summary["most_negative_community"]  = labels[min_idx]
            summary["most_negative_pct"]        = pos_arr[min_idx]

    mom = (insights or {}).get("sentiment_momentum") or {}
    summary["momentum_direction"] = mom.get("direction", "unknown")

    return summary


def get_deep_dive_insights(
    topic_name: str,
    insights: dict,
    charts_data: dict,
) -> dict:
    """
    Call Gemini to generate 2-sentence descriptions for each Deep Dive chart.
    Returns dict with keys matching REQUIRED_KEYS. Empty strings on any error.
    """
    client, model_id = _make_gemini_client()
    if client is None:
        print("[gemini] No credentials — skipping deep dive descriptions")
        return EMPTY_INSIGHTS.copy()

    takeaways = (insights or {}).get("takeaways") or {}
    positive_pct = takeaways.get("pos_pct", 0)
    negative_pct = takeaways.get("neg_pct", 0)
    neutral_pct  = takeaways.get("neu_pct", 0)
    total_comments = takeaways.get("total", 0)

    mom = (insights or {}).get("sentiment_momentum") or {}
    mom_labels = mom.get("labels", [])
    date_range = f"{mom_labels[0]} to {mom_labels[-1]}" if len(mom_labels) >= 2 else "unknown"

    charts_summary = _build_charts_summary(charts_data, insights)
    charts_json = json.dumps(charts_summary, indent=2)[:3000]

    keyword_split = (insights or {}).get("keyword_split", {})
    pos_kw = _build_keyword_context(keyword_split, "positive", max_items=5)
    neg_kw = _build_keyword_context(keyword_split, "negative", max_items=5)
    positive_keywords_json = json.dumps(pos_kw, indent=2) if pos_kw else "[]"
    negative_keywords_json = json.dumps(neg_kw, indent=2) if neg_kw else "[]"

    prompt = DEEP_DIVE_PROMPT.format(
        topic=topic_name or "this topic",
        total_comments=total_comments,
        date_range=date_range,
        positive_pct=round(positive_pct, 1),
        negative_pct=round(negative_pct, 1),
        neutral_pct=round(neutral_pct, 1),
        positive_keywords_json=positive_keywords_json,
        negative_keywords_json=negative_keywords_json,
        charts_json=charts_json,
    )

    try:
        import time
        response = None
        for attempt in range(3):
            try:
                response = client.models.generate_content(
                    model=model_id,
                    contents=prompt,
                    config=_json_config(),
                )
                break
            except Exception as e:
                err_str = str(e)
                if "GenerateRequestsPerDayPerProjectPerModel" in err_str or \
                   ("429" in err_str and "day" in err_str.lower()):
                    print("[gemini] Daily quota exhausted — skipping descriptions")
                    break
                elif "429" in err_str and attempt < 2:
                    wait = 15 * (attempt + 1)
                    print(f"[gemini] Rate limited, retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"[gemini] API error: {e}")
                    break

        if response is None:
            return EMPTY_INSIGHTS.copy()

        result = _parse_gemini_json(response.text, context="deep dive")
        return {k: str(result.get(k, "")) for k in REQUIRED_KEYS}

    except json.JSONDecodeError:
        pass  # already logged by _parse_gemini_json
    except Exception as e:
        print(f"[gemini] Unexpected error (deep dive): {e}")

    return EMPTY_INSIGHTS.copy()


# ──────────────────────────────────────────────────────────────────────────────
# Compare-page insights
# ──────────────────────────────────────────────────────────────────────────────

COMPARE_PROMPT = """
You are a data analyst writing plain-English insights for a public sentiment comparison dashboard.
Audience is non-technical: explain what the data means, not what the charts look like.

Topic A: "{topic_a}"  —  {total_a} comments,  {pos_a}% positive,  {neg_a}% negative
Topic B: "{topic_b}"  —  {total_b} comments,  {pos_b}% positive,  {neg_b}% negative

--- TOPIC A: WHAT PEOPLE LOVE (top phrases, positive comments) ---
{pos_kw_a}

--- TOPIC A: WHAT PEOPLE CRITICISE (top phrases, negative comments) ---
{neg_kw_a}

--- TOPIC B: WHAT PEOPLE LOVE ---
{pos_kw_b}

--- TOPIC B: WHAT PEOPLE CRITICISE ---
{neg_kw_b}

--- KEY NUMBERS ---
{charts_json}

Instructions:
- "overview": 2-3 sentences. Start with the biggest sentiment difference. Then explain what it means and who "wins" public opinion.
- "sentiment_gap": 1-2 sentences. Focus on the Positive vs Negative split numbers. Be specific (e.g. "Topic A is 12 points more positive").
- "timeline_compare": 1-2 sentences. Describe whether the two topics trend together or diverge over time.
- "keywords_a_love": 1-2 sentences. What specific things do Topic A fans praise? Use real phrases from the data.
- "keywords_a_criticise": 1-2 sentences. What do Topic A critics complain about?
- "keywords_b_love": 1-2 sentences. What specific things do Topic B fans praise?
- "keywords_b_criticise": 1-2 sentences. What do Topic B critics complain about?
- "audience_compare": 1-2 sentences. How do the two communities' engagement patterns differ (timing, volume growth, weekly rhythm)?

Return ONLY valid JSON with exactly these 8 keys. No markdown, no code blocks:
{{
  "overview": "...",
  "sentiment_gap": "...",
  "timeline_compare": "...",
  "keywords_a_love": "...",
  "keywords_a_criticise": "...",
  "keywords_b_love": "...",
  "keywords_b_criticise": "...",
  "audience_compare": "..."
}}
"""

COMPARE_KEYS = {
    "overview", "sentiment_gap", "timeline_compare",
    "keywords_a_love", "keywords_a_criticise",
    "keywords_b_love", "keywords_b_criticise",
    "audience_compare",
}
EMPTY_COMPARE = {k: "" for k in COMPARE_KEYS}


def get_compare_insights(cmp_data: dict, topic_a_name: str, topic_b_name: str) -> dict:
    """
    Generate Gemini AI overviews for the compare topics page.
    Returns empty strings on any error.
    """
    client, model_id = _make_gemini_client()
    if client is None:
        print("[gemini] No credentials — skipping compare insights")
        return EMPTY_COMPARE.copy()

    if not cmp_data:
        return EMPTY_COMPARE.copy()

    ta = cmp_data.get("topic_a", {})
    tb = cmp_data.get("topic_b", {})

    split = cmp_data.get("chart_split", {})
    pos_a = split.get("topic_a", [0, 0])[0]
    neg_a = split.get("topic_a", [0, 0])[1] if len(split.get("topic_a", [])) > 1 else 0
    pos_b = split.get("topic_b", [0, 0])[0]
    neg_b = split.get("topic_b", [0, 0])[1] if len(split.get("topic_b", [])) > 1 else 0

    kw_a = cmp_data.get("keywords_a") or {}
    kw_b = cmp_data.get("keywords_b") or {}

    def _kw_list(side, sentiment, n=5):
        s = side.get(sentiment, {})
        labels = s.get("labels", [])[:n]
        quotes = s.get("quotes", [])
        out = []
        for i, phrase in enumerate(labels):
            entry = {"phrase": phrase}
            if i < len(quotes) and quotes[i]:
                entry["example"] = quotes[i][:150]
            out.append(entry)
        return json.dumps(out)

    key_numbers = {
        "sentiment_split_a": {"positive": pos_a, "negative": neg_a},
        "sentiment_split_b": {"positive": pos_b, "negative": neg_b},
        "total_comments_a": ta.get("total_comments", 0),
        "total_comments_b": tb.get("total_comments", 0),
    }

    hours = cmp_data.get("chart_hours", {})
    if hours.get("topic_a") and hours.get("labels"):
        ha = hours["topic_a"]
        key_numbers["peak_hour_a"] = hours["labels"][ha.index(max(ha))]
    if hours.get("topic_b") and hours.get("labels"):
        hb = hours["topic_b"]
        key_numbers["peak_hour_b"] = hours["labels"][hb.index(max(hb))]

    mom = cmp_data.get("chart_momentum", {})
    if mom.get("topic_a"):
        vals = [v for v in mom["topic_a"] if v is not None]
        if vals:
            key_numbers["momentum_last_a"] = round(vals[-1], 1)
    if mom.get("topic_b"):
        vals = [v for v in mom["topic_b"] if v is not None]
        if vals:
            key_numbers["momentum_last_b"] = round(vals[-1], 1)

    cum = cmp_data.get("chart_cumulative", {})
    if cum.get("topic_a"):
        key_numbers["cumulative_final_a"] = cum["topic_a"][-1]
    if cum.get("topic_b"):
        key_numbers["cumulative_final_b"] = cum["topic_b"][-1]

    prompt = COMPARE_PROMPT.format(
        topic_a=topic_a_name,
        topic_b=topic_b_name,
        total_a=ta.get("total_comments", 0),
        total_b=tb.get("total_comments", 0),
        pos_a=round(pos_a, 1),
        neg_a=round(neg_a, 1),
        pos_b=round(pos_b, 1),
        neg_b=round(neg_b, 1),
        pos_kw_a=_kw_list(kw_a, "positive"),
        neg_kw_a=_kw_list(kw_a, "negative"),
        pos_kw_b=_kw_list(kw_b, "positive"),
        neg_kw_b=_kw_list(kw_b, "negative"),
        charts_json=json.dumps(key_numbers, indent=2),
    )

    try:
        import time
        response = None
        for attempt in range(3):
            try:
                response = client.models.generate_content(
                    model=model_id,
                    contents=prompt,
                    config=_json_config(),
                )
                break
            except Exception as e:
                err_str = str(e)
                if "GenerateRequestsPerDayPerProjectPerModel" in err_str or \
                   ("429" in err_str and "day" in err_str.lower()):
                    print("[gemini] Daily quota exhausted (compare)")
                    break
                elif "429" in err_str and attempt < 2:
                    wait = 15 * (attempt + 1)
                    print(f"[gemini] Rate limited, retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"[gemini] API error (compare): {e}")
                    break

        if response is None:
            return EMPTY_COMPARE.copy()

        result = _parse_gemini_json(response.text, context="compare")
        return {k: str(result.get(k, "")) for k in COMPARE_KEYS}

    except json.JSONDecodeError:
        pass  # already logged by _parse_gemini_json
    except Exception as e:
        print(f"[gemini] Unexpected error (compare): {e}")

    return EMPTY_COMPARE.copy()


# ──────────────────────────────────────────────────────────────────────────────
# Narrative report
# ──────────────────────────────────────────────────────────────────────────────

NARRATIVE_PROMPT = """You are a data journalist writing a brief editorial analysis for a public sentiment platform.
Your audience: general public, non-technical readers who want to understand what people think about "{topic}".

DATA:
- PSI Rating: {psi_rating} / 100 (where +100 = unanimously positive, -100 = unanimously negative)
- Sentiment: {positive_pct}% positive, {negative_pct}% negative, {neutral_pct}% neutral
- Total comments: {total_comments}
- Date range: {date_range}
- Dominant emotion: {dominant_emotion}
- Sources: {source_split}

WHAT PEOPLE LOVE (top phrases + real quotes from positive comments):
{positive_keywords}

WHAT PEOPLE CRITICISE (top phrases + real quotes from negative comments):
{negative_keywords}

INSTRUCTIONS:
Write 3-4 paragraphs of flowing editorial prose about "{topic}" based strictly on this data.
- Paragraph 1: Lead with the headline finding — what is the overall mood and why?
- Paragraph 2: What are people most enthusiastic about? Ground it in the actual phrases and quotes.
- Paragraph 3: What are the main criticisms or concerns? Quote real phrases.
- Paragraph 4 (optional): Interesting nuance — platform differences, intensity, any surprising patterns.

Rules:
- Write as if publishing in a newsletter or blog. No bullet points. Flowing sentences.
- Reference real phrases and quotes from the data to make it feel grounded.
- Do NOT use "the data shows", "according to our analysis", or technical jargon.
- Do NOT fabricate events or context not in the data.
- Use **bold** sparingly for key phrases only.
- Maximum 250 words total.
- Return plain text/markdown only — no JSON, no code blocks.
"""


def get_narrative_report(
    topic_name: str,
    insights: dict,
    psi_rating: float = 0,
    source_split: dict = None,
) -> str:
    """
    Generate a journalist-style 3-4 paragraph editorial about the topic.
    Returns markdown string. Falls back to "" on any error.
    """
    client, model_id = _make_gemini_client()
    if client is None:
        return ""

    takeaways = (insights or {}).get("takeaways") or {}
    positive_pct   = takeaways.get("pos_pct", 0)
    negative_pct   = takeaways.get("neg_pct", 0)
    neutral_pct    = takeaways.get("neu_pct", 0)
    total_comments = takeaways.get("total", 0)
    dominant_emotion = takeaways.get("dominant_emotion", "mixed")

    mom = (insights or {}).get("sentiment_momentum") or {}
    mom_labels = mom.get("labels", [])
    date_range = f"{mom_labels[0]} to {mom_labels[-1]}" if len(mom_labels) >= 2 else "recent period"

    keyword_split = (insights or {}).get("keyword_split", {})
    pos_kw = _build_keyword_context(keyword_split, "positive", max_items=5)
    neg_kw = _build_keyword_context(keyword_split, "negative", max_items=5)

    if source_split:
        reddit_pct = round(source_split.get("reddit", 0) * 100)
        yt_pct = round(source_split.get("youtube", 0) * 100)
        split_str = f"{reddit_pct}% Reddit, {yt_pct}% YouTube"
    else:
        split_str = "Reddit + YouTube"

    prompt = NARRATIVE_PROMPT.format(
        topic=topic_name or "this topic",
        psi_rating=round(psi_rating, 1),
        positive_pct=round(positive_pct, 1),
        negative_pct=round(negative_pct, 1),
        neutral_pct=round(neutral_pct, 1),
        total_comments=total_comments,
        date_range=date_range,
        dominant_emotion=dominant_emotion,
        source_split=split_str,
        positive_keywords=json.dumps(pos_kw, indent=2)[:1200],
        negative_keywords=json.dumps(neg_kw, indent=2)[:1200],
    )

    try:
        response = client.models.generate_content(model=model_id, contents=prompt)
        return response.text.strip()
    except Exception as e:
        print(f"[gemini] Error (narrative): {e}")
        return ""


# ──────────────────────────────────────────────────────────────────────────────
# Opinion clusters
# ──────────────────────────────────────────────────────────────────────────────

CLUSTERS_PROMPT = """You are an audience analyst for a public sentiment platform.
Analyze these {sample_size} comments about "{topic}" and identify 3-5 distinct opinion groups.

COMMENTS (format: [SENTIMENT/SOURCE] comment text):
{comments}

INSTRUCTIONS:
Identify 3-5 distinct groups of people by their attitude or opinion about "{topic}".
Each cluster should represent a meaningfully different perspective — not just "positive" vs "negative" but specific sub-groups.

For each cluster, provide:
- label: 2-4 word descriptive name (e.g. "Nostalgic Fans", "Price Skeptics", "Casual Newcomers")
- pct: estimated % of the audience this cluster represents (all pcts must sum to ~100)
- summary: 1 sentence describing what this group thinks
- quote: one short representative quote (15-30 words) from the actual comments above

Return ONLY a valid JSON array. No markdown, no code blocks:
[
  {{"label": "...", "pct": 38, "summary": "...", "quote": "..."}},
  {{"label": "...", "pct": 29, "summary": "...", "quote": "..."}},
  ...
]
"""


def get_opinion_clusters(topic_name: str, sampled_comments: list) -> list:
    """
    Identify 3-5 opinion clusters from a stratified comment sample.
    Returns list of dicts: [{label, pct, summary, quote}, ...]
    Falls back to [] on any error.
    """
    client, model_id = _make_gemini_client()
    if client is None:
        return []

    if not sampled_comments:
        return []

    lines = []
    for c in sampled_comments[:120]:
        text = (c.get("text") or "")[:180].replace("\n", " ")
        sentiment = c.get("sentiment_label", "")
        source = c.get("source_type", "")
        lines.append(f"[{sentiment}/{source}] {text}")

    prompt = CLUSTERS_PROMPT.format(
        topic=topic_name or "this topic",
        sample_size=len(lines),
        comments="\n".join(lines)[:5000],
    )

    try:
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=_json_config(),
        )
        clusters = _parse_gemini_json(response.text, context="clusters")
        if isinstance(clusters, list):
            return [
                {
                    "label":   str(c.get("label", ""))[:50],
                    "pct":     int(c.get("pct", 0)),
                    "summary": str(c.get("summary", ""))[:200],
                    "quote":   str(c.get("quote", ""))[:200],
                }
                for c in clusters if isinstance(c, dict)
            ]
    except json.JSONDecodeError:
        pass  # already logged by _parse_gemini_json
    except Exception as e:
        print(f"[gemini] Error (clusters): {e}")

    return []


# ──────────────────────────────────────────────────────────────────────────────
# Ask anything — conversational Q&A
# ──────────────────────────────────────────────────────────────────────────────

ASK_SYSTEM_PROMPT = """You are an expert analyst for a public sentiment intelligence platform.
You have access to real comment data collected from Reddit and YouTube about "{topic}".

DATASET CONTEXT:
- Total comments: {total_comments}
- Date range: {date_range}
- Sentiment breakdown: {positive_pct}% positive, {negative_pct}% negative, {neutral_pct}% neutral
- PSI Rating: {psi_rating} (scale -100 to +100, where +100 = overwhelmingly positive)
- Dominant emotion: {dominant_emotion}
- Source split: {source_split}

WHAT PEOPLE LOVE (top phrases from positive comments, with real quotes):
{positive_keywords}

WHAT PEOPLE CRITICISE (top phrases from negative comments, with real quotes):
{negative_keywords}

TOP POSITIVE COMMENT (most upvoted):
{top_positive_comment}

TOP NEGATIVE COMMENT (most upvoted):
{top_negative_comment}

SAMPLED COMMENTS (representative mix of {sample_size} comments):
{sampled_comments}

INSTRUCTIONS:
- Answer the user's question directly and concisely using the actual data above.
- Ground every claim in the data — quote real comments when they add colour.
- If the data doesn't support a definitive answer, say so honestly.
- Speak conversationally, not like a report. 2-5 sentences unless a longer answer is clearly needed.
- Never say "as an AI" or "based on the data provided to me". Just answer.
- Format with markdown if it helps readability (bold, bullets).
"""


def ask_about_topic(
    question: str,
    topic_name: str,
    insights: dict,
    top_positive: list,
    top_negative: list,
    sampled_comments: list,
    psi_rating: float = 0,
    source_split: dict = None,
) -> str:
    """
    Answer a free-form question about a topic using Gemini + real comment data.
    Returns a markdown-formatted answer string, or an error message on failure.
    """
    client, model_id = _make_gemini_client()
    if client is None:
        return "Gemini is currently unavailable. Please try again later."

    takeaways = (insights or {}).get("takeaways") or {}
    positive_pct = takeaways.get("pos_pct", 0)
    negative_pct = takeaways.get("neg_pct", 0)
    neutral_pct  = takeaways.get("neu_pct", 0)
    total_comments = takeaways.get("total", 0)
    dominant_emotion = takeaways.get("dominant_emotion", "unknown")

    mom = (insights or {}).get("sentiment_momentum") or {}
    mom_labels = mom.get("labels", [])
    date_range = f"{mom_labels[0]} to {mom_labels[-1]}" if len(mom_labels) >= 2 else "recent period"

    keyword_split = (insights or {}).get("keyword_split", {})
    pos_kw = _build_keyword_context(keyword_split, "positive", max_items=6)
    neg_kw = _build_keyword_context(keyword_split, "negative", max_items=6)

    def _fmt_comment(c):
        if not c:
            return "N/A"
        text = (c.get("text") or "")[:300]
        score = c.get("score", 0)
        source = c.get("source_type", "")
        return f'"{text}" (score: {score}, source: {source})'

    top_pos_str = _fmt_comment(top_positive[0] if top_positive else None)
    top_neg_str = _fmt_comment(top_negative[0] if top_negative else None)

    sample_lines = []
    for c in sampled_comments[:80]:
        text = (c.get("text") or "")[:200].replace("\n", " ")
        sentiment = c.get("sentiment_label", "")
        source = c.get("source_type", "")
        sample_lines.append(f"[{sentiment}/{source}] {text}")
    sampled_str = "\n".join(sample_lines) if sample_lines else "No comments available."

    if source_split:
        reddit_pct = round(source_split.get("reddit", 0) * 100)
        yt_pct = round(source_split.get("youtube", 0) * 100)
        split_str = f"{reddit_pct}% Reddit, {yt_pct}% YouTube"
    else:
        split_str = "Reddit + YouTube"

    system = ASK_SYSTEM_PROMPT.format(
        topic=topic_name or "this topic",
        total_comments=total_comments,
        date_range=date_range,
        positive_pct=round(positive_pct, 1),
        negative_pct=round(negative_pct, 1),
        neutral_pct=round(neutral_pct, 1),
        psi_rating=round(psi_rating, 1),
        dominant_emotion=dominant_emotion,
        source_split=split_str,
        positive_keywords=json.dumps(pos_kw, indent=2)[:1500],
        negative_keywords=json.dumps(neg_kw, indent=2)[:1500],
        top_positive_comment=top_pos_str,
        top_negative_comment=top_neg_str,
        sample_size=len(sample_lines),
        sampled_comments=sampled_str[:4000],
    )

    full_prompt = f"{system}\n\nUSER QUESTION: {question}"

    try:
        response = client.models.generate_content(
            model=model_id,
            contents=full_prompt,
        )
        return response.text.strip()
    except Exception as e:
        err_str = str(e)
        if "429" in err_str or "quota" in err_str.lower():
            return "Daily request limit reached. Please try again tomorrow."
        print(f"[gemini] Error (ask): {e}")
        return "An error occurred while processing your question. Please try again."
