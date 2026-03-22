"""
gemini_insights.py
==================
Generates plain-English 2-sentence descriptions for each "Deep Dive" chart
using Gemini (Vertex AI preferred, AI Studio fallback).

Called once per analysis from compute_all_insights() and injected into the
template as insights_*.gemini_insights.

Returns a dict with keys:
  summary           — 2-sentence overall sentiment summary for the topic
  topic_context     — what this topic is and why people are talking about it now
  psi_meaning       — plain-English meaning of the PSI score (below the gauge)
  keyword_love      — 2-3 sentence synthesis of what people love (from real quotes)
  keyword_criticise — 2-3 sentence synthesis of what people criticise (from real quotes)
  viral_posts       — why the top-upvoted posts resonated and went viral
  debate_summary    — summary of the main arguments/camps in the debate
  engagement        — chart3: avg upvotes by sentiment
  scatter           — chart4: positivity vs virality scatter
  volatility        — chart6: daily sentiment volatility
  pos_intensity     — chart8: positive confidence histogram
  neg_intensity     — chart7: negative confidence histogram
  score_dist        — chart13: score distribution / viral inequality
  text_length       — chart12: comment length vs positivity
  peak_hours        — chart10: volume by hour
  weekly            — chart15: sentiment by day of week
  cumulative        — chart14: cumulative discussion growth
  community         — chart17: sentiment by subreddit / video
"""

import os
import json
import re
from dotenv import load_dotenv

load_dotenv()

PROMPT_TEMPLATE = """
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

    # chart3: avg upvotes by sentiment
    c3 = charts_data.get("chart3_avg_upvotes_sentiment") or {}
    if c3.get("labels") and c3.get("values"):
        summary["engagement_avg_upvotes"] = dict(zip(c3["labels"], c3["values"]))

    # chart4: scatter sample (just count points)
    c4 = charts_data.get("chart4_sentiment_vs_engagement") or {}
    summary["scatter_sample_size"] = len(c4.get("data", []))

    # chart6: volatility — peak and average
    c6 = charts_data.get("chart6_sentiment_volatility") or {}
    if c6.get("values"):
        vals = [v for v in c6["values"] if v is not None]
        if vals:
            peak_idx = vals.index(max(vals))
            summary["volatility_peak_date"] = (c6.get("labels") or [""])[peak_idx]
            summary["volatility_peak"] = round(max(vals), 3)
            summary["volatility_avg"]  = round(sum(vals) / len(vals), 3)

    # chart7/8: intensity histograms (% in highest bin)
    for key, chart_key in [("pos_intensity", "chart8_positive_intensity"),
                            ("neg_intensity", "chart7_negative_intensity")]:
        cx = charts_data.get(chart_key) or {}
        if cx.get("values"):
            total = sum(cx["values"]) or 1
            high_bin = cx["values"][-1]
            summary[f"{key}_high_bin_pct"] = round(high_bin / total * 100, 1)

    # chart13: score distribution
    c13 = charts_data.get("chart13_score_distribution") or {}
    if c13.get("labels") and c13.get("values"):
        total = sum(c13["values"]) or 1
        first = c13["values"][0] if c13["values"] else 0
        summary["score_dist_low_bin_pct"] = round(first / total * 100, 1)
        summary["score_dist"] = dict(zip(c13["labels"], c13["values"]))

    # chart12: scatter (just note it exists)
    c12 = charts_data.get("chart12_text_length_vs_sentiment") or {}
    summary["text_length_sample"] = len(c12.get("data", []))

    # chart10: peak hour
    c10 = charts_data.get("chart10_volume_by_hour") or {}
    if c10.get("values"):
        peak_idx = c10["values"].index(max(c10["values"]))
        summary["peak_hour"] = (c10.get("labels") or [""])[peak_idx]
        summary["peak_hour_count"] = max(c10["values"])

    # chart15: best/worst day
    c15 = charts_data.get("chart15_sentiment_by_day") or {}
    if c15.get("labels") and c15.get("datasets"):
        pos_per_day = c15["datasets"].get("Positive", [])
        if pos_per_day and c15["labels"]:
            peak_idx = pos_per_day.index(max(pos_per_day)) if max(pos_per_day) > 0 else 0
            summary["best_day"] = c15["labels"][peak_idx]

    # chart14: growth direction
    c14 = charts_data.get("chart14_cumulative_posts") or {}
    if c14.get("values") and len(c14["values"]) >= 2:
        summary["total_comments_tracked"] = c14["values"][-1]
        summary["first_day"] = (c14.get("labels") or [""])[0]

    # chart17: community breakdown — top and bottom community by positive %
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

    # momentum direction from insights
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

    Parameters
    ----------
    topic_name  : human-readable topic name
    insights    : result of compute_all_insights() (for takeaways, momentum)
    charts_data : result of get_all_charts_data() (for numeric chart values)

    Returns
    -------
    dict with keys: summary, engagement, scatter, volatility, pos_intensity,
                    neg_intensity, score_dist, text_length, peak_hours, weekly,
                    cumulative, community
    Empty strings for all keys on any error.
    """
    from app.utils.gemini_sources import _make_gemini_client

    client, model_id = _make_gemini_client()
    if client is None:
        print("[gemini_insights] No credentials — skipping deep dive descriptions")
        return EMPTY_INSIGHTS.copy()

    # Extract takeaways for context
    takeaways = (insights or {}).get("takeaways") or {}
    positive_pct = takeaways.get("pos_pct", 0)
    negative_pct = takeaways.get("neg_pct", 0)
    neutral_pct  = takeaways.get("neu_pct", 0)
    total_comments = takeaways.get("total", 0)

    # Date range from momentum labels
    mom = (insights or {}).get("sentiment_momentum") or {}
    mom_labels = mom.get("labels", [])
    date_range = f"{mom_labels[0]} to {mom_labels[-1]}" if len(mom_labels) >= 2 else "unknown"

    charts_summary = _build_charts_summary(charts_data, insights)
    charts_json = json.dumps(charts_summary, indent=2)[:3000]  # cap at 3KB

    # Build keyword context from the same phrases/quotes shown to users
    keyword_split = (insights or {}).get("keyword_split", {})
    pos_kw = _build_keyword_context(keyword_split, "positive", max_items=5)
    neg_kw = _build_keyword_context(keyword_split, "negative", max_items=5)
    positive_keywords_json  = json.dumps(pos_kw, indent=2) if pos_kw else "[]"
    negative_keywords_json  = json.dumps(neg_kw, indent=2) if neg_kw else "[]"

    prompt = PROMPT_TEMPLATE.format(
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
                )
                break
            except Exception as e:
                err_str = str(e)
                if "GenerateRequestsPerDayPerProjectPerModel" in err_str or \
                   ("429" in err_str and "day" in err_str.lower()):
                    print(f"[gemini_insights] Daily quota exhausted — skipping descriptions")
                    break
                elif "429" in err_str and attempt < 2:
                    wait = 15 * (attempt + 1)
                    print(f"[gemini_insights] Rate limited, retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"[gemini_insights] API error: {e}")
                    break

        if response is None:
            return EMPTY_INSIGHTS.copy()

        raw = response.text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        raw = raw.strip()

        result = json.loads(raw)

        # Fill any missing keys with empty string rather than crashing
        return {k: str(result.get(k, "")) for k in REQUIRED_KEYS}

    except json.JSONDecodeError as e:
        print(f"[gemini_insights] JSON parse error: {e}")
    except Exception as e:
        print(f"[gemini_insights] Unexpected error: {e}")

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
    Uses the overall (all-sources) comparison data to produce 8 insight strings.
    Returns empty strings on any error.
    """
    from app.utils.gemini_sources import _make_gemini_client

    client, model_id = _make_gemini_client()
    if client is None:
        print("[gemini_compare] No credentials — skipping compare insights")
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
                )
                break
            except Exception as e:
                err_str = str(e)
                if "GenerateRequestsPerDayPerProjectPerModel" in err_str or \
                   ("429" in err_str and "day" in err_str.lower()):
                    print("[gemini_compare] Daily quota exhausted")
                    break
                elif "429" in err_str and attempt < 2:
                    wait = 15 * (attempt + 1)
                    print(f"[gemini_compare] Rate limited, retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"[gemini_compare] API error: {e}")
                    break

        if response is None:
            return EMPTY_COMPARE.copy()

        raw = response.text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        result = json.loads(raw.strip())
        return {k: str(result.get(k, "")) for k in COMPARE_KEYS}

    except json.JSONDecodeError as e:
        print(f"[gemini_compare] JSON parse error: {e}")
    except Exception as e:
        print(f"[gemini_compare] Unexpected error: {e}")

    return EMPTY_COMPARE.copy()
