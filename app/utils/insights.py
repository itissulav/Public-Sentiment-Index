"""
insights.py
===========
Compute all text and chart insights from a classified comments DataFrame.
Updated for v2 schema: uses emotion_label, emotion_scores columns.
"""

import re
import json
import pandas as pd
from collections import Counter

STOPWORDS = {
    'a', 'an', 'the', 'and', 'or', 'but', 'if', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
    'before', 'after', 'above', 'below', 'between', 'out', 'off', 'over',
    'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when',
    'where', 'why', 'how', 'all', 'both', 'each', 'few', 'more', 'most',
    'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same',
    'so', 'than', 'too', 'very', 'just', 'as', 'is', 'are', 'was', 'were',
    'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
    'will', 'would', 'could', 'should', 'may', 'might', 'shall', 'can',
    'must', 'per', 'via', 'vs', 'aka', 'etc',
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves',
    'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his',
    'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
    'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which',
    'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'get', 'got',
    'also', 'even', 'back', 'still', 'much', 'well', 'now', 'like', 'one',
    'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
    'hundred', 'thousand', 'million', 'billion',
    'said', 'say', 'think', 'know', 'see', 'make', 'go',
    'come', 'take', 'want', 'look', 'use', 'find', 'give', 'tell', 'work',
    'way', 'new', 'good', 'bad', 'first', 'last', 'long', 'great', 'little',
    'right', 'big', 'high', 'old', 'next', 'let', 'put', 'need', 'keep',
    'born', 'made', 'done', 'went', 'left', 'own', 'set', 'run',
    've', 're', 'll', 'd', 's', 't', 'don', 'doesn', 'didn', 'isn', 'aren',
    'wasn', 'weren', 'hasn', 'haven', 'hadn', 'won', 'wouldn', 'couldn',
    'shouldn', 'actually', 'really', 'literally', 'basically', 'probably',
    'people', 'thing', 'things', 'time', 'year', 'years', 'day', 'days',
    'guy', 'guys', 'man', 'men', 'woman', 'women', 'lot', 'lots', 'bit',
    'https', 'http', 'www', 'com', 'reddit', 'edit', 'post', 'comment',
    'deleted', 'removed', 'null', 'none', 'just', 'going', 'watch', 'show',
    'never', 'always', 'every', 'anyone', 'everyone', 'someone', 'nothing',
    'something', 'anything', 'thought', 'feel', 'feels', 'felt', 'seems',
    'seem', 'mean', 'means', 'pretty', 'kind',
    # internet / media noise
    'gif', 'giphy', 'imgur', 'amp', 'lol', 'lmao', 'omg', 'wtf', 'smh',
    'yeah', 'yea', 'yep', 'nope', 'okay', 'ok', 'hey', 'oh', 'ah',
    'upvote', 'downvote', 'karma', 'sub', 'subreddit', 'thread', 'bot',
    'iirc', 'imo', 'imho', 'afaik', 'tldr', 'eli',
}

EMOTION_COLORS = {
    "positive": "#10b981",
    "negative": "#ef4444",
    "neutral":  "#6b7280",
}

EMOTION_LABELS = ["positive", "negative", "neutral"]


def _tokenize(text):
    text = str(text).lower()
    text = re.sub(r'https?://\S+', ' ', text)
    text = re.sub(r'[^a-z\s]', ' ', text)
    # min length 4 cuts single-char artifacts and 3-char filler ('etc' already in stopwords but catches others)
    return [w for w in text.split() if w not in STOPWORDS and len(w) >= 4]


def _make_bigrams(tokens):
    """
    Return bigrams only where both words are adjacent in the *original* token list
    AND the two words are distinct (filters 'usa usa', 'etc etc' etc.).
    """
    return [
        f"{tokens[i]} {tokens[i+1]}"
        for i in range(len(tokens) - 1)
        if tokens[i] != tokens[i+1]
    ]


def _expand_emotion_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    If 'emotion_scores' column exists as JSONB/dict, expand it into individual columns.
    Safe to call even if columns already exist.
    """
    if "emotion_scores" not in df.columns:
        return df

    def _parse(x):
        if isinstance(x, dict):
            return x
        if isinstance(x, str):
            try:
                return json.loads(x)
            except Exception:
                return {}
        return {}

    expanded = df["emotion_scores"].apply(_parse).apply(pd.Series)
    # Only add columns that aren't already present
    for col in expanded.columns:
        if col not in df.columns:
            df = df.copy()
            df[col] = expanded[col]
    return df


def get_emotion_distribution(df: pd.DataFrame) -> dict | None:
    """
    Returns the average emotion score across all comments as a chart-ready dict.
    {
      "labels": ["anger", "disgust", ...],
      "values": [0.12, 0.05, ...],
      "colors": ["#ef4444", ...]
    }
    """
    df = _expand_emotion_scores(df)
    present = [e for e in EMOTION_LABELS if e in df.columns]
    if not present:
        # Fall back to counting emotion_label if scores unavailable
        if "emotion_label" not in df.columns:
            return None
        counts = df["emotion_label"].value_counts(normalize=True)
        present = [e for e in EMOTION_LABELS if e in counts.index]
        if not present:
            return None
        values = [round(float(counts.get(e, 0)), 4) for e in present]
    else:
        values = [round(float(df[e].mean()), 4) for e in present]

    return {
        "labels": present,
        "values": values,
        "colors": [EMOTION_COLORS.get(e, "#6b7280") for e in present],
    }


def get_keyword_split(df, top_n=10):
    # Sample for speed: bigram counting doesn't need the full dataset
    MAX_ROWS = 3000
    df_sample = df.sample(n=min(MAX_ROWS, len(df)), random_state=42) if len(df) > MAX_ROWS else df

    # Collect bigrams using plain Python lists (avoids pandas row-access overhead)
    texts  = df_sample['text'].fillna('').tolist()
    labels = df_sample['sentiment_label'].fillna('').tolist()
    pos_phrases, neg_phrases = [], []
    for text, label in zip(texts, labels):
        tokens = _tokenize(text)
        bigrams = _make_bigrams(tokens)
        if label == 'Positive':
            pos_phrases.extend(bigrams)
        elif label == 'Negative':
            neg_phrases.extend(bigrams)

    pos_counts = Counter(pos_phrases)
    neg_counts = Counter(neg_phrases)
    total_pos = max(len(pos_phrases), 1)
    total_neg = max(len(neg_phrases), 1)

    # Pre-lowercase text once for quote lookup (avoids N×str.lower() calls)
    df_lookup = df.copy()
    df_lookup['_tl'] = df_lookup['text'].fillna('').str.lower()

    def _is_discriminative(phrase, own_count, own_total, other_counts, other_total):
        """Return True if phrase is at least 2× more common in own sentiment than other."""
        own_rate   = own_count / own_total
        other_rate = other_counts.get(phrase, 0) / other_total
        return own_rate >= 1.5 * other_rate

    def _top_with_quotes(counts, other_counts, other_total, own_total, sentiment, n):
        results = []
        seen_quotes = set()
        sent_mask = df_lookup['sentiment_label'] == sentiment
        for phrase, count in counts.most_common(n * 10):
            if not _is_discriminative(phrase, count, own_total, other_counts, other_total):
                continue
            w0, w1 = phrase.split()
            # regex=False is ~10× faster than default regex matching
            mask = sent_mask & \
                   df_lookup['_tl'].str.contains(w0, na=False, regex=False) & \
                   df_lookup['_tl'].str.contains(w1, na=False, regex=False)
            quote = score = subreddit = post_title = author = None
            if mask.any():
                top5 = df_lookup[mask].nlargest(5, 'score')
                for row in top5.itertuples(index=False):
                    text = str(getattr(row, 'text', '') or '')
                    candidate_quote = (text[:220] + '…') if len(text) > 220 else text
                    if candidate_quote not in seen_quotes:
                        quote      = candidate_quote
                        score      = int(getattr(row, 'score', 0) or 0)
                        subreddit  = str(getattr(row, 'source_id', '') or '') or None
                        post_title = str(getattr(row, 'post_title', '') or '') or None
                        author     = str(getattr(row, 'author', '') or '') or None
                        if post_title and len(post_title) > 80:
                            post_title = post_title[:80] + '…'
                        seen_quotes.add(quote)
                        break
            # Skip phrases with no matching quote — don't show empty cards
            if quote is None:
                continue
            results.append({
                'phrase':     phrase,
                'count':      count,
                'quote':      quote,
                'score':      score,
                'subreddit':  subreddit,
                'post_title': post_title,
                'author':     author,
            })
            if len(results) == n:
                break
        return results

    pos_top = _top_with_quotes(pos_counts, neg_counts, total_neg, total_pos, 'Positive', top_n)
    neg_top = _top_with_quotes(neg_counts, pos_counts, total_pos, total_neg, 'Negative', top_n)

    # Remove phrases that appear in both lists, then equalize counts
    neg_phrase_set = {r['phrase'] for r in neg_top}
    pos_phrase_set = {r['phrase'] for r in pos_top}
    pos_top = [r for r in pos_top if r['phrase'] not in neg_phrase_set]
    neg_top = [r for r in neg_top if r['phrase'] not in pos_phrase_set]
    n_equal = min(len(pos_top), len(neg_top))
    pos_top, neg_top = pos_top[:n_equal], neg_top[:n_equal]

    def _serialise(items):
        return {
            'labels':     [r['phrase']     for r in items],
            'values':     [r['count']      for r in items],
            'quotes':     [r['quote']      for r in items],
            'scores':     [r['score']      for r in items],
            'subreddits': [r['subreddit']  for r in items],
            'titles':     [r['post_title'] for r in items],
            'authors':    [r['author']     for r in items],
        }

    return {'positive': _serialise(pos_top), 'negative': _serialise(neg_top)}


def get_post_title_debate(df, top_n=10):
    if 'post_title' not in df.columns:
        return None

    df = df.copy()
    df['post_title'] = df['post_title'].fillna('(no title)').astype(str)

    grouped = (
        df.groupby('post_title')['sentiment_label']
        .value_counts()
        .unstack(fill_value=0)
    )
    for col in ['Positive', 'Negative', 'Neutral']:
        if col not in grouped.columns:
            grouped[col] = 0

    grouped['total'] = grouped[['Positive', 'Negative', 'Neutral']].sum(axis=1)
    grouped = grouped.nlargest(top_n, 'total')

    labels = [(t[:65] + '…') if len(t) > 65 else t for t in grouped.index.tolist()]

    # Extract top debate keywords per post title (what's actually being discussed)
    # Use itertuples (5-10× faster than iterrows) and cap per-title rows for speed
    keywords_per_title = []
    MAX_PER_TITLE = 300
    for raw_title in grouped.index.tolist():
        title_df = df[df['post_title'] == raw_title]
        if len(title_df) > MAX_PER_TITLE:
            title_df = title_df.sample(n=MAX_PER_TITLE, random_state=42)
        bigrams = []
        for row in title_df.itertuples(index=False):
            tokens = _tokenize(str(getattr(row, 'text', '') or ''))
            bigrams.extend(_make_bigrams(tokens))
        top3 = [phrase for phrase, _ in Counter(bigrams).most_common(3)]
        keywords_per_title.append(top3)

    # Return percentages directly — avoids error-prone JS normalization
    raw_totals = grouped['total'].tolist()
    pct_datasets = {}
    for col in ['Positive', 'Negative', 'Neutral']:
        pct_datasets[col] = [
            round(float(grouped[col].iloc[i]) / grouped['total'].iloc[i] * 100, 1)
            if grouped['total'].iloc[i] > 0 else 0.0
            for i in range(len(grouped))
        ]

    return {
        'labels':   labels,
        'datasets': pct_datasets,    # percentages (0–100), not raw counts
        'totals':   raw_totals,      # raw comment counts per title (for display)
        'keywords': keywords_per_title,
    }


def get_sentiment_momentum(df):
    """7-day rolling net sentiment. Works with both 'timestamp' (v1) and 'published_at' (v2)."""
    date_col = 'published_at' if 'published_at' in df.columns else 'timestamp'
    if date_col not in df.columns:
        return None

    df = df.copy()
    df['date'] = pd.to_datetime(df[date_col], errors='coerce', utc=True).dt.date
    df = df.dropna(subset=['date'])
    if df.empty:
        return None

    daily = (
        df.groupby('date')['sentiment_label']
        .apply(lambda x: int((x == 'Positive').sum()) - int((x == 'Negative').sum()))
        .reset_index()
    )
    daily.columns = ['date', 'net']
    daily = daily.sort_values('date')
    daily['rolling'] = daily['net'].rolling(7, min_periods=1).mean().round(2)

    if len(daily) >= 14:
        direction = 'improving' if daily['rolling'].tail(7).mean() > daily['rolling'].head(7).mean() else 'declining'
    else:
        direction = 'improving' if daily['rolling'].iloc[-1] > 0 else 'declining'

    return {
        'labels':    [str(d) for d in daily['date'].tolist()],
        'values':    [float(v) for v in daily['rolling'].tolist()],
        'direction': direction,
    }


def get_division_score(df):
    total = len(df)
    if total == 0:
        return 50.0
    pos = int((df['sentiment_label'] == 'Positive').sum())
    neg = int((df['sentiment_label'] == 'Negative').sum())
    neu = total - pos - neg
    dominant = max(pos, neg, neu)
    return round((1 - dominant / total) * 100, 1)


def get_key_takeaways(df):
    total = len(df)
    if total == 0:
        return None

    pos = int((df['sentiment_label'] == 'Positive').sum())
    neg = int((df['sentiment_label'] == 'Negative').sum())
    neu = total - pos - neg

    most_debated = None
    if 'post_title' in df.columns:
        counts = df['post_title'].value_counts()
        if not counts.empty:
            t = str(counts.index[0])
            most_debated = (t[:80] + '…') if len(t) > 80 else t

    # Dominant emotion across all comments
    dominant_emotion = None
    if 'emotion_label' in df.columns:
        ec = df['emotion_label'].value_counts()
        if not ec.empty:
            dominant_emotion = str(ec.index[0])

    kw = get_keyword_split(df, top_n=3)
    top_praised    = kw['positive']['labels'][0] if kw['positive']['labels'] else None
    top_criticised = kw['negative']['labels'][0] if kw['negative']['labels'] else None

    return {
        'total':           total,
        'pos_pct':         round(pos / total * 100, 1),
        'neg_pct':         round(neg / total * 100, 1),
        'neu_pct':         round(neu / total * 100, 1),
        'most_debated':    most_debated,
        'top_praised':     top_praised,
        'top_criticised':  top_criticised,
        'division_score':  get_division_score(df),
        'dominant_emotion': dominant_emotion,
    }


def compute_all_insights(df, topic_name: str = "", charts_data: dict = None):
    """Single entry point — returns the full insights dict for the template.

    Parameters
    ----------
    df          : classified comments DataFrame
    topic_name  : human-readable topic name (used for Gemini context)
    charts_data : result of get_all_charts_data() (used for Gemini context)
    """
    try:
        df = _expand_emotion_scores(df)
        result = {
            'keyword_split':       get_keyword_split(df),
            'post_title_debate':   get_post_title_debate(df),
            'sentiment_momentum':  get_sentiment_momentum(df),
            'takeaways':           get_key_takeaways(df),
            'emotion_distribution': get_emotion_distribution(df),
            'gemini_insights':     {},
        }

        # Generate Gemini chart descriptions (requires topic_name + charts_data)
        if topic_name and charts_data:
            try:
                from app.utils.gemini_insights import get_deep_dive_insights
                result['gemini_insights'] = get_deep_dive_insights(
                    topic_name, result, charts_data
                )
            except Exception as e:
                print(f"[insights] Gemini deep dive skipped: {e}")

        return result
    except Exception as e:
        print(f"[insights] Error computing insights: {e}")
        return {}
