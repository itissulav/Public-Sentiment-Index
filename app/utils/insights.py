import re
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
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves',
    'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his',
    'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
    'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which',
    'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'get', 'got',
    'also', 'even', 'back', 'still', 'much', 'well', 'now', 'like', 'one',
    'two', 'three', 'said', 'say', 'think', 'know', 'see', 'make', 'go',
    'come', 'take', 'want', 'look', 'use', 'find', 'give', 'tell', 'work',
    'way', 'new', 'good', 'bad', 'first', 'last', 'long', 'great', 'little',
    'right', 'big', 'high', 'old', 'next', 'let', 'put', 'need', 'keep',
    've', 're', 'll', 'd', 's', 't', 'don', 'doesn', 'didn', 'isn', 'aren',
    'wasn', 'weren', 'hasn', 'haven', 'hadn', 'won', 'wouldn', 'couldn',
    'shouldn', 'actually', 'really', 'literally', 'basically', 'probably',
    'people', 'thing', 'things', 'time', 'year', 'years', 'day', 'days',
    'guy', 'guys', 'man', 'men', 'woman', 'women', 'lot', 'lots', 'bit',
    'https', 'http', 'www', 'com', 'reddit', 'edit', 'post', 'comment',
    'deleted', 'removed', 'null', 'none', 'just', 'going', 'watch', 'show',
    'never', 'always', 'every', 'anyone', 'everyone', 'someone', 'nothing',
    'something', 'anything', 'thought', 'feel', 'feels', 'felt', 'seems',
    'seem', 'mean', 'means', 'something', 'anything', 'pretty', 'kind',
}


def _tokenize(text):
    """Lowercase, remove URLs and punctuation, filter stopwords and short words."""
    text = str(text).lower()
    text = re.sub(r'https?://\S+', ' ', text)
    text = re.sub(r'[^a-z\s]', ' ', text)
    return [w for w in text.split() if w not in STOPWORDS and len(w) > 2]


def get_keyword_split(df, top_n=15):
    """Return top keywords from positive vs negative comments."""
    pos_words, neg_words = [], []

    for _, row in df.iterrows():
        tokens = _tokenize(row.get('text', ''))
        label = row.get('sentiment_label', '')
        if label == 'Positive':
            pos_words.extend(tokens)
        elif label == 'Negative':
            neg_words.extend(tokens)

    pos_freq = Counter(pos_words).most_common(top_n)
    neg_freq = Counter(neg_words).most_common(top_n)

    return {
        'positive': {
            'labels': [w for w, _ in pos_freq],
            'values': [c for _, c in pos_freq],
        },
        'negative': {
            'labels': [w for w, _ in neg_freq],
            'values': [c for _, c in neg_freq],
        },
    }


def get_post_title_debate(df, top_n=10):
    """Top N most-commented post titles with positive/negative/neutral breakdown."""
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

    return {
        'labels': labels,
        'datasets': {
            'Positive': [int(v) for v in grouped['Positive'].tolist()],
            'Negative': [int(v) for v in grouped['Negative'].tolist()],
            'Neutral':  [int(v) for v in grouped['Neutral'].tolist()],
        },
    }


def get_sentiment_momentum(df):
    """7-day rolling net sentiment per day, with an 'improving' or 'declining' direction."""
    if 'timestamp' not in df.columns:
        return None

    df = df.copy()
    df['date'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True).dt.date
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
    """
    0 = full consensus (one sentiment dominates completely)
    100 = perfectly divided
    """
    total = len(df)
    if total == 0:
        return 50.0
    pos = int((df['sentiment_label'] == 'Positive').sum())
    neg = int((df['sentiment_label'] == 'Negative').sum())
    neu = total - pos - neg
    dominant = max(pos, neg, neu)
    return round((1 - dominant / total) * 100, 1)


def get_key_takeaways(df):
    """Plain-language summary stats for the takeaways panel."""
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

    kw = get_keyword_split(df, top_n=3)
    top_praised    = kw['positive']['labels'][0] if kw['positive']['labels'] else None
    top_criticised = kw['negative']['labels'][0] if kw['negative']['labels'] else None

    return {
        'total':          total,
        'pos_pct':        round(pos / total * 100, 1),
        'neg_pct':        round(neg / total * 100, 1),
        'neu_pct':        round(neu / total * 100, 1),
        'most_debated':   most_debated,
        'top_praised':    top_praised,
        'top_criticised': top_criticised,
        'division_score': get_division_score(df),
    }


def compute_all_insights(df):
    """Single entry point — returns the full insights dict for the template."""
    try:
        return {
            'keyword_split':      get_keyword_split(df),
            'post_title_debate':  get_post_title_debate(df),
            'sentiment_momentum': get_sentiment_momentum(df),
            'takeaways':          get_key_takeaways(df),
        }
    except Exception as e:
        print(f"[insights] Error computing insights: {e}")
        return {}
