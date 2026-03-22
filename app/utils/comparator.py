import json
import pandas as pd
from app.utils.insights import get_keyword_split, get_emotion_distribution, _expand_emotion_scores

EMOTION_LABELS = ["positive", "negative", "neutral"]


def _sentiment_split_pct(df):
    total = len(df)
    if total == 0:
        return [0.0, 0.0]
    pos = int((df['sentiment_label'] == 'Positive').sum())
    neg = int((df['sentiment_label'] == 'Negative').sum())
    return [round(pos / total * 100, 1), round(neg / total * 100, 1)]


def _avg_upvotes_by_sentiment(df):
    out = []
    for s in ['Positive', 'Negative']:
        mask = df['sentiment_label'] == s
        if mask.any():
            avg = df.loc[mask, 'score'].mean()
            out.append(round(float(avg), 1) if not pd.isna(avg) else 0.0)
        else:
            out.append(0.0)
    return out


def _sentiment_momentum(df, window=7):
    """7-day rolling average of daily positive %."""
    dates, vals = _daily_positive_pct(df)
    if not dates:
        return [], []
    s = pd.Series(vals, index=pd.to_datetime(dates))
    rolled = s.rolling(window, min_periods=1).mean().round(1)
    return [str(d.date()) for d in rolled.index], [float(v) for v in rolled.tolist()]


def _cumulative_volume(df):
    """Cumulative comment count by date."""
    col = _date_col(df)
    if col is None:
        return [], []
    df = df.copy()
    df['date'] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.date
    df = df.dropna(subset=['date'])
    if df.empty:
        return [], []
    daily = df.groupby('date').size().sort_index()
    cumsum = daily.cumsum()
    return [str(d) for d in cumsum.index.tolist()], [int(v) for v in cumsum.tolist()]


def _text_length_by_sentiment(df):
    """Average text length (chars) by sentiment label: [positive_avg, negative_avg]."""
    out = []
    for s in ['Positive', 'Negative']:
        if 'text' in df.columns and 'sentiment_label' in df.columns:
            mask = df['sentiment_label'] == s
            if mask.any():
                avg = df.loc[mask, 'text'].str.len().mean()
                out.append(round(float(avg), 1) if not pd.isna(avg) else 0.0)
            else:
                out.append(0.0)
        else:
            out.append(0.0)
    return out


def _date_col(df):
    """Return the best available date column name."""
    for col in ('published_at', 'timestamp', 'created_at'):
        if col in df.columns:
            return col
    return None


def _daily_positive_pct(df):
    """Returns (dates_list, values_list) — daily % of comments that are Positive."""
    col = _date_col(df)
    if col is None:
        return [], []
    df = df.copy()
    df['date'] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.date
    df = df.dropna(subset=['date'])
    if df.empty:
        return [], []
    daily = (
        df.groupby('date')['sentiment_label']
        .apply(lambda x: round((x == 'Positive').sum() / len(x) * 100, 1))
        .reset_index()
    )
    daily.columns = ['date', 'pct']
    daily = daily.sort_values('date')
    return [str(d) for d in daily['date'].tolist()], [float(v) for v in daily['pct'].tolist()]


def _daily_volatility(df):
    """Returns (dates_list, values_list) — daily std dev of sentiment direction."""
    col = _date_col(df)
    if col is None:
        return [], []
    df = df.copy()
    df['date'] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.date
    df = df.dropna(subset=['date'])
    if df.empty:
        return [], []
    df['dir'] = df['sentiment_label'].map({'Positive': 1, 'Negative': -1, 'Neutral': 0}).fillna(0)
    vol = df.groupby('date')['dir'].std().fillna(0).reset_index()
    vol.columns = ['date', 'vol']
    vol = vol.sort_values('date')
    return [str(d) for d in vol['date'].tolist()], [round(float(v), 3) for v in vol['vol'].tolist()]


def _posting_hours(df):
    col = _date_col(df)
    if col is None:
        return [0] * 24
    df = df.copy()
    df['hour'] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.hour
    counts = df.groupby('hour').size().reindex(range(24), fill_value=0)
    return [int(v) for v in counts.tolist()]


def _weekly_rhythm(df):
    col = _date_col(df)
    if col is None:
        return [0] * 7
    df = df.copy()
    df['dow'] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.dayofweek
    counts = df.groupby('dow').size().reindex(range(7), fill_value=0)
    return [int(v) for v in counts.tolist()]


def _align_timeseries(dates_a, vals_a, dates_b, vals_b):
    """Merge two date series onto a shared sorted union of dates, filling gaps with None."""
    all_dates = sorted(set(dates_a) | set(dates_b))
    map_a = dict(zip(dates_a, vals_a))
    map_b = dict(zip(dates_b, vals_b))
    return (
        all_dates,
        [map_a.get(d, None) for d in all_dates],
        [map_b.get(d, None) for d in all_dates],
    )


def build_comparison_data(df_a, info_a, df_b, info_b):
    """
    Build the full comparison chart data dict for two topics.

    df_a / df_b  : DataFrames from comments table
    info_a / info_b : dicts from topics rows (must have name, rating, sentiment, total_comments)
    """
    df_a = _expand_emotion_scores(df_a)
    df_b = _expand_emotion_scores(df_b)
    data = {
        'topic_a': {
            'name':           info_a.get('name', 'Topic A'),
            'rating':         float(info_a.get('rating') or 0),
            'sentiment':      info_a.get('sentiment') or 'Neutral',
            'total_comments': int(info_a.get('total_comments') or len(df_a)),
        },
        'topic_b': {
            'name':           info_b.get('name', 'Topic B'),
            'rating':         float(info_b.get('rating') or 0),
            'sentiment':      info_b.get('sentiment') or 'Neutral',
            'total_comments': int(info_b.get('total_comments') or len(df_b)),
        },
    }

    # 1. Sentiment split (%)
    data['chart_split'] = {
        'labels':  ['Positive', 'Negative'],
        'topic_a': _sentiment_split_pct(df_a),
        'topic_b': _sentiment_split_pct(df_b),
    }

    # 2. Average upvotes by sentiment
    data['chart_upvotes'] = {
        'labels':  ['Positive', 'Negative'],
        'topic_a': _avg_upvotes_by_sentiment(df_a),
        'topic_b': _avg_upvotes_by_sentiment(df_b),
    }

    # 3. Daily positive % timeline (aligned on shared date range)
    da, va = _daily_positive_pct(df_a)
    db, vb = _daily_positive_pct(df_b)
    labels, vals_a, vals_b = _align_timeseries(da, va, db, vb)
    data['chart_timeline'] = {'labels': labels, 'topic_a': vals_a, 'topic_b': vals_b}

    # 4. Volatility (aligned)
    da, va = _daily_volatility(df_a)
    db, vb = _daily_volatility(df_b)
    labels, vals_a, vals_b = _align_timeseries(da, va, db, vb)
    data['chart_volatility'] = {'labels': labels, 'topic_a': vals_a, 'topic_b': vals_b}

    # 5. Posting hours
    data['chart_hours'] = {
        'labels':  [f'{h:02d}:00' for h in range(24)],
        'topic_a': _posting_hours(df_a),
        'topic_b': _posting_hours(df_b),
    }

    # 6. Weekly rhythm
    data['chart_weekly'] = {
        'labels':  ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
        'topic_a': _weekly_rhythm(df_a),
        'topic_b': _weekly_rhythm(df_b),
    }

    # 7. Keywords (love / hate) for each topic separately
    data['keywords_a'] = get_keyword_split(df_a, top_n=10)
    data['keywords_b'] = get_keyword_split(df_b, top_n=10)

    # 8. Emotion distribution
    emo_a = get_emotion_distribution(df_a)
    emo_b = get_emotion_distribution(df_b)
    all_labels = EMOTION_LABELS
    map_a = dict(zip(emo_a['labels'], emo_a['values'])) if emo_a else {}
    map_b = dict(zip(emo_b['labels'], emo_b['values'])) if emo_b else {}
    data['chart_emotions'] = {
        'labels':  [e.capitalize() for e in all_labels],
        'topic_a': [round(map_a.get(e, 0.0), 4) for e in all_labels],
        'topic_b': [round(map_b.get(e, 0.0), 4) for e in all_labels],
    }

    # 9. Sentiment momentum (7-day rolling avg)
    da, va = _sentiment_momentum(df_a)
    db, vb = _sentiment_momentum(df_b)
    labels, vals_a, vals_b = _align_timeseries(da, va, db, vb)
    data['chart_momentum'] = {'labels': labels, 'topic_a': vals_a, 'topic_b': vals_b}

    # 10. Cumulative volume
    da, va = _cumulative_volume(df_a)
    db, vb = _cumulative_volume(df_b)
    labels, vals_a, vals_b = _align_timeseries(da, va, db, vb)
    data['chart_cumulative'] = {'labels': labels, 'topic_a': vals_a, 'topic_b': vals_b}

    # 11. Text length by sentiment
    data['chart_text_length'] = {
        'labels':  ['Positive', 'Negative'],
        'topic_a': _text_length_by_sentiment(df_a),
        'topic_b': _text_length_by_sentiment(df_b),
    }

    return data
