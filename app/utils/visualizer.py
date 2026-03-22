import numpy as np
import pandas as pd
import json
import os
from datetime import datetime

# Canonical sentiment keys — always in this order
SENTIMENT_KEYS = ["Positive", "Negative", "Neutral"]


class ElectionDataVisualizer:
    def __init__(self, data_source):
        self.df = None
        if isinstance(data_source, str):
            self.csv_path = data_source
            self._load_csv()
        elif isinstance(data_source, pd.DataFrame):
            self.df = data_source
            self._preprocess_data()

        if self.df is None or self.df.empty:
            print("Warning: Visualizer initialized with empty data.")

    def _load_csv(self):
        """Loads CSV data."""
        if os.path.exists(self.csv_path):
            self.df = pd.read_csv(self.csv_path)
            self._preprocess_data()
        else:
            print(f"File not found: {self.csv_path}")

    def _preprocess_data(self):
        """Preprocesses the DataFrame for visualization."""
        if self.df is None or self.df.empty:
            return

        # v2 schema uses 'published_at'; v1 used 'timestamp' / 'created_at'
        if 'published_at' in self.df.columns and 'timestamp' not in self.df.columns:
            self.df['timestamp'] = self.df['published_at']
        elif 'created_at' in self.df.columns and 'timestamp' not in self.df.columns:
            self.df['timestamp'] = self.df['created_at']

        if 'sentiment_label' in self.df.columns and 'sentiment' not in self.df.columns:
            self.df['sentiment'] = self.df['sentiment_label']

        # Vectorized score columns — ~5× faster than apply(lambda) for large DataFrames
        if 'confidence_score' in self.df.columns and 'sentiment' in self.df.columns:
            s = self.df['sentiment'].to_numpy()
            c = self.df['confidence_score'].fillna(0).to_numpy(dtype=float)
            self.df['score_positive'] = np.where(s == 'Positive', c, 0.0)
            self.df['score_negative'] = np.where(s == 'Negative', c, 0.0)
            self.df['score_neutral']  = np.where(s == 'Neutral',  c, 0.0)

        # Parse timestamps
        self.df['timestamp'] = pd.to_datetime(self.df.get('timestamp'), errors='coerce')

        # Add derived columns
        self.df['date']        = self.df['timestamp'].dt.date
        self.df['hour']        = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.day_name()
        self.df['text_length'] = self.df.get('text', pd.Series(dtype=str)).fillna('').apply(len)

        # Expand emotion_scores JSONB into individual columns if not already present
        if 'emotion_scores' in self.df.columns:
            import json as _json
            def _parse(x):
                if isinstance(x, dict): return x
                if isinstance(x, str):
                    try: return _json.loads(x)
                    except Exception: return {}
                return {}
            expanded = self.df['emotion_scores'].apply(_parse).apply(pd.Series)
            for col in expanded.columns:
                if col not in self.df.columns:
                    self.df[col] = expanded[col]

    def _normalize_datasets(self, datasets: dict, n_items: int) -> dict:
        """Ensure Positive/Negative/Neutral keys always exist, zero-filled if absent.

        This prevents JS from crashing when a topic has no comments of a given
        sentiment (e.g. zero Negative comments → unstack omits the key).
        """
        return {k: datasets.get(k, [0] * n_items) for k in SENTIMENT_KEYS}

    def get_all_charts_data(self, primary_only=False):
        """Returns a compiled dictionary of all visualization datasets.

        Each chart function is individually guarded — one failure does NOT
        cause all other charts to return empty.

        Parameters
        ----------
        primary_only : bool
            When True, compute only the 3 above-fold charts (donut, timeline,
            top posts). Deep-dive charts are deferred to the background AJAX
            endpoint for faster initial page load.
        """
        if self.df is None or self.df.empty:
            return {}

        def _s(fn, *args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                print(f"[visualizer] {fn.__name__} error: {e}")
                return None

        charts = {
            "chart1_overall_sentiment":  _s(self._get_overall_sentiment),
            "chart2_sentiment_timeline": _s(self._get_sentiment_timeline),
            "chart5_top_10_posts":       _s(self._get_top_10_posts),
        }
        if primary_only:
            return charts

        charts.update({
            "chart3_avg_upvotes_sentiment":     _s(self._get_avg_upvotes_by_sentiment),
            "chart4_sentiment_vs_engagement":   _s(self._get_sentiment_vs_engagement),
            "chart6_sentiment_volatility":      _s(self._get_sentiment_volatility),
            "chart7_negative_intensity":        _s(self._get_intensity_distribution, 'score_negative'),
            "chart8_positive_intensity":        _s(self._get_intensity_distribution, 'score_positive'),
            "chart10_volume_by_hour":           _s(self._get_volume_by_hour),
            "chart12_text_length_vs_sentiment": _s(self._get_text_length_vs_sentiment),
            "chart13_score_distribution":       _s(self._get_score_distribution),
            "chart14_cumulative_posts":         _s(self._get_cumulative_posts),
            "chart15_sentiment_by_day":         _s(self._get_sentiment_by_day),
            "chart17_community_breakdown":      _s(self._get_community_breakdown),
        })
        return charts

    def _get_overall_sentiment(self):
        col = 'emotion_label' if 'emotion_label' in self.df.columns else 'sentiment'
        counts = self.df[col].value_counts()
        return {
            "labels": counts.index.tolist(),
            "values": counts.values.tolist()
        }

    def _get_sentiment_timeline(self):
        col = 'emotion_label' if 'emotion_label' in self.df.columns else 'sentiment'
        daily_sentiment = self.df.groupby(['date', col]).size().unstack(fill_value=0)
        n = len(daily_sentiment)
        return {
            "labels":   [str(d) for d in daily_sentiment.index],
            "datasets": self._normalize_datasets(
                {c: daily_sentiment[c].tolist() for c in daily_sentiment.columns}, n
            ),
        }

    def _get_avg_upvotes_by_sentiment(self):
        group_col = 'emotion_label' if 'emotion_label' in self.df.columns else 'sentiment'
        avg_score = self.df.groupby(group_col)['score'].mean().round(2)
        raw = {k: round(float(v), 2) for k, v in avg_score.items()}
        # Return all 3 keys guaranteed
        return {
            "labels": SENTIMENT_KEYS,
            "values": [raw.get(k, 0.0) for k in SENTIMENT_KEYS],
        }

    def _get_sentiment_vs_engagement(self):
        # Sample data to avoid massive payloads (max 500 points)
        sample = self.df.sample(n=min(500, len(self.df)), random_state=42)
        return {
            "data": [{"x": float(r.score_positive), "y": int(r.score)}
                     for r in sample[['score_positive', 'score']].itertuples(index=False)]
        }

    def _get_top_10_posts(self):
        if 'post_title' in self.df.columns:
            # Deduplicate to one row per post (highest-score comment row for that post)
            agg_cols = {'score': 'first'}
            if 'sentiment' in self.df.columns:
                agg_cols['sentiment'] = lambda x: x.mode()[0] if not x.empty else 'Neutral'
            if 'source_id' in self.df.columns:
                agg_cols['source_id'] = 'first'
            grouped = self.df.sort_values('score', ascending=False) \
                              .groupby('post_title', sort=False).agg(agg_cols).reset_index()
            top_10 = grouped.nlargest(10, 'score')
            posts = []
            for row in top_10.itertuples(index=False):
                title = str(getattr(row, 'post_title', ''))
                posts.append({
                    'title':     (title[:90] + '…') if len(title) > 90 else title,
                    'score':     int(getattr(row, 'score', 0)),
                    'sentiment': str(getattr(row, 'sentiment', 'Neutral')),
                    'source_id': str(getattr(row, 'source_id', '')),
                })
            return {
                'labels': [p['title'][:45] + '…' if len(p['title']) > 45 else p['title'] for p in posts],
                'values': [p['score'] for p in posts],
                'posts':  posts,
            }
        else:
            top_10 = self.df.nlargest(10, 'score')
            return {
                "labels": [f"Post {r.get('post_id','?')} ({r.get('sentiment','?')})" for r in top_10.to_dict('records')],
                "values": top_10['score'].tolist(),
                "posts":  [],
            }

    def _get_sentiment_volatility(self):
        # Standard deviation of positive score per day
        volatility = self.df.groupby('date')['score_positive'].std().fillna(0).round(4)
        return {
            "labels": [str(d) for d in volatility.index],
            "values": volatility.values.tolist()
        }

    def _get_intensity_distribution(self, col):
        # Guard: if column is all zeros (e.g. YouTube), return empty bins instead of crashing
        if col not in self.df.columns or self.df[col].max() == 0:
            return {
                "labels": ["0.0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", "0.8-1.0"],
                "values": [0, 0, 0, 0, 0],
            }
        bins = pd.cut(self.df[col], bins=[0, 0.2, 0.4, 0.6, 0.8, 1.0])
        counts = bins.value_counts().sort_index()
        return {
            "labels": [str(b) for b in counts.index],
            "values": counts.values.tolist()
        }

    def _get_neutral_volume(self):
        neutral_only = self.df[self.df['sentiment'] == 'Neutral']
        daily_neutral = neutral_only.groupby('date').size()
        return {
            "labels": [str(d) for d in daily_neutral.index],
            "values": daily_neutral.values.tolist()
        }

    def _get_volume_by_hour(self):
        hourly = self.df.groupby('hour').size().reindex(range(24), fill_value=0)
        return {
            "labels": [f"{h:02d}:00" for h in hourly.index],
            "values": hourly.values.tolist()
        }

    def _get_avg_probabilities(self):
        emotion_cols = ["positive", "negative", "neutral"]
        present = [e for e in emotion_cols if e in self.df.columns]
        if not present:
            return None
        return {
            "labels": [e.capitalize() for e in present],
            "values": [round(float(self.df[e].mean()), 4) for e in present]
        }

    def _get_text_length_vs_sentiment(self):
        sample = self.df.sample(n=min(300, len(self.df)), random_state=42)
        return {
            "data": [{"x": int(r.text_length), "y": float(r.score_positive)}
                     for r in sample[['text_length', 'score_positive']].itertuples(index=False)]
        }

    def _get_score_distribution(self):
        # Engagement ranges — guard against YouTube where all scores are 0
        max_score = int(self.df['score'].max())
        upper = max(max_score + 1, 5001)
        bins = pd.cut(self.df['score'], bins=[-1, 10, 50, 100, 500, 1000, 5000, upper])
        counts = bins.value_counts().sort_index()
        labels = ["0-10", "11-50", "51-100", "101-500", "501-1k", "1k-5k", "5k+"]
        return {
            "labels": labels[:len(counts)],
            "values": counts.values.tolist()
        }

    def _get_cumulative_posts(self):
        daily = self.df.groupby('date').size()
        cumulative = daily.cumsum()
        return {
            "labels": [str(d) for d in cumulative.index],
            "values": cumulative.values.tolist()
        }

    def _get_sentiment_by_day(self):
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        col = 'emotion_label' if 'emotion_label' in self.df.columns else 'sentiment'
        grouped = self.df.groupby(['day_of_week', col]).size().unstack(fill_value=0)
        grouped = grouped.reindex(day_order, fill_value=0)
        return {
            "labels":   grouped.index.tolist(),
            "datasets": self._normalize_datasets(
                {c: grouped[c].tolist() for c in grouped.columns}, len(grouped)
            ),
        }

    def _get_community_breakdown(self, label_source: str = "reddit"):
        """Per-source sentiment % breakdown.

        Parameters
        ----------
        label_source : 'reddit' → prefix labels with r/
                       'youtube' → use post_title as label (truncated to 45 chars)
        """
        if 'source_id' not in self.df.columns:
            return None
        sources = self.df['source_id'].dropna().unique()
        if len(sources) < 1:
            return None
        col = 'emotion_label' if 'emotion_label' in self.df.columns else 'sentiment'
        grouped = self.df.groupby(['source_id', col]).size().unstack(fill_value=0)
        totals = grouped.sum(axis=1)
        pct = (grouped.div(totals, axis=0) * 100).round(1)
        pct = pct.loc[totals.sort_values(ascending=False).index]

        raw_source_ids = [str(s) for s in pct.index.tolist()]
        n = len(pct)

        if label_source == "youtube" and 'post_title' in self.df.columns:
            title_map = self.df.groupby("source_id")["post_title"].first().to_dict()
            labels = []
            for s in pct.index.tolist():
                t = str(title_map.get(s, '') or s)  # fallback to source_id if title None
                labels.append((t[:45] + "…") if len(t) > 45 else t)
        else:
            labels = [f"r/{s}" for s in pct.index.tolist()]

        return {
            "labels":    labels,
            "source_ids": raw_source_ids,
            "datasets":  self._normalize_datasets(
                {c: pct[c].tolist() for c in pct.columns}, n
            ),
            "totals":    totals[pct.index].tolist(),
        }

    def _get_emotion_distribution(self):
        """Average sentiment scores across all comments (radar / bar chart)."""
        EMOTION_COLS = ["positive", "negative", "neutral"]
        EMOTION_COLORS = {
            "positive": "#10b981",
            "negative": "#ef4444",
            "neutral":  "#6b7280",
        }
        present = [e for e in EMOTION_COLS if e in self.df.columns]
        if not present:
            # Fall back to counting emotion_label
            if 'emotion_label' not in self.df.columns:
                return None
            counts = self.df['emotion_label'].value_counts(normalize=True)
            present = [e for e in EMOTION_COLS if e in counts.index]
            if not present:
                return None
            values = [round(float(counts.get(e, 0)) * 100, 2) for e in present]
        else:
            values = [round(float(self.df[e].mean()) * 100, 2) for e in present]

        return {
            "labels": [e.capitalize() for e in present],
            "values": values,
            "colors": [EMOTION_COLORS.get(e, "#6b7280") for e in present],
        }
