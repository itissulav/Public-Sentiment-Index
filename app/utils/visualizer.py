import pandas as pd
import json
import os
from datetime import datetime

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
            
        # Map Supabase DB columns if they exist
        if 'created_at' in self.df.columns and 'timestamp' not in self.df.columns:
            self.df['timestamp'] = self.df['created_at']
        if 'sentiment_label' in self.df.columns and 'sentiment' not in self.df.columns:
            self.df['sentiment'] = self.df['sentiment_label']
            
        # Derive probability scores for legacy charts if confidence_score exists
        if 'confidence_score' in self.df.columns:
            self.df['score_positive'] = self.df.apply(lambda x: x['confidence_score'] if x.get('sentiment') == 'Positive' else 0, axis=1)
            self.df['score_negative'] = self.df.apply(lambda x: x['confidence_score'] if x.get('sentiment') == 'Negative' else 0, axis=1)
            self.df['score_neutral'] = self.df.apply(lambda x: x['confidence_score'] if x.get('sentiment') == 'Neutral' else 0, axis=1)

        # Parse timestamps
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'], errors='coerce')
        
        # Add derived columns
        self.df['date'] = self.df['timestamp'].dt.date
        self.df['hour'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.day_name()
        self.df['text_length'] = self.df.get('text', pd.Series(dtype=str)).fillna('').apply(len)

    def get_all_charts_data(self):
        """Returns a compiled dictionary of all 15 visualization datasets."""
        if self.df is None or self.df.empty:
            return {}

        return {
            "chart1_overall_sentiment": self._get_overall_sentiment(),
            "chart2_sentiment_timeline": self._get_sentiment_timeline(),
            "chart3_avg_upvotes_sentiment": self._get_avg_upvotes_by_sentiment(),
            "chart4_sentiment_vs_engagement": self._get_sentiment_vs_engagement(),
            "chart5_top_10_posts": self._get_top_10_posts(),
            "chart6_sentiment_volatility": self._get_sentiment_volatility(),
            "chart7_negative_intensity": self._get_intensity_distribution('score_negative'),
            "chart8_positive_intensity": self._get_intensity_distribution('score_positive'),
            "chart9_neutral_volume": self._get_neutral_volume(),
            "chart10_volume_by_hour": self._get_volume_by_hour(),
            "chart11_avg_probabilities": self._get_avg_probabilities(),
            "chart12_text_length_vs_sentiment": self._get_text_length_vs_sentiment(),
            "chart13_score_distribution": self._get_score_distribution(),
            "chart14_cumulative_posts": self._get_cumulative_posts(),
            "chart15_sentiment_by_day": self._get_sentiment_by_day()
        }

    def _get_overall_sentiment(self):
        counts = self.df['sentiment'].value_counts()
        return {
            "labels": counts.index.tolist(),
            "values": counts.values.tolist()
        }

    def _get_sentiment_timeline(self):
        daily_sentiment = self.df.groupby(['date', 'sentiment']).size().unstack(fill_value=0)
        return {
            "labels": [str(d) for d in daily_sentiment.index],
            "datasets": {col: daily_sentiment[col].tolist() for col in daily_sentiment.columns}
        }

    def _get_avg_upvotes_by_sentiment(self):
        avg_score = self.df.groupby('sentiment')['score'].mean().round(2)
        return {
            "labels": avg_score.index.tolist(),
            "values": avg_score.values.tolist()
        }

    def _get_sentiment_vs_engagement(self):
        # Sample data to avoid massive payloads (max 500 points)
        sample = self.df.sample(n=min(500, len(self.df)), random_state=42)
        return {
            "data": [{"x": row['score_positive'], "y": row['score']} for _, row in sample.iterrows()]
        }

    def _get_top_10_posts(self):
        top_10 = self.df.nlargest(10, 'score')
        return {
            "labels": [f"Post {row['post_id']} ({row['sentiment']})" for _, row in top_10.iterrows()],
            "values": top_10['score'].tolist()
        }

    def _get_sentiment_volatility(self):
        # Standard deviation of positive score per day
        volatility = self.df.groupby('date')['score_positive'].std().fillna(0).round(4)
        return {
            "labels": [str(d) for d in volatility.index],
            "values": volatility.values.tolist()
        }

    def _get_intensity_distribution(self, col):
        # Create bins 0.0-0.1, 0.1-0.2, etc.
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
        return {
            "labels": ["Negative", "Neutral", "Positive"],
            "values": [
                round(self.df['score_negative'].mean(), 4),
                round(self.df['score_neutral'].mean(), 4),
                round(self.df['score_positive'].mean(), 4)
            ]
        }

    def _get_text_length_vs_sentiment(self):
        sample = self.df.sample(n=min(300, len(self.df)), random_state=42)
        return {
            "data": [{"x": row['text_length'], "y": row['score_positive']} for _, row in sample.iterrows()]
        }

    def _get_score_distribution(self):
        # Engagement ranges
        bins = pd.cut(self.df['score'], bins=[-1, 10, 50, 100, 500, 1000, 5000, max(5001, self.df['score'].max())])
        counts = bins.value_counts().sort_index()
        # Clean labels manually since Interval looks messy
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
        grouped = self.df.groupby(['day_of_week', 'sentiment']).size().unstack(fill_value=0)
        grouped = grouped.reindex(day_order, fill_value=0)
        return {
            "labels": grouped.index.tolist(),
            "datasets": {col: grouped[col].tolist() for col in grouped.columns}
        }
