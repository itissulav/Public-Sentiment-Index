# Phase 2: High-Volume Tracking & Executive Dashboard

This implementation plan focuses on redesigning the architecture to support tens of thousands of data points at scale and overhauling the user interface into a layman-friendly, top-tier analytical dashboard.

## Proposed Changes

### 1. Database Schema Optimization (Relational & Indexed)
Relying on a single table with a text field for topics is inefficient at scale. We will normalize the database into two tables with formal foreign-keys and indexing to guarantee blindingly fast reads even with 100,000+ rows.

**Table 1: `search_topics`**
- [id](file:///s:/FYP/Public%20Sentiment%20Index/app/models/topic.py#21-23) (UUID, PK)
- [name](file:///s:/FYP/Public%20Sentiment%20Index/app/models/topic.py#24-26) (Text, Unique) - "Donald Trump", "Macbook Neo", etc.
- `last_updated` (Timestamp)
- `total_comments` (Integer)

**Table 2: [reddit_comments](file:///s:/FYP/Public%20Sentiment%20Index/app/utils/fetcher.py#24-103)**
- [id](file:///s:/FYP/Public%20Sentiment%20Index/app/models/topic.py#21-23) (UUID, PK)
- `topic_id` (UUID, FK -> `search_topics(id)`)
- `post_id` (Text)
- [text](file:///s:/FYP/Public%20Sentiment%20Index/app/utils/visualizer.py#153-158) (Text)
- `author` (Text)
- [score](file:///s:/FYP/Public%20Sentiment%20Index/app/utils/visualizer.py#159-169) (Integer)
- `sentiment_label` (Text)
- `confidence_score` (Float)
- `timestamp` (Timestamp)

*An index will be created on `topic_id` to make fetching instantaneous.*

### 2. High-Volume Extraction Logic
- [fetcher.py](file:///s:/FYP/Public%20Sentiment%20Index/app/utils/fetcher.py) limit_posts increased to `250` and `max_comments` threshold raised to `2000` minimum per topic.
- [hf_analyzer.py](file:///s:/FYP/Public%20Sentiment%20Index/app/utils/hf_analyzer.py) will automatically resolve `topic_id` lookups.

### 3. Layman-Friendly Executive Dashboard (UI Overhaul)
The UI will be restructured from a generic data dump to a curated Analyst Report.
- **Hero Topic Cards**: The 5 pre-defined topics will be massive, beautifully styled "Dashboard entry" cards.
- **Remove Defaults**: The page will initially be entirely empty of charts (No default loads).
- **Reddit Highlights component**: Build a custom HTML/CSS UI that perfectly mirrors Reddit's interface to showcase the #1 Most Engaged Positive Comment and the #1 Most Engaged Negative Comment.
- **Chart Layout & Explanations**: 
  - The hidden "i" tooltips are removed entirely.
  - Every single chart will exist inside an "Analysis Block" with a clear, layman-readable text column immediately next to or below the graph explicitly stating what the data proves (e.g., "The data reveals that Highly Negative posts receive 40% more engagement...").
  - We will display 10+ core visualization metrics.

## Verification Plan
1. Send SQL snippets to user to format their Database natively.
2. Re-run [seed_topics.py](file:///s:/FYP/Public%20Sentiment%20Index/seed_topics.py) to populate DB with 10,000 (5 x 2000) data points.
3. Validate UI rendering latency is < 1 second on the DOM.
