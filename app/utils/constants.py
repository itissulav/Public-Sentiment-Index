"""
app/utils/constants.py
=======================
Shared constants used across controllers and services.
"""

PREDEFINED_TOPICS = [
    "Donald Trump",
    "Elon Musk",
    "Gaza Conflict",
    "AI Technology",
    "Climate Change",
]

# Set version for O(1) membership tests
PREDEFINED_TOPICS_SET = set(PREDEFINED_TOPICS)

# Image filenames for predefined topic cards
PREDEFINED_TOPIC_IMAGES = {
    "Donald Trump":  "donaldtrump.png",
    "Elon Musk":     "elonmusk.png",
    "Gaza Conflict": "gazaconflict.png",
    "AI Technology": "aitechnology.png",
    "Climate Change": "climatechange.png",
}

# PostgREST nested-select string for comments with post/source context
COMMENT_COLS = (
    "id,topic_id,text,author,score,published_at,"
    "sentiment_label,emotion_label,emotion_scores,confidence_score,"
    "posts(external_post_id,title,topic_sources(source_type,source_id))"
)
