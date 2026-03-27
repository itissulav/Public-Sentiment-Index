# Re-export shim — module has moved to app/api/huggingface.py
from app.api.huggingface import (  # noqa: F401
    process_and_store_comments, HuggingFaceError,
    _get_or_create_topic_source, _bulk_upsert_posts,
)
