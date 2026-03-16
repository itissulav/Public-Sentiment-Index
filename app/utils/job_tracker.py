# app/utils/job_tracker.py
# Thread-safe in-memory store for background analysis job states.
# Key: (user_id_str, topic_name_lower)  Value: 'processing' | 'complete' | 'failed'

import threading

_lock = threading.Lock()
_jobs: dict[tuple, str] = {}


def _key(user_id, topic_name: str) -> tuple:
    return (str(user_id), topic_name.strip().lower())


def mark_processing(user_id, topic_name: str):
    with _lock:
        _jobs[_key(user_id, topic_name)] = "processing"


def mark_complete(user_id, topic_name: str):
    with _lock:
        _jobs[_key(user_id, topic_name)] = "complete"


def mark_failed(user_id, topic_name: str):
    with _lock:
        _jobs[_key(user_id, topic_name)] = "failed"


def get_status(user_id, topic_name: str) -> str | None:
    """Returns 'processing', 'complete', 'failed', or None (not tracked)."""
    with _lock:
        return _jobs.get(_key(user_id, topic_name))


def clear(user_id, topic_name: str):
    with _lock:
        _jobs.pop(_key(user_id, topic_name), None)
