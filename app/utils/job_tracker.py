# app/utils/job_tracker.py
# Thread-safe in-memory store for background analysis job states.
# Key: (user_id_str, topic_name_lower)  Value: 'processing' | 'complete' | 'failed'

import threading

_lock = threading.Lock()
_jobs: dict[tuple, str] = {}
_steps: dict[tuple, str] = {}


def _key(user_id, topic_name: str) -> tuple:
    return (str(user_id), topic_name.strip().lower())


def mark_processing(user_id, topic_name: str):
    with _lock:
        _jobs[_key(user_id, topic_name)] = "processing"


def mark_complete(user_id, topic_name: str):
    with _lock:
        k = _key(user_id, topic_name)
        _jobs[k] = "complete"
        _steps.pop(k, None)


def mark_failed(user_id, topic_name: str):
    with _lock:
        k = _key(user_id, topic_name)
        _jobs[k] = "failed"
        _steps.pop(k, None)


def get_status(user_id, topic_name: str) -> str | None:
    """Returns 'processing', 'complete', 'failed', or None (not tracked)."""
    with _lock:
        return _jobs.get(_key(user_id, topic_name))


def set_step(user_id, topic_name: str, step: str):
    """Set the current granular step: 'fetching' | 'classifying' | 'storing'."""
    with _lock:
        _steps[_key(user_id, topic_name)] = step


def get_step(user_id, topic_name: str) -> str | None:
    with _lock:
        return _steps.get(_key(user_id, topic_name))


def clear(user_id, topic_name: str):
    with _lock:
        k = _key(user_id, topic_name)
        _jobs.pop(k, None)
        _steps.pop(k, None)
