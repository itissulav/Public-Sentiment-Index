import time
import threading


class TopicDataCache:
    """In-process TTL cache for paginated Supabase comment rows."""

    def __init__(self, ttl_seconds=300):
        self._ttl = ttl_seconds
        self._store = {}  # topic_id (int) -> {"data": list[dict], "ts": float}
        self._lock = threading.Lock()

    def get(self, topic_id):
        with self._lock:
            entry = self._store.get(int(topic_id))
            if entry and (time.time() - entry["ts"]) < self._ttl:
                return entry["data"]
            return None

    def set(self, topic_id, data):
        with self._lock:
            self._store[int(topic_id)] = {"data": data, "ts": time.time()}

    def invalidate(self, topic_id=None):
        with self._lock:
            if topic_id is None:
                self._store.clear()
            else:
                self._store.pop(int(topic_id), None)


# Module-level singleton — shared across all Flask request threads
topic_cache = TopicDataCache(ttl_seconds=300)
