"""
app/services/topic_service.py
==============================
All Supabase DB operations for topics, snapshots, and user profiles.

Consolidates the 6+ repeated topic/snapshot query blocks that were
copy-pasted throughout main_controller.py.
"""

import time
from app.services.supabase_client import admin_supabase


# ── Cached predefined-topics lookup ──────────────────────────────────────────
# Queried on every Home/Trends/Compare render — keep it off the DB hot path.
# Invalidated by admin add/delete (see admin_controller).

_PREDEFINED_CACHE: dict = {"ts": 0.0, "rows": []}
_PREDEFINED_TTL_S = 60.0


def list_predefined_topics() -> list[dict]:
    """Return [{'id', 'name', 'category'}] for all predefined topics (cached 60 s)."""
    now = time.time()
    if _PREDEFINED_CACHE["rows"] and (now - _PREDEFINED_CACHE["ts"]) < _PREDEFINED_TTL_S:
        return _PREDEFINED_CACHE["rows"]
    try:
        res = admin_supabase.table("topics").select("id, name, category") \
            .is_("user_id", "null").order("name").execute()
        _PREDEFINED_CACHE["rows"] = res.data or []
        _PREDEFINED_CACHE["ts"]   = now
    except Exception as e:
        print(f"[topic_service] list_predefined_topics error: {e}")
        # Serve stale data if we have any — better than crashing the page
        return _PREDEFINED_CACHE["rows"]
    return _PREDEFINED_CACHE["rows"]


def invalidate_predefined_cache() -> None:
    _PREDEFINED_CACHE["ts"] = 0.0


def get_predefined_names_set() -> set[str]:
    """Fast O(1) membership test — replaces the old PREDEFINED_TOPICS_SET constant."""
    return {r["name"] for r in list_predefined_topics()}


def get_predefined_topic_id(topic_name: str) -> int | None:
    """Return the id of a predefined (user_id IS NULL) topic, or None."""
    try:
        res = admin_supabase.table("topics").select("id") \
            .eq("name", topic_name).is_("user_id", "null").execute()
        return res.data[0]["id"] if res.data else None
    except Exception:
        return None


def get_user_topic_id(topic_name: str, user_id: str) -> int | None:
    """Return the id of a user's custom topic, or None."""
    try:
        res = admin_supabase.table("topics").select("id") \
            .eq("name", topic_name).eq("user_id", user_id).execute()
        return res.data[0]["id"] if res.data else None
    except Exception:
        return None


def get_topic_info(topic_id: int) -> dict:
    """
    Fetch the latest snapshot for a topic and return an enriched info dict.
    Keys: name (caller must add), rating, sentiment, dominant_emotion, total_comments.
    """
    try:
        snap = admin_supabase.table("daily_snapshots") \
            .select("psi_rating, dominant_emotion, total_comments") \
            .eq("topic_id", topic_id).order("snapshot_date", desc=True).limit(1).execute()
        if snap.data:
            info = snap.data[0]
            rating = info.get("psi_rating", 0) or 0
            return {
                "rating":          rating,
                "sentiment":       "Positive" if rating > 0 else ("Negative" if rating < 0 else "Neutral"),
                "dominant_emotion": info.get("dominant_emotion", "neutral"),
                "total_comments":  info.get("total_comments", 0),
            }
    except Exception:
        pass
    return {"rating": 0, "sentiment": "Neutral", "dominant_emotion": "neutral", "total_comments": 0}


def get_trending_topics(limit: int = 5) -> list:
    """
    Return predefined topics enriched with snapshot data, sorted by PSI rating desc.
    Each row also gets an `image_filename` resolved via the slug/placeholder convention.
    """
    from app.utils.topic_image import get_topic_image_filename
    topics = []
    try:
        res = admin_supabase.table("topics").select("id, name, category").is_("user_id", "null").execute()
        for t in (res.data or []):
            info = get_topic_info(t["id"])
            t.update(info)
            t["image_filename"] = get_topic_image_filename(t["name"])
        topics = sorted(res.data or [], key=lambda x: x.get("rating", 0), reverse=True)[:limit]
    except Exception:
        pass
    return topics


def get_saved_scan_count(user_id: str) -> int:
    """Return count of topics saved by a user."""
    try:
        res = admin_supabase.table("topics").select("id", count="exact") \
            .eq("user_id", user_id).execute()
        return res.count or 0
    except Exception:
        return 0


def get_saved_scans(user_id: str) -> list:
    """
    Return user's saved topics enriched with latest snapshot data, newest first.
    """
    try:
        res = admin_supabase.table("topics") \
            .select("id, name, category, created_at") \
            .eq("user_id", user_id) \
            .order("created_at", desc=True).execute()
        for t in (res.data or []):
            snap = admin_supabase.table("daily_snapshots") \
                .select("psi_rating, dominant_emotion, total_comments, snapshot_date") \
                .eq("topic_id", t["id"]).order("snapshot_date", desc=True).limit(1).execute()
            if snap.data:
                s = snap.data[0]
                t["rating"]           = s.get("psi_rating", 0)
                t["total_comments"]   = s.get("total_comments", 0)
                t["dominant_emotion"] = s.get("dominant_emotion", "neutral")
                t["last_updated"]     = s.get("snapshot_date", t.get("created_at", ""))
            else:
                t["rating"]           = 0
                t["total_comments"]   = 0
                t["dominant_emotion"] = "neutral"
                t["last_updated"]     = t.get("created_at", "")
            r = t["rating"] or 0
            t["sentiment"] = "Positive" if r > 0 else ("Negative" if r < 0 else "Neutral")
        return res.data or []
    except Exception:
        return []


def enrich_predefined_topics(topics: list) -> list:
    """
    Attach latest snapshot data (rating, sentiment, total_comments) to each
    predefined topic dict in-place. Used by the admin dashboard.
    """
    for t in topics:
        try:
            snap = admin_supabase.table("daily_snapshots") \
                .select("psi_rating, dominant_emotion, total_comments, snapshot_date") \
                .eq("topic_id", t["id"]).order("snapshot_date", desc=True).limit(1).execute()
            if snap.data:
                t["rating"]         = snap.data[0].get("psi_rating", 0)
                t["sentiment"]      = "Positive" if (t.get("rating") or 0) > 0 else ("Negative" if (t.get("rating") or 0) < 0 else "Neutral")
                t["total_comments"] = snap.data[0].get("total_comments", 0)
        except Exception:
            pass
    return topics


def delete_predefined_topic(topic_id: int):
    """Delete a predefined topic (user_id IS NULL guard). Returns True on success."""
    try:
        admin_supabase.table("topics") \
            .delete() \
            .eq("id", topic_id) \
            .is_("user_id", "null") \
            .execute()
        invalidate_predefined_cache()
        return True
    except Exception as e:
        print(f"[topic_service] delete_predefined_topic error: {e}")
        return False


def update_user_profile(user_id: str, fields: dict):
    """Update user profile fields in the Users table."""
    admin_supabase.table("Users").update(fields).eq("UID", user_id).execute()


def get_user_topic_by_id(topic_id: int, user_id: str) -> dict | None:
    """Return {id, name} if the user owns this topic, else None."""
    try:
        res = admin_supabase.table("topics").select("id,name") \
            .eq("id", topic_id).eq("user_id", user_id).execute()
        return res.data[0] if res.data else None
    except Exception:
        return None


def get_user_topic_suggestions(user_id: str) -> list:
    """Return the user's saved topic names, newest first."""
    try:
        res = admin_supabase.table("topics").select("name") \
            .eq("user_id", user_id).order("created_at", desc=True).execute()
        return [r["name"] for r in (res.data or [])]
    except Exception:
        return []


def get_cross_user_topic(topic_name: str, days_back: int = 7) -> int | None:
    """
    Find the most recent topic with the same name from any user (case-insensitive).
    Returns topic_id or None. Used for cross-user cache reuse.
    """
    try:
        from datetime import datetime, timedelta, timezone
        week_ago = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
        res = admin_supabase.table("topics").select("id") \
            .ilike("name", topic_name).gte("created_at", week_ago).limit(1).execute()
        return res.data[0]["id"] if res.data else None
    except Exception:
        return None


def get_pulse_data(days: int = 90) -> tuple:
    """
    Return (topics_list, snapshots_list) for all predefined topics
    with their daily_snapshots over the last N days.
    """
    from datetime import date, timedelta
    try:
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        topics_res = admin_supabase.table("topics") \
            .select("id,name,category").is_("user_id", "null").execute()
        topics = topics_res.data or []
        if not topics:
            return [], []
        topic_ids = [t["id"] for t in topics]
        snaps_res = admin_supabase.table("daily_snapshots") \
            .select("topic_id,snapshot_date,psi_rating,dominant_emotion,total_comments") \
            .in_("topic_id", topic_ids) \
            .gte("snapshot_date", cutoff) \
            .order("snapshot_date") \
            .execute()
        return topics, snaps_res.data or []
    except Exception as e:
        print(f"[topic_service] get_pulse_data error: {e}")
        return [], []


def get_topic_psi_history(topic_id: int, days: int = 90) -> list:
    """
    Return a list of daily PSI snapshots for a topic over the last N days.
    Returned as [{"snapshot_date": "...", "psi_rating": ...}, ...]
    """
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    try:
        res = admin_supabase.table("daily_snapshots") \
            .select("snapshot_date, psi_rating") \
            .eq("topic_id", topic_id) \
            .gte("snapshot_date", cutoff) \
            .order("snapshot_date") \
            .execute()
        return res.data or []
    except Exception as e:
        print(f"[topic_service] get_topic_psi_history error: {e}")
        return []


def get_latest_snapshot_date(topic_id: int) -> str | None:
    """Return the most recent snapshot_date (YYYY-MM-DD) for a topic, or None."""
    try:
        res = admin_supabase.table("daily_snapshots") \
            .select("snapshot_date") \
            .eq("topic_id", topic_id) \
            .order("snapshot_date", desc=True) \
            .limit(1).execute()
        return res.data[0]["snapshot_date"] if res.data else None
    except Exception as e:
        print(f"[topic_service] get_latest_snapshot_date error: {e}")
        return None
