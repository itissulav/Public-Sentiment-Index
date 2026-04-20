"""
app/services/notification_service.py
=====================================
DB queries for the notifications page and API endpoint.

Shared by both the rendered page (/notifications) and the JSON endpoint
(/api/notifications) so the logic isn't duplicated.
"""

from datetime import date, timedelta
from app.services.supabase_client import admin_supabase

SHIFT_THRESHOLD = 10   # PSI points — shifts below this are ignored


def get_psi_shifts(threshold: int = SHIFT_THRESHOLD, days: int = 30) -> list:
    """
    Return a list of predefined topics that had a significant PSI shift
    in the last `days` days.

    Each entry: {name, delta, direction, date, latest_psi}
    Sorted by |delta| descending.
    """
    shifts = []
    try:
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        topics_res = admin_supabase.table("topics").select("id,name").is_("user_id", "null").execute()
        topics_list = topics_res.data or []
        if not topics_list:
            return []

        topic_ids = [t["id"] for t in topics_list]
        snaps_res = admin_supabase.table("daily_snapshots") \
            .select("topic_id,snapshot_date,psi_rating") \
            .in_("topic_id", topic_ids) \
            .gte("snapshot_date", cutoff) \
            .order("snapshot_date") \
            .execute()

        id_to_name = {t["id"]: t["name"] for t in topics_list}
        by_topic: dict = {}
        for s in (snaps_res.data or []):
            by_topic.setdefault(s["topic_id"], []).append(s)

        for tid, rows in by_topic.items():
            psi = [round(r["psi_rating"] or 0, 1) for r in rows]
            if len(psi) < 2:
                continue
            day_delta = psi[-1] - psi[-2]
            if abs(day_delta) >= threshold:
                shifts.append({
                    "name":       id_to_name.get(tid, ""),
                    "delta":      round(day_delta, 1),
                    "direction":  "up" if day_delta > 0 else "down",
                    "date":       rows[-1]["snapshot_date"],
                    "latest_psi": round(psi[-1], 1),
                })

        shifts.sort(key=lambda x: abs(x["delta"]), reverse=True)
    except Exception as e:
        print(f"[notification_service] get_psi_shifts error: {e}")

    return shifts


def get_completed_scans(user_id: str) -> list:
    """
    Return a user's topics that have at least one daily_snapshot,
    enriched with completed_date and psi_rating. Newest first.
    """
    completed = []
    try:
        res = admin_supabase.table("topics") \
            .select("id,name,created_at") \
            .eq("user_id", user_id) \
            .order("created_at", desc=True) \
            .execute()
        for t in (res.data or []):
            snap = admin_supabase.table("daily_snapshots") \
                .select("snapshot_date,psi_rating") \
                .eq("topic_id", t["id"]) \
                .order("snapshot_date", desc=True).limit(1).execute()
            if snap.data:
                t["completed_date"] = snap.data[0]["snapshot_date"]
                t["psi_rating"]     = round(snap.data[0]["psi_rating"] or 0, 1)
                completed.append(t)
    except Exception as e:
        print(f"[notification_service] get_completed_scans error: {e}")

    return completed
