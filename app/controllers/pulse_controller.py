"""
app/controllers/pulse_controller.py
=====================================
Routes: GET /pulse
"""

import json
from flask import Blueprint, render_template, session
from app.models.users import User
from app.services import topic_service

pulse_bp = Blueprint("pulse", __name__)

_SHIFT_THRESHOLD = 10


@pulse_bp.route("/pulse")
def pulse():
    """Pulse dashboard — multi-topic PSI timeline over the last 90 days."""
    from flask import redirect, url_for, flash

    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    # Pulse dashboard requires login - redirect guests to login
    if not user:
        flash("You must be logged in to view the Pulse dashboard.")
        return redirect(url_for("auth.login"))

    topics, snaps = topic_service.get_pulse_data(days=90)

    pulse_data = {}
    movers     = []
    shifts     = []

    if topics and snaps:
        id_to_name = {t["id"]: t["name"] for t in topics}

        by_topic: dict = {}
        for s in snaps:
            by_topic.setdefault(s["topic_id"], []).append(s)

        for tid, rows in by_topic.items():
            name    = id_to_name.get(tid, str(tid))
            dates   = [r["snapshot_date"] for r in rows]
            psi     = [round(r["psi_rating"] or 0, 1) for r in rows]
            emotions = [r.get("dominant_emotion") or "neutral" for r in rows]
            pulse_data[name] = {"dates": dates, "psi": psi, "emotions": emotions}

            if len(psi) >= 2:
                latest       = psi[-1]
                week_ago_idx = max(0, len(psi) - 8)
                delta        = round(latest - psi[week_ago_idx], 1)
                movers.append({
                    "name":       name,
                    "latest_psi": latest,
                    "delta":      delta,
                    "direction":  "up" if delta > 0 else ("down" if delta < 0 else "flat"),
                })

                day_delta = psi[-1] - psi[-2]
                if abs(day_delta) >= _SHIFT_THRESHOLD:
                    shifts.append({
                        "name":       name,
                        "delta":      round(day_delta, 1),
                        "direction":  "up" if day_delta > 0 else "down",
                        "date":       dates[-1],
                        "latest_psi": round(psi[-1], 1),
                    })

        movers.sort(key=lambda m: abs(m["delta"]), reverse=True)
        movers  = movers[:3]
        shifts.sort(key=lambda x: abs(x["delta"]), reverse=True)

    return render_template(
        "pulse.html",
        user=user,
        pulse_data=json.dumps(pulse_data),
        movers=movers,
        shifts=shifts,
        topics=topics,
    )
