"""
app/controllers/notifications_controller.py
=============================================
Routes: GET /notifications, GET /api/notifications
"""

from flask import Blueprint, render_template, session, jsonify
from app.models.users import User
from app.services import notification_service

notifications_bp = Blueprint("notifications", __name__)


@notifications_bp.route("/notifications")
def notifications():
    from flask import redirect, url_for, flash

    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    # Notifications require login - redirect guests to login
    if not user:
        flash("You must be logged in to view notifications.")
        return redirect(url_for("auth.login"))

    shifts = notification_service.get_psi_shifts()
    completed_scans = []
    if user:
        completed_scans = notification_service.get_completed_scans(user.get_user_id())

    return render_template(
        "notifications.html",
        user=user,
        shifts=shifts,
        completed_scans=completed_scans,
    )


@notifications_bp.route("/api/notifications")
def api_notifications():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    shifts = notification_service.get_psi_shifts()
    completed_scans = []
    if user:
        raw = notification_service.get_completed_scans(user.get_user_id())
        completed_scans = [
            {
                "name":           t["name"],
                "completed_date": t["completed_date"],
                "psi_rating":     t["psi_rating"],
            }
            for t in raw
        ]

    return jsonify({"shifts": shifts, "completed_scans": completed_scans})
