"""
app/controllers/history_controller.py
=======================================
Routes: GET /history
"""

from flask import Blueprint, render_template, session, flash, redirect, url_for
from app.models.users import User
from app.services import topic_service

history_bp = Blueprint("history", __name__)


@history_bp.route("/history")
def history():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    if not user:
        flash("You must be logged in to view your scan history.")
        return redirect(url_for("auth.login"))

    saved_scans = topic_service.get_saved_scans(user.get_user_id())

    return render_template("history.html", user=user, saved_scans=saved_scans)
