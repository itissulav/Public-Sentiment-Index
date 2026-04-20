"""
app/controllers/profile_controller.py
=======================================
Routes: GET|POST /profile
"""

from flask import Blueprint, render_template, session, flash, redirect, url_for, request
from app.models.users import User
from app.services import topic_service

profile_bp = Blueprint("profile", __name__)


@profile_bp.route("/profile", methods=["GET", "POST"])
def profile():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    if not user:
        flash("You must be logged in to view your profile.")
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        first_name = request.form.get("first_name", "").strip()
        last_name  = request.form.get("last_name", "").strip()
        username   = request.form.get("username", "").strip()

        if not first_name or not last_name or not username:
            flash("All fields are required.")
        else:
            try:
                topic_service.update_user_profile(user.get_user_id(), {
                    "First Name": first_name,
                    "Last Name":  last_name,
                    "Username":   username,
                })
                user.set_first_name(first_name)
                user.set_last_name(last_name)
                user.set_username(username)
                session["user"] = user.to_dict()
                flash("Profile updated successfully.")
            except Exception as e:
                flash(f"Could not update profile: {e}")

        return redirect(url_for("profile.profile"))

    scan_count = None
    if user.get_role().lower() != "admin":
        scan_count = topic_service.get_saved_scan_count(user.get_user_id())

    return render_template("profile.html", user=user, scan_count=scan_count)
