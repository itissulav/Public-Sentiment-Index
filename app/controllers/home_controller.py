"""
app/controllers/home_controller.py
====================================
Routes: GET /, GET /about
"""

from flask import Blueprint, render_template, session
from app.models.users import User
from app.services import topic_service

home_bp = Blueprint("home", __name__)


@home_bp.route("/")
def home():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    trending_topics = topic_service.get_trending_topics(limit=5)

    saved_scan_count = 0
    if user:
        saved_scan_count = topic_service.get_saved_scan_count(user.get_user_id())

    return render_template(
        "home.html",
        user=user,
        trending_topics=trending_topics,
        saved_scan_count=saved_scan_count,
    )


@home_bp.route("/about")
def about():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    return render_template("about.html", user=user)
