"""
app/controllers/api_controller.py
===================================
Routes: GET /api/fetch_progress, GET /api/topic-image, GET /api/topic_status
"""

from flask import Blueprint, jsonify, session, request
from app.models.users import User
from app.services import topic_service, comment_service

api_bp = Blueprint("api", __name__)


@api_bp.route("/api/fetch_progress")
def get_fetch_progress():
    from app.api.reddit import fetch_progress
    return jsonify(fetch_progress)


@api_bp.route("/api/topic-image")
def api_topic_image():
    """Return a Wikipedia thumbnail URL for any topic name."""
    topic = request.args.get("topic", "").strip()
    if not topic:
        return jsonify({"url": None})
    from app.api.wiki import get_topic_image_url
    url = get_topic_image_url(topic)
    return jsonify({"url": url})


@api_bp.route("/api/topic_status")
def api_topic_status():
    """Polling endpoint: returns the background job state for a given topic."""
    from app.utils import job_tracker

    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user:
        return jsonify({"status": "unauthenticated"}), 401

    topic = request.args.get("topic", "").strip()
    if not topic:
        return jsonify({"status": "missing_topic"}), 400

    user_id = user.get_user_id()
    status  = job_tracker.get_status(user_id, topic)
    step    = job_tracker.get_step(user_id, topic)

    if status in (None, "not_found"):
        # DB fallback: check whether the topic already has data after a restart
        tid = topic_service.get_user_topic_id(topic, str(user_id))
        if tid and comment_service.get_comment_count(tid) > 0:
            return jsonify({"status": "complete", "step": None})

    return jsonify({"status": status or "not_found", "step": step})
