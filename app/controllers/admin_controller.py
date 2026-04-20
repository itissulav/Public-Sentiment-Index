"""
app/controllers/admin_controller.py
=====================================
Routes: GET /admin, POST|PUT|DELETE /admin/api/topics,
        DELETE /admin/api/predefined-topics/<id>,
        POST /admin/api/topics/rerun
"""

import threading
from flask import Blueprint, render_template, session, flash, redirect, url_for, request, jsonify, current_app
from app.models.users import User
from app.services import admin_service, topic_service
from app.services.analysis_service import run_predefined_analysis

admin_bp = Blueprint("admin", __name__)


def _is_admin(user) -> bool:
    return user and str(user.get_role()).lower() == "admin"


def _start_predefined_analysis(topic_name: str, purge_existing: bool = False):
    """Kick off the full 5k Reddit + 5k YouTube analysis in a daemon thread."""
    _app_obj = current_app._get_current_object()
    threading.Thread(
        target=run_predefined_analysis,
        args=(_app_obj, topic_name),
        kwargs={"purge_existing": purge_existing},
        daemon=True,
    ).start()
    print(f"[admin] Started predefined analysis for '{topic_name}' "
          f"(purge_existing={purge_existing})")


@admin_bp.route("/admin")
def admindashboard():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    if not user:
        flash("You must be logged in to view that page.")
        return redirect(url_for("auth.login"))

    if not _is_admin(user):
        flash("Access Denied. Administrator privileges required.")
        return redirect(url_for("home.home"))

    total_users, all_users       = admin_service.get_all_users()
    total_topics, predefined_topics = admin_service.get_predefined_topics()
    topic_service.enrich_predefined_topics(predefined_topics)

    return render_template(
        "admindashboard.html",
        user=user,
        total_users=total_users,
        all_users=all_users,
        total_topics=total_topics,
        predefined_topics=predefined_topics,
    )


@admin_bp.route("/admin/api/topics", methods=["POST"])
def api_add_topic():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not _is_admin(user):
        return jsonify({"error": "Unauthorized"}), 403

    # Validate required fields before touching the DB
    payload = request.json or {}
    topic_name = (payload.get("name") or payload.get("TopicName") or "").strip()
    if not topic_name:
        return jsonify({"error": "Topic name is required."}), 400

    result = admin_service.add_topic(payload)
    if not result:
        return jsonify({"error": "Failed to add topic"}), 500

    # Invalidate the predefined-topic cache so the new topic appears on
    # Home/Trends without waiting for the 60 s TTL.
    try:
        topic_service.invalidate_predefined_cache()
    except Exception:
        pass

    # Fire-and-forget full analysis — 5k Reddit + 5k YouTube, local model.
    _start_predefined_analysis(topic_name, purge_existing=False)

    return jsonify({
        "success": True,
        "data": result,
        "message": f"Topic '{topic_name}' added — 5k Reddit + 5k YouTube analysis started in background.",
    }), 201


@admin_bp.route("/admin/api/topics/<int:topic_id>", methods=["PUT"])
def api_update_topic(topic_id):
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not _is_admin(user):
        return jsonify({"error": "Unauthorized"}), 403

    result = admin_service.update_topic(topic_id, request.json)
    if result:
        return jsonify({"success": True, "data": result}), 200
    return jsonify({"error": "Failed to update topic"}), 500


@admin_bp.route("/admin/api/topics/<int:topic_id>", methods=["DELETE"])
def api_delete_topic(topic_id):
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not _is_admin(user):
        return jsonify({"error": "Unauthorized"}), 403

    success = admin_service.delete_topic(topic_id)
    if success:
        return jsonify({"success": True}), 200
    return jsonify({"error": "Failed to delete topic"}), 500


@admin_bp.route("/admin/api/predefined-topics/<topic_id>", methods=["DELETE"])
def api_delete_predefined_topic(topic_id):
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not _is_admin(user):
        return jsonify({"error": "Unauthorized"}), 403

    try:
        topic_service.delete_predefined_topic(int(topic_id))
        try:
            topic_service.invalidate_predefined_cache()
        except Exception:
            pass
        return jsonify({"success": True}), 200
    except Exception as e:
        print(f"Error deleting predefined topic {topic_id}: {e}")
        return jsonify({"error": str(e)}), 500


@admin_bp.route("/admin/api/topics/rerun", methods=["POST"])
def api_rerun_topic():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not _is_admin(user):
        return jsonify({"error": "Unauthorized"}), 403

    topic_name = (request.json or {}).get("topicName")
    if not topic_name:
        return jsonify({"error": "No topic name provided"}), 400

    # Rerun wipes existing data first, then re-runs the full 5k + 5k pipeline.
    _start_predefined_analysis(topic_name, purge_existing=True)
    return jsonify({"success": True, "message": "Analysis started safely in background"}), 200
