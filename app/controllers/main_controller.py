# app/controllers/main_controller.py

from flask import Blueprint, render_template, session, flash, redirect, url_for, current_app, request, jsonify
import os
import json
import pandas as pd
from app.models.users import User
from app.services.supabase_client import admin_supabase
from app.utils.visualizer import ElectionDataVisualizer
from app.services import admin_service

# This MUST match what you import in __init__.py
main_bp = Blueprint("main", __name__)  

@main_bp.route("/")
def home():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    return render_template("home.html", user=user)

@main_bp.route("/about")
def about():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    return render_template("about.html", user=user)

@main_bp.route("/admin")
def admindashboard():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    
    if not user:
        flash("You must be logged in to view that page.")
        return redirect(url_for("auth.login"))
        
    if str(user.get_role()).lower() != "admin":
        flash("Access Denied. Administrator privileges required.")
        return redirect(url_for("main.home"))
    
    total_users, all_users = admin_service.get_all_users()
    total_topics, all_topics = admin_service.get_all_topics()

    return render_template("admindashboard.html", user=user, total_users=total_users, all_users=all_users, total_topics=total_topics, topics=all_topics)

@main_bp.route("/trends", methods=["GET", "POST"])
def trends():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    
    current_topic = None
    charts_data = {}
    top_positive = None
    top_negative = None
    
    PREDEFINED_TOPICS = ["Donald Trump", "The Boys", "Avengers Doomsday", "Macbook Neo", "America vs Iran"]
    
    if request.method == "POST":
        topic_query = request.form.get("topic")
        
        if topic_query:
            current_topic = topic_query
            
            # Helper to fetch and extract data from Relational DB
            def extract_and_visualize(t_id):
                nonlocal charts_data, top_positive, top_negative
                comment_res = admin_supabase.table("reddit_comments").select("*").eq("topic_id", t_id).execute()
                if comment_res.data:
                    df = pd.DataFrame(comment_res.data)
                    
                    # Extract top viral comments for the Reddit UI component
                    if not df[df['sentiment_label'] == 'Positive'].empty:
                        top_positive = df[df['sentiment_label'] == 'Positive'].nlargest(1, 'score').to_dict('records')[0]
                    if not df[df['sentiment_label'] == 'Negative'].empty:
                        top_negative = df[df['sentiment_label'] == 'Negative'].nlargest(1, 'score').to_dict('records')[0]
                        
                    try:
                        vis = ElectionDataVisualizer(df)
                        charts_data = vis.get_all_charts_data()
                        return True
                    except Exception as e:
                        flash(f"Error visualizing data: {str(e)}")
                return False

            if current_topic in PREDEFINED_TOPICS:
                # 1. PREDEFINED TOPIC: Pull dynamically from relational DB schema
                topic_res = admin_supabase.table("search_topics").select("id").eq("name", current_topic).execute()
                
                if topic_res.data:
                    topic_id = topic_res.data[0]['id']
                    if not extract_and_visualize(topic_id):
                         flash(f"No comments found for predefined topic: {current_topic}")
                else:
                    flash(f"No database records found for preset topic: {current_topic}. Please run the seeder script first!")
                    
            else:
                # 2. CUSTOM TOPIC: Scrape Reddit (limit=250 posts deep) -> HF API -> Supabase DB -> Render
                from app.utils.fetcher import get_reddit_comments
                from app.utils.hf_analyzer import process_and_store_comments
                
                df = get_reddit_comments(topic_query, limit_posts=250, max_comments=2000, subreddit_name="all", topic_name=topic_query)
                
                if not df.empty:
                    # Run Hugging Face Analysis and Store in DB (creates and links tables)
                    process_and_store_comments(topic_query, df)
                    
                    # Immediately query DB to visualize the exact saved data
                    topic_res = admin_supabase.table("search_topics").select("id").eq("name", current_topic).execute()
                    if topic_res.data:
                         topic_id = topic_res.data[0]['id']
                         extract_and_visualize(topic_id)
                else:
                    flash(f"No comments found for custom topic: {current_topic}")

    else:
        # GET request: Render completely empty to wait for user interaction
        pass
    
    return render_template("trends.html", user=user, charts_data=json.dumps(charts_data), current_topic=current_topic, top_positive=top_positive, top_negative=top_negative)


# --- PROGRESS API ROUTE ---
@main_bp.route("/api/fetch_progress")
def get_fetch_progress():
    from app.utils.fetcher import fetch_progress
    return jsonify(fetch_progress)

# --- ADMIN API ROUTES FOR TOPICS ---

@main_bp.route("/admin/api/topics", methods=["POST"])
def api_add_topic():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403
        
    data = request.json
    result = admin_service.add_topic(data)
    if result:
        return jsonify({"success": True, "data": result}), 201
    return jsonify({"error": "Failed to add topic"}), 500

@main_bp.route("/admin/api/topics/<int:topic_id>", methods=["PUT"])
def api_update_topic(topic_id):
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403
        
    data = request.json
    result = admin_service.update_topic(topic_id, data)
    if result:
        return jsonify({"success": True, "data": result}), 200
    return jsonify({"error": "Failed to update topic"}), 500

@main_bp.route("/admin/api/topics/<int:topic_id>", methods=["DELETE"])
def api_delete_topic(topic_id):
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403
        
    success = admin_service.delete_topic(topic_id)
    if success:
        return jsonify({"success": True}), 200
    return jsonify({"error": "Failed to delete topic"}), 500
