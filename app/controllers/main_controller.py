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

    trending_topics = []
    try:
        res = admin_supabase.table("search_topics").select("*").is_("user_id", "null").order("rating", desc=True).limit(5).execute()
        if res.data:
            trending_topics = res.data
    except:
        pass

    saved_scan_count = 0
    if user:
        try:
            res = admin_supabase.table("search_topics").select("id", count="exact") \
                                .eq("user_id", user.get_user_id()).execute()
            saved_scan_count = res.count or 0
        except:
            pass

    return render_template("home.html", user=user, trending_topics=trending_topics, saved_scan_count=saved_scan_count)

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
    total_topics, predefined_topics = admin_service.get_predefined_topics()

    return render_template("admindashboard.html", user=user, total_users=total_users, all_users=all_users, total_topics=total_topics, predefined_topics=predefined_topics)

@main_bp.route("/trends", methods=["GET", "POST"])
def trends():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    current_user_id = user.get_user_id() if user else None

    current_topic = None
    charts_data = {}
    top_positive = None
    top_negative = None
    topic_info = None
    insights_data = {}
    job_status = None   # 'processing' | None

    if request.method == "POST":
        topic_query  = request.form.get("topic")
        topic_id_param = request.form.get("topic_id")   # set by View Analysis on history page

        if topic_query:
            current_topic = topic_query

            # Helper to fetch and extract data from Relational DB
            def extract_and_visualize(t_id):
                nonlocal charts_data, top_positive, top_negative, insights_data
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
                    except Exception as e:
                        flash(f"Error visualizing data: {str(e)}")

                    try:
                        from app.utils.insights import compute_all_insights
                        insights_data = compute_all_insights(df)
                    except Exception as e:
                        print(f"Insights error: {e}")

                    return True
                return False

            # VIEW SAVED SCAN: topic_id provided → load from DB without re-scraping
            if topic_id_param and current_user_id:
                topic_res = admin_supabase.table("search_topics") \
                                          .select("id, name, rating, sentiment, total_comments") \
                                          .eq("id", topic_id_param) \
                                          .eq("user_id", current_user_id) \
                                          .execute()
                if topic_res.data:
                    topic_id   = topic_res.data[0]['id']
                    topic_info = topic_res.data[0]
                    current_topic = topic_res.data[0]['name']
                    if not extract_and_visualize(topic_id):
                        flash("No comment data found for this saved scan.")
                else:
                    flash("Saved scan not found or access denied.")

            elif current_topic in PREDEFINED_TOPICS:
                # 1. PREDEFINED TOPIC: Pull dynamically from relational DB schema
                topic_res = admin_supabase.table("search_topics") \
                                          .select("id, rating, sentiment, total_comments") \
                                          .eq("name", current_topic) \
                                          .is_("user_id", "null") \
                                          .execute()

                if topic_res.data:
                    topic_id   = topic_res.data[0]['id']
                    topic_info = topic_res.data[0]
                    if not extract_and_visualize(topic_id):
                         flash(f"No comments found for predefined topic: {current_topic}")
                else:
                    flash(f"No database records found for preset topic: {current_topic}. Please run the seeder script first!")
                    
            else:
                # 2. CUSTOM TOPIC — run in background so the user isn't blocked
                from app.utils import job_tracker

                if not user:
                    flash("You must be logged in to analyse a custom topic.")
                else:
                    existing_status = job_tracker.get_status(current_user_id, current_topic)

                    if existing_status == "processing":
                        # Already running — just show the processing UI again
                        job_status = "processing"

                    elif existing_status == "complete":
                        # Job finished — load from DB and clear the tracker entry
                        job_tracker.clear(current_user_id, current_topic)
                        topic_res = admin_supabase.table("search_topics") \
                                                  .select("id, total_comments") \
                                                  .eq("name", current_topic) \
                                                  .eq("user_id", current_user_id) \
                                                  .execute()
                        if topic_res.data:
                            from app.utils.analyzer import calculate_topic_rating
                            topic_id       = topic_res.data[0]['id']
                            total_comments = topic_res.data[0].get('total_comments', 0)
                            rating, sentiment = calculate_topic_rating(topic_id)
                            topic_info = {'rating': rating, 'sentiment': sentiment, 'total_comments': total_comments}
                            extract_and_visualize(topic_id)
                        else:
                            flash(f"Analysis completed but data could not be loaded. Check your History.")

                    elif existing_status == "failed":
                        job_tracker.clear(current_user_id, current_topic)
                        flash(f"The background analysis for '{current_topic}' failed. Please try again.")

                    else:
                        # No job running — kick off a new background thread
                        job_tracker.mark_processing(current_user_id, current_topic)
                        app = current_app._get_current_object()
                        user_email     = user.get_email()
                        user_firstname = user.get_first_name()

                        thread = threading.Thread(
                            target=_background_analyse_user_topic,
                            args=(app, current_user_id, user_email, user_firstname, current_topic),
                            daemon=True,
                        )
                        thread.start()
                        job_status = "processing"

    else:
        # GET request: Render completely empty to wait for user interaction
        pass
    
    return render_template(
        "trends.html",
        user=user,
        charts_data=json.dumps(charts_data),
        insights_data=insights_data,
        current_topic=current_topic,
        top_positive=top_positive,
        top_negative=top_negative,
        topic_info=topic_info,
        job_status=job_status,
    )


PREDEFINED_TOPICS = ["Donald Trump", "The Boys", "Avengers Doomsday", "Macbook Neo", "America vs Iran"]


@main_bp.route("/compare", methods=["GET", "POST"])
def compare():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    current_user_id = user.get_user_id() if user else None

    # Build autocomplete suggestions: predefined + user history
    suggestions = [{'name': t, 'kind': 'featured'} for t in PREDEFINED_TOPICS]
    if user:
        try:
            res = admin_supabase.table("search_topics") \
                                .select("name") \
                                .eq("user_id", current_user_id) \
                                .order("last_updated", desc=True) \
                                .execute()
            if res.data:
                existing_names = {s['name'] for s in suggestions}
                for row in res.data:
                    if row['name'] not in existing_names:
                        suggestions.append({'name': row['name'], 'kind': 'saved'})
        except:
            pass

    comparison_data = None
    topic_a_name = None
    topic_b_name = None

    if request.method == "POST":
        topic_a_name = (request.form.get("topic_a") or "").strip()
        topic_b_name = (request.form.get("topic_b") or "").strip()

        if topic_a_name and topic_b_name:

            def load_topic_for_compare(topic_name):
                """Return (df, info_dict) from DB or by scraping fresh (max 500 comments)."""
                # 1. Predefined topic
                if topic_name in PREDEFINED_TOPICS:
                    res = admin_supabase.table("search_topics").select("*") \
                                        .eq("name", topic_name).is_("user_id", "null").execute()
                    if res.data:
                        t_id = res.data[0]['id']
                        cr = admin_supabase.table("reddit_comments").select("*").eq("topic_id", t_id).execute()
                        if cr.data:
                            return pd.DataFrame(cr.data), res.data[0]

                # 2. User's saved history
                if current_user_id:
                    res = admin_supabase.table("search_topics").select("*") \
                                        .eq("name", topic_name).eq("user_id", current_user_id).execute()
                    if res.data:
                        t_id = res.data[0]['id']
                        cr = admin_supabase.table("reddit_comments").select("*").eq("topic_id", t_id).execute()
                        if cr.data:
                            return pd.DataFrame(cr.data), res.data[0]

                # 3. New topic — requires login to save
                if not current_user_id:
                    flash(f"Please log in to analyse a new topic: '{topic_name}'")
                    return None, None

                from app.utils.fetcher import get_reddit_comments
                from app.utils.hf_analyzer import process_and_store_comments
                from app.utils.analyzer import calculate_topic_rating

                df = get_reddit_comments(topic_name, limit_posts=100, max_comments=500,
                                         subreddit_name="all", topic_name=topic_name)
                if df.empty:
                    return None, None

                process_and_store_comments(topic_name, df, current_user_id)

                res = admin_supabase.table("search_topics").select("*") \
                                    .eq("name", topic_name).eq("user_id", current_user_id).execute()
                if res.data:
                    t_id = res.data[0]['id']
                    calculate_topic_rating(t_id)
                    res2 = admin_supabase.table("search_topics").select("*").eq("id", t_id).execute()
                    cr   = admin_supabase.table("reddit_comments").select("*").eq("topic_id", t_id).execute()
                    if cr.data and res2.data:
                        return pd.DataFrame(cr.data), res2.data[0]

                return None, None

            df_a, info_a = load_topic_for_compare(topic_a_name)
            df_b, info_b = load_topic_for_compare(topic_b_name)

            if df_a is not None and df_b is not None:
                from app.utils.comparator import build_comparison_data
                comparison_data = build_comparison_data(df_a, info_a, df_b, info_b)
            else:
                if df_a is None:
                    flash(f"Could not load data for '{topic_a_name}'. Run a full analysis first.")
                if df_b is None:
                    flash(f"Could not load data for '{topic_b_name}'. Run a full analysis first.")

    return render_template(
        "compare.html",
        user=user,
        suggestions=suggestions,
        topic_a_name=topic_a_name,
        topic_b_name=topic_b_name,
        comparison_json=json.dumps(comparison_data) if comparison_data else None,
    )


@main_bp.route("/profile", methods=["GET", "POST"])
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
                admin_supabase.table("Users").update({
                    "First Name": first_name,
                    "Last Name":  last_name,
                    "Username":   username,
                }).eq("UID", user.get_user_id()).execute()

                # Refresh session
                user.set_first_name(first_name)
                user.set_last_name(last_name)
                user.set_username(username)
                session["user"] = user.to_dict()
                flash("Profile updated successfully.")
            except Exception as e:
                flash(f"Could not update profile: {e}")

        return redirect(url_for("main.profile"))

    # Count saved scans for regular users
    scan_count = None
    if user.get_role().lower() != "admin":
        try:
            res = admin_supabase.table("search_topics").select("id", count="exact") \
                                .eq("user_id", user.get_user_id()).execute()
            scan_count = res.count or 0
        except:
            scan_count = 0

    return render_template("profile.html", user=user, scan_count=scan_count)


@main_bp.route("/history")
def history():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None

    if not user:
        flash("You must be logged in to view your scan history.")
        return redirect(url_for("auth.login"))

    saved_scans = []
    try:
        res = admin_supabase.table("search_topics") \
                            .select("*") \
                            .eq("user_id", user.get_user_id()) \
                            .order("last_updated", desc=True) \
                            .execute()
        if res.data:
            saved_scans = res.data
    except Exception as e:
        flash(f"Could not load scan history: {e}")

    return render_template("history.html", user=user, saved_scans=saved_scans)


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

import threading


def _background_analyse_user_topic(app, user_id, user_email, first_name, topic_name):
    """Scrape → analyse → store → email for a user's custom topic."""
    from app.utils import job_tracker
    from app.utils.fetcher import get_reddit_comments
    from app.utils.hf_analyzer import process_and_store_comments
    from app.utils.analyzer import calculate_topic_rating
    from app.utils.mailer import send_analysis_ready_email

    with app.app_context():
        try:
            print(f"[bg] Starting analysis for user={user_id} topic='{topic_name}'")
            df = get_reddit_comments(
                topic_name, limit_posts=250, max_comments=2000,
                subreddit_name="all", topic_name=topic_name
            )

            if df.empty:
                print(f"[bg] No Reddit data found for '{topic_name}'")
                job_tracker.mark_failed(user_id, topic_name)
                return

            process_and_store_comments(topic_name, df, user_id)

            topic_res = admin_supabase.table("search_topics") \
                                      .select("id") \
                                      .eq("name", topic_name) \
                                      .eq("user_id", user_id) \
                                      .execute()
            if topic_res.data:
                calculate_topic_rating(topic_res.data[0]["id"])

            job_tracker.mark_complete(user_id, topic_name)
            print(f"[bg] Analysis complete for '{topic_name}' — sending email to {user_email}")

            base_url = app.config.get("APP_BASE_URL", "http://127.0.0.1:5000")
            send_analysis_ready_email(user_email, first_name, topic_name, base_url)

        except Exception as e:
            print(f"[bg] Analysis failed for '{topic_name}': {e}")
            job_tracker.mark_failed(user_id, topic_name)


@main_bp.route("/api/topic_status")
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

    status = job_tracker.get_status(user.get_user_id(), topic)
    return jsonify({"status": status or "not_found"})


def background_run_topic(topic_name):
    from app.utils.fetcher import get_reddit_comments
    from app.utils.hf_analyzer import process_and_store_comments
    from app.utils.analyzer import calculate_topic_rating
    
    try:
        print(f"Starting Background Analysis Thread for: {topic_name}")
        df = get_reddit_comments(topic_name, limit_posts=250, max_comments=2000, subreddit_name="all", topic_name=topic_name)
        if not df.empty:
            # Admin rerun is always for predefined topics — no user ownership
            process_and_store_comments(topic_name, df, user_id=None)

            # Find the predefined topic row to recalculate its rating
            res = admin_supabase.table("search_topics").select("id") \
                                .eq("name", topic_name) \
                                .is_("user_id", "null") \
                                .execute()
            if res.data:
                calculate_topic_rating(res.data[0]['id'])
        print(f"Background thread finished efficiently for: {topic_name}")
    except Exception as e:
        print(f"Background rerun failed for {topic_name}: {e}")

@main_bp.route("/admin/api/predefined-topics/<topic_id>", methods=["DELETE"])
def api_delete_predefined_topic(topic_id):
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    try:
        # Safety: only delete rows that are predefined (user_id IS NULL)
        res = admin_supabase.table("search_topics") \
                            .delete() \
                            .eq("id", topic_id) \
                            .is_("user_id", "null") \
                            .execute()
        return jsonify({"success": True}), 200
    except Exception as e:
        print(f"Error deleting predefined topic {topic_id}: {e}")
        return jsonify({"error": str(e)}), 500


@main_bp.route("/admin/api/topics/rerun", methods=["POST"])
def api_rerun_topic():
    user_data = session.get("user")
    user = User.from_dict(user_data) if user_data else None
    if not user or str(user.get_role()).lower() != "admin":
        return jsonify({"error": "Unauthorized"}), 403
        
    topic_name = request.json.get("topicName")
    if not topic_name:
         return jsonify({"error": "No topic name provided"}), 400
         
    # Launch background thread to prevent API timeout
    thread = threading.Thread(target=background_run_topic, args=(topic_name,))
    thread.daemon = True # allow flask to close without locking this thread
    thread.start()
    
    return jsonify({"success": True, "message": "Analysis started safely in background"}), 200
