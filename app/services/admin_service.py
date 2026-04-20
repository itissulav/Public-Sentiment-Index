from app.services.supabase_client import public_supabase, admin_supabase

def get_all_users():

    try:     
        # Fetch registered users
        recent_users_response = admin_supabase.table("Users").select("*").execute()
        all_users = recent_users_response.data
        total_users = len(all_users)

        print(all_users)
        
    except Exception as e:
        print("Error fetching admin stats:", e)
        total_users = 0
        all_users = []

    return total_users, all_users

def get_all_topics():

    try:
        response = admin_supabase.table("Topic").select("*").execute()
        return len(response.data), response.data
    except Exception as e:
        print("Error fetching topics:", e)
        return 0, []

def get_predefined_topics():
    """Fetch all predefined (shared) topics from topics where user_id IS NULL."""
    try:
        response = admin_supabase.table("topics") \
                                 .select("id, name, category, created_at") \
                                 .is_("user_id", "null") \
                                 .order("name") \
                                 .execute()
        return len(response.data), response.data
    except Exception as e:
        print("Error fetching predefined topics:", e)
        return 0, []

def add_topic(topic_data: dict):
    """Insert a new predefined topic into the v2 topics table."""
    try:
        name = (topic_data.get("name") or topic_data.get("TopicName") or "").strip()
        if not name:
            return None
        response = admin_supabase.table("topics").insert({
            "name":    name,
            "user_id": None,   # NULL = predefined topic
        }).execute()
        return response.data
    except Exception as e:
        print("Error adding topic:", e)
        return None

def update_topic(topic_id: int, topic_data: dict):
    """Update a predefined topic's name in the v2 topics table."""
    try:
        name = (topic_data.get("name") or topic_data.get("TopicName") or "").strip()
        if not name:
            return None
        response = admin_supabase.table("topics").update({
            "name": name,
        }).eq("id", topic_id).is_("user_id", "null").execute()
        return response.data
    except Exception as e:
        print("Error updating topic:", e)
        return None

def delete_topic(topic_id):
    try:
        response = admin_supabase.table("Topic").delete().eq("id", topic_id).execute()
        return True
    except Exception as e:
        print("Error deleting topic:", e)
        return False