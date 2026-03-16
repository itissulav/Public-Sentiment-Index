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
    """Fetch all predefined (shared) topics from search_topics where user_id IS NULL."""
    try:
        response = admin_supabase.table("search_topics") \
                                 .select("id, name, rating, sentiment, total_comments, last_updated") \
                                 .is_("user_id", "null") \
                                 .order("name") \
                                 .execute()
        return len(response.data), response.data
    except Exception as e:
        print("Error fetching predefined topics:", e)
        return 0, []

def add_topic(topic_data):
    try:
        # Fetch current max ID to implement auto-increment
        max_id_resp = admin_supabase.table("Topic").select("id").order("id", desc=True).limit(1).execute()
        max_id = max_id_resp.data[0]['id'] if max_id_resp.data else 0
        topic_data['id'] = max_id + 1
        
        response = admin_supabase.table("Topic").insert(topic_data).execute()
        return response.data
    except Exception as e:
        print("Error adding topic:", e)
        return None

def update_topic(topic_id, topic_data):
    try:
        response = admin_supabase.table("Topic").update(topic_data).eq("id", topic_id).execute()
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