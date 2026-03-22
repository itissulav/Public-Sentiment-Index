from app.services.supabase_client import public_supabase, admin_supabase

def registerUser(user):
    firstName = user.get_first_name()
    lastName = user.get_last_name()
    email = user.get_email()
    username = user.get_username()
    password = user.get_password()

    try:
        # 1. Sign up the user in Supabase Auth
        auth_response = public_supabase.auth.sign_up({
            "email" : email,
            "password": password
        })

        if auth_response.user is None:
            return {"success": False, "error": "Could not create user account. Please try again."}

        user_id = auth_response.user.id

        # 2. Insert the user details into our custom users table
        try:
            db_response = (
                admin_supabase.table("Users")
                .insert({
                    "UID": user_id,
                    "First Name": firstName,
                    "Last Name": lastName,
                    "Email": email,
                    "Username": username,
                    "Role": "User"
                })
                .execute()
            )
            return {"success": True, "data": db_response, "error": None}

        except Exception as e:
            print(f"Database Insert Error: {e}")
            return {"success": False, "error": "Your account was created, but we failed to save your profile details. Please contact support."}

    except Exception as e:
        error_msg = str(e)
        if "User already registered" in error_msg:
            return {"success": False, "error": "An account with this email already exists."}
        elif "Password should be at least" in error_msg:
            return {"success": False, "error": "Your password is too weak. Please use a stronger password."}

        print(f"Supabase Auth Error: {e}")
        return {"success": False, "error": "Registration failed due to a server error. Please try again later."}

def loginUser(user):
    email = user.get_email()
    password = user.get_password()

    try:
        response = public_supabase.auth.sign_in_with_password({
            "email": email,
            "password": password
        })

        if response.user is None:
            return {"success": False, "error": "Invalid email or password."}

        user_id = response.user.id
        auth_email = response.user.email

        try:
            # Get user's extra profile info
            db_user = admin_supabase.table("Users").select("*").eq("UID", user_id).execute()

            if db_user.data and len(db_user.data) > 0:
                user_data = db_user.data[0]
                user.set_user_id(user_data.get("UID"))
                user.set_first_name(user_data.get("First Name"))
                user.set_last_name(user_data.get("Last Name"))
                user.set_username(user_data.get("Username"))
                user.set_role(user_data.get("Role"))
            else:
                # Account exists in Supabase Auth but has no profile row —
                # this happens when the account was created via the Supabase dashboard.
                print(f"No profile row found for UID {user_id}, auto-creating one...")
                username = auth_email.split("@")[0]
                insert_resp = admin_supabase.table("Users").insert({
                    "UID": user_id,
                    "First Name": username,
                    "Last Name": "",
                    "Email": auth_email,
                    "Username": username,
                    "Role": "Admin"
                }).execute()

                if insert_resp.data:
                    user_data = insert_resp.data[0]
                    user.set_user_id(user_data.get("UID"))
                    user.set_first_name(user_data.get("First Name"))
                    user.set_last_name(user_data.get("Last Name"))
                    user.set_username(user_data.get("Username"))
                    user.set_role(user_data.get("Role"))
                    print(f"Auto-created profile row for {auth_email} with Role=Admin")
                else:
                    return {"success": False, "error": "Could not find or create your user profile."}

        except Exception as e:
            print(f"Profile Fetch/Insert Error: {e}")
            return {"success": False, "error": "Logged in, but failed to load profile data."}

        return {"success": True, "data": response, "error": None}

    except Exception as e:
        error_msg = str(e).lower()
        if "invalid login credentials" in error_msg:
            return {"success": False, "error": "Invalid email or password."}

        print(f"Login error: {e}")
        return {"success": False, "error": "An unexpected error occurred during login. Please try again."}

def logoutUser():
    try:
        response = public_supabase.auth.sign_out()
        return {"success": True, "error": None}
    except Exception as e:
        print(f"Logout Error: {e}")
        return {"success": False, "error": "Failed to log out properly. Please refresh the page."}
