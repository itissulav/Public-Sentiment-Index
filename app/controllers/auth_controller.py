from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from app.models.users import User
import app.services.auth_service as service

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]

        user = User.login_user_email(email, password)

        result = service.loginUser(user)

        if result.get("success"):
            session["user"] = user.to_dict()
            return redirect(url_for("home.home"))
        else:
            flash(result.get("error", "An unknown error occurred."))
            return redirect(url_for("auth.login"))

    return render_template("login.html")


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        user = User.register_user(
            first_name=request.form.get("first_name"),
            last_name=request.form.get("last_name"),
            email=request.form.get("email"),
            username=request.form.get("username"),
            password=request.form.get("password")
        )

        response = service.registerUser(user)

        if response.get("success"):
            session["user"] = user.to_dict()
            return redirect(url_for("home.home"))
        else:
            flash(response.get("error", "Registration failed."))
            return redirect(url_for("auth.register"))

    return render_template("register.html")

@auth_bp.route("/logout", methods=["GET", "POST"])
def logout():
    session["user"] = None
    response = service.logoutUser()

    if not response.get("success"):
        flash(response.get("error", "Error logging out."))
        
    return redirect(url_for("home.home"))

