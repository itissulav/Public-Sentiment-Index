from flask import Flask
import os

def create_app():
    app = Flask(__name__)
    app.secret_key = "dev-secret-key"
    app.config["APP_BASE_URL"] = os.getenv("APP_BASE_URL", "http://127.0.0.1:5000")

    from app.controllers.auth_controller import auth_bp
    from app.controllers.main_controller import main_bp

    app.register_blueprint(auth_bp)      # auth blueprint
    app.register_blueprint(main_bp)      # main blueprint

    return app