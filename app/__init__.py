from flask import Flask
import os

def create_app():
    app = Flask(__name__)
    app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-me")
    app.config["APP_BASE_URL"] = os.getenv("APP_BASE_URL", "http://127.0.0.1:5000")

    from app.controllers.auth_controller          import auth_bp
    from app.controllers.home_controller          import home_bp
    from app.controllers.trends_controller        import trends_bp
    from app.controllers.pulse_controller         import pulse_bp
    from app.controllers.compare_controller       import compare_bp
    from app.controllers.profile_controller       import profile_bp
    from app.controllers.notifications_controller import notifications_bp
    from app.controllers.history_controller       import history_bp
    from app.controllers.api_controller           import api_bp
    from app.controllers.admin_controller         import admin_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(home_bp)
    app.register_blueprint(trends_bp)
    app.register_blueprint(pulse_bp)
    app.register_blueprint(compare_bp)
    app.register_blueprint(profile_bp)
    app.register_blueprint(notifications_bp)
    app.register_blueprint(history_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(admin_bp)

    return app
