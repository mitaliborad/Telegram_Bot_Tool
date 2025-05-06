# from flask import Flask
# import logging
# from flask_cors import CORS
# from typing import Dict, Any
# from config import format_bytes
# from flask_login import LoginManager
# from flask_jwt_extended import JWTManager
# import os
# from datetime import timedelta # <<< CORRECTED IMPORT

# # --- Flask Application Setup ---
# app = Flask(__name__, template_folder='.')

# # Use a strong, unique secret key, ideally from environment variables
# app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default-flask-secret-key-change-me!') # Change this default!
# # Use a separate strong secret key for JWT
# app.config["JWT_SECRET_KEY"] = os.environ.get('JWT_SECRET_KEY', 'default-jwt-secret-key-change-me!') # Change this default!
# # Configure token expiration
# app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1) # <<< This line should now work

# # Register custom Jinja filters
# app.jinja_env.filters['format_bytes'] = format_bytes
# logging.info("Custom Jinja filter 'format_bytes' registered.")

# # --- Initialize CORS ---
# allowed_origins = "http://localhost:4200"
# CORS(app, origins=allowed_origins, supports_credentials=True)
# logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins}")

# # --- Initialize JWT ---
# jwt = JWTManager(app)
# logging.info("Flask-JWT-Extended initialized.")

# # --- Initialize Flask-Login (Keep if used for non-API parts) ---
# login_manager = LoginManager()
# login_manager.init_app(app)
# login_manager.login_view = 'login' # Route function name for server-side login page
# login_manager.login_message = u"Please log in to access this page."
# login_manager.login_message_category = "info"
# logging.info("Flask-Login initialized.")

# # --- Global State ---
# upload_progress_data: Dict[str, Any] = {}
# download_prep_data: Dict[str, Any] = {}
# logging.info("Global state variables initialized (upload_progress_data, download_prep_data).")
# import logging
# import os 

# # --- Import App and Config ---
# from app_setup import app
# import routes


# # --- Application Runner ---
# if __name__ == '__main__':
#     logging.info("Starting Flask development server...")
#     app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)





# app_setup.py

import os
import logging
from flask import Flask
from flask_cors import CORS
from flask_login import LoginManager
from flask_jwt_extended import JWTManager
from datetime import timedelta
from typing import Dict, Any

# Attempt to import format_bytes from a local config.py
# If config.py or format_bytes doesn't exist, it will be handled gracefully.
try:
    from config import format_bytes
    HAS_FORMAT_BYTES = True
except ImportError:
    HAS_FORMAT_BYTES = False
    logging.warning("Could not import 'format_bytes' from 'config.py'. Jinja filter will not be available.")
    # Define a dummy function if you still want to register something or handle it in Jinja
    def format_bytes(size):
        return f"{size} B" # Fallback basic formatting

# --- Flask Application Setup ---
app = Flask(__name__, template_folder='.') # Assuming templates are in the root for now

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Flask application instance created.")

# --- Secret Keys Configuration ---
# Use strong, unique secret keys, ideally from environment variables.
# Provide defaults for local development, but these should be overridden in production.
default_flask_secret = 'your-default-flask-secret-key-for-dev-pls-change-in-prod'
default_jwt_secret = 'your-default-jwt-secret-key-for-dev-pls-change-in-prod'

app.secret_key = os.environ.get('FLASK_SECRET_KEY', default_flask_secret)
if app.secret_key == default_flask_secret:
    logging.warning("FLASK_SECRET_KEY is using the default development value. SET THIS IN PRODUCTION!")

app.config["JWT_SECRET_KEY"] = os.environ.get('JWT_SECRET_KEY', default_jwt_secret)
if app.config["JWT_SECRET_KEY"] == default_jwt_secret:
    logging.warning("JWT_SECRET_KEY is using the default development value. SET THIS IN PRODUCTION!")

# Configure token expiration (e.g., 1 hour for access tokens)
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=int(os.environ.get('JWT_ACCESS_TOKEN_EXPIRES_HOURS', 1)))
app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(days=int(os.environ.get('JWT_REFRESH_TOKEN_EXPIRES_DAYS', 30))) # Example for refresh tokens
logging.info(f"JWT access token expiration set to {app.config['JWT_ACCESS_TOKEN_EXPIRES']}.")

# --- Register Custom Jinja Filters (if available) ---
if HAS_FORMAT_BYTES:
    app.jinja_env.filters['format_bytes'] = format_bytes
    logging.info("Custom Jinja filter 'format_bytes' registered.")
else:
    # Optionally, register the fallback if you want to avoid template errors
    # app.jinja_env.filters['format_bytes'] = format_bytes # This would use the dummy
    logging.info("'format_bytes' Jinja filter not registered as it was not found in config.py.")


# --- Initialize CORS (Cross-Origin Resource Sharing) ---
# Get the frontend URL from an environment variable.
# Fallback to localhost for local development if the env var isn't set.
# Ensure FRONTEND_URL is set in Render's environment variables (e.g., https://your-app.vercel.app)
frontend_url = os.environ.get("FRONTEND_URL")
if not frontend_url:
    logging.warning("FRONTEND_URL environment variable not set. CORS might not work as expected in production.")
    # For local development, you might want a default:
    # frontend_url = "http://localhost:4200"

# Configure CORS to allow requests from your frontend domain
# If frontend_url is not set, CORS will be restrictive by default or might not work.
# It's better to have a defined list of origins.
allowed_origins = []
if frontend_url:
    allowed_origins.append(frontend_url)
else:
    # If no FRONTEND_URL, you might want to allow a specific local dev URL by default,
    # but this is less secure if accidentally deployed.
    # allowed_origins.append("http://localhost:4200") # Example, be cautious
    logging.warning("No FRONTEND_URL specified for CORS, requests from frontend may be blocked.")


# For more robust local development with multiple potential origins:
# dev_origins = ["http://localhost:4200", "http://127.0.0.1:4200"]
# if not os.environ.get("RENDER"): # Check if not running on Render
#    allowed_origins.extend(o for o in dev_origins if o not in allowed_origins)

CORS(app,
     origins=allowed_origins if allowed_origins else "*", # Be careful with "*" in production
     supports_credentials=True,
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"], # Specify methods your API uses
     allow_headers=["Content-Type", "Authorization", "X-Requested-With"] # Common headers
)
logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins if allowed_origins else 'ALL (if default was *)'}")


# --- Initialize JWTManager (Flask-JWT-Extended) ---
jwt = JWTManager(app)
logging.info("Flask-JWT-Extended initialized.")

# --- Initialize Flask-Login (If still used for session-based auth, e.g., admin panel) ---
# If your entire app is API-based and uses JWT for everything, you might not need Flask-Login.
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth_routes.login' # Example: if login route is in an 'auth_routes' blueprint
login_manager.login_message = u"Please log in to access this page."
login_manager.login_message_category = "info"
# You'll need a user_loader callback for Flask-Login:
# @login_manager.user_loader
# def load_user(user_id):
#     # Return user object from your database or user store
#     return User.get(user_id) # Replace User.get with your actual user retrieval logic
logging.info("Flask-Login initialized. Remember to set up a user_loader function.")


# --- Global Application State (Example) ---
# Use these cautiously; for more complex state, consider a database or Redis.
upload_progress_data: Dict[str, Any] = {}
download_prep_data: Dict[str, Any] = {}
logging.info("Global state variables (upload_progress_data, download_prep_data) initialized.")

logging.info("app_setup.py finished execution.")

# IMPORTANT: Do NOT include app.run() here.
# Gunicorn (or another WSGI server) will be responsible for running the 'app' object
# when deployed to Render. main.py can contain app.run() for local development.