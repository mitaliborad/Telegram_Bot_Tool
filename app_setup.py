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

from flask import Flask
import logging
from flask_cors import CORS
from typing import Dict, Any
try:
    from config import format_bytes # Assuming this exists in config.py
except ImportError:
    format_bytes = None # Define a fallback or handle appropriately
    logging.warning("'format_bytes' not found in config.py or config.py missing. Jinja filter may not work.")
from flask_login import LoginManager
from flask_jwt_extended import JWTManager
import os
from datetime import timedelta

# --- Configure Logging (if not already done elsewhere and you want it here) ---
# Basic logging configuration (can be more sophisticated)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(threadName)s] [%(module)s] - %(message)s')

# --- Flask Application Setup ---
app = Flask(__name__, template_folder='.') # Assuming templates are in the root

# Use a strong, unique secret key, ideally from environment variables
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default-flask-secret-key-change-me-immediately!')
if app.secret_key == 'default-flask-secret-key-change-me-immediately!':
    logging.warning("FLASK_SECRET_KEY is using the default insecure value. SET IT IN YOUR ENVIRONMENT!")

# Use a separate strong secret key for JWT
app.config["JWT_SECRET_KEY"] = os.environ.get('JWT_SECRET_KEY', 'default-jwt-secret-key-change-me-immediately!')
if app.config["JWT_SECRET_KEY"] == 'default-jwt-secret-key-change-me-immediately!':
    logging.warning("JWT_SECRET_KEY is using the default insecure value. SET IT IN YOUR ENVIRONMENT!")

# Configure token expiration
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1)

# Register custom Jinja filters
if format_bytes:
    app.jinja_env.filters['format_bytes'] = format_bytes
    logging.info("Custom Jinja filter 'format_bytes' registered.")
else:
    logging.info("Custom Jinja filter 'format_bytes' NOT registered as it was not found.")

# --- Initialize CORS ---
# Get the frontend URL from an environment variable.
# Default to localhost for local development if the env var isn't set.
frontend_url = os.environ.get("FRONTEND_URL") # No default here for production, to make issues obvious
if not frontend_url:
    logging.error("FRONTEND_URL environment variable is NOT SET. CORS will likely fail for your frontend.")
    # For local development, you might uncomment the line below,
    # but it should definitely be set in Render's environment.
    # frontend_url = "http://localhost:4200"
    allowed_origins_list = [] # Or handle as an error
else:
    # Allow for multiple origins if needed, separated by commas
    allowed_origins_list = [url.strip() for url in frontend_url.split(',')]

if allowed_origins_list:
    CORS(app, origins=allowed_origins_list, supports_credentials=True, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
    logging.info(f"Flask-CORS initialized. Allowing origins: {', '.join(allowed_origins_list)}")
else:
    logging.warning("Flask-CORS initialized with NO ALLOWED ORIGINS due to missing FRONTEND_URL.")


# --- Initialize JWT ---
jwt = JWTManager(app)
logging.info("Flask-JWT-Extended initialized.")

# --- Initialize Flask-Login (Keep if used for non-API parts) ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' # Route function name for server-side login page
login_manager.login_message = u"Please log in to access this page."
login_manager.login_message_category = "info"
logging.info("Flask-Login initialized.")

# --- Global State (Consider if these truly need to be global in this module) ---
upload_progress_data: Dict[str, Any] = {}
download_prep_data: Dict[str, Any] = {}
logging.info("Global state variables initialized (upload_progress_data, download_prep_data).")

# DO NOT ADD "from app_setup import app" or "import routes" HERE
# That caused the circular import.
# Routes will be imported in your main entry point file (e.g., main.py)