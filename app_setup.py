from flask import Flask
import logging
from flask_cors import CORS
from typing import Dict, Any
from config import format_bytes
from flask_login import LoginManager
from flask_jwt_extended import JWTManager
import os
from datetime import timedelta # <<< CORRECTED IMPORT
from dotenv import load_dotenv
load_dotenv()

# static_folder_path = os.path.join(os.path.dirname(__file__), 'dist', 'telegrambot')

# --- Flask Application Setup ---
app = Flask(__name__, template_folder='.')

# Use a strong, unique secret key, ideally from environment variables
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default-flask-secret-key-change-me!') # Change this default!
# Use a separate strong secret key for JWT
app.config["JWT_SECRET_KEY"] = os.environ.get('JWT_SECRET_KEY', 'default-jwt-secret-key-change-me!') # Change this default!
# Configure token expiration
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=1) # <<< This line should now work

# Register custom Jinja filters
app.jinja_env.filters['format_bytes'] = format_bytes
logging.info("Custom Jinja filter 'format_bytes' registered.")

#default_frontend_url = "http://localhost:4200"
FRONTEND_URL_FROM_ENV = os.environ.get('FRONTEND_URL')
#, default_frontend_url
#allowed_origins = FRONTEND_URL_FROM_ENV
allowed_origins = "*"

# --- Initialize CORS ---
# allowed_origins = "https://telegrambot-rosy-psi.vercel.app/home"
# CORS(app, origins=allowed_origins, supports_credentials=True)
# logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins}")

#allowed_origins = "https://telegrambot-rosy-psi.vercel.app"
CORS(app, origins=allowed_origins, supports_credentials=True, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins}")


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

# --- Global State ---
upload_progress_data: Dict[str, Any] = {}
download_prep_data: Dict[str, Any] = {}
logging.info("Global state variables initialized (upload_progress_data, download_prep_data).")
import logging
import os 

# --- Import App and Config ---
from app_setup import app
import routes


# --- Application Runner ---
if __name__ == '__main__':
    logging.info("Starting Flask development server...")
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)