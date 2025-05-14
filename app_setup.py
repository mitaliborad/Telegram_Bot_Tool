# app_setup.py
import logging
import os
from flask_cors import CORS
# from flask_login import LoginManager # No longer defined here
# from flask_jwt_extended import JWTManager # No longer defined here
from dotenv import load_dotenv

load_dotenv() # Load .env variables first

from config import app, mail, format_bytes # Import app & mail from config.py
# Import instances from your new extensions.py
from extensions import login_manager, jwt, upload_progress_data, download_prep_data

# --- Flask Application Extensions Setup ---
app.jinja_env.filters['format_bytes'] = format_bytes
# logging.info("Custom Jinja filter 'format_bytes' registered.") # Logging is set up in config.py

env_frontend_url_setting = os.environ.get('FRONTEND_URL')
allowed_origins_config = "*" # Default
if env_frontend_url_setting and env_frontend_url_setting.strip() == "*":
    allowed_origins_config = "*"
elif env_frontend_url_setting:
    allowed_origins_config = [url.strip() for url in env_frontend_url_setting.split(',')]
else:
    logging.warning("FRONTEND_URL environment variable not set. Defaulting CORS to allow all origins ('*'). "
                    "For production, it's recommended to set FRONTEND_URL to your specific frontend domain(s).")

CORS(app, 
     origins=allowed_origins_config, 
     supports_credentials=True, 
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins_config}")

# Initialize JWT with the app object
jwt.init_app(app) # jwt instance from extensions.py
logging.info("Flask-JWT-Extended initialized.")

# Initialize LoginManager with the app object
login_manager.init_app(app) # login_manager instance from extensions.py
login_manager.login_view = 'auth.login' # Make sure 'auth.login' is correct after blueprint setup
login_manager.login_message = u"Please log in to access this page."
login_manager.login_message_category = "info"
logging.info("Flask-Login initialized.")

# Global state variables are now imported from extensions.py, no need to redefine here
# upload_progress_data and download_prep_data are already imported.
logging.info("Global state variables (from extensions.py) are available.")


# --- Import and Register Blueprints ---

from routes.password_reset_routes import password_reset_bp 
from routes.auth_routes import auth_bp                     
from routes.upload_routes import upload_bp                 
from routes.download_routes import download_bp             
from routes.file_routes import file_bp 

if 'password_reset' not in app.blueprints:
    app.register_blueprint(password_reset_bp)
    logging.info("Password Reset Blueprint registered.")
else:
    logging.warning("Blueprint 'password_reset' was already registered. Skipping re-registration.")

if 'auth' not in app.blueprints:
    app.register_blueprint(auth_bp)
    logging.info("Auth Blueprint registered.")
else:
    logging.warning("Blueprint 'auth' was already registered. Skipping re-registration.")

if 'upload' not in app.blueprints:
    app.register_blueprint(upload_bp)
    logging.info("Upload Blueprint registered.")
else:
    logging.warning("Blueprint 'upload' was already registered. Skipping re-registration.")

if 'download' not in app.blueprints:
    app.register_blueprint(download_bp)
    logging.info("Download Blueprint registered.")
else:
    logging.warning("Blueprint 'download' was already registered. Skipping re-registration.")

if 'file' not in app.blueprints:
    app.register_blueprint(file_bp)
    logging.info("File Blueprint registered.")
else:
    logging.warning("Blueprint 'file' was already registered. Skipping re-registration.")

# ... (rest of your app_setup.py, e.g., Angular serving routes, if __name__ == '__main__')

# --- Application Runner ---
if __name__ == '__main__':
    logging.info("Starting Flask development server...")
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)