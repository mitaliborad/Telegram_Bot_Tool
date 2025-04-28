# --- Imports ---
from flask import Flask
import logging
from flask_cors import CORS # <--- IMPORT CORS HERE
from typing import Dict, Any
# --- Import Utilities & Filters ---
# Ensure utils is imported if its filters/functions are needed during setup
# (format_bytes is registered here)
from utils import format_bytes

# --- Flask Application Setup ---

# Initialize Flask app
# Assuming templates are in the root directory ('.') where app is run
app = Flask(__name__, template_folder='.')

# Secret key for session management (e.g., flash messages)
# TODO: Replace with a strong, environment-specific secret key in production
app.secret_key = 'a_simple_secret_key_for_now'

# Register custom Jinja filters
app.jinja_env.filters['format_bytes'] = format_bytes
logging.info("Custom Jinja filter 'format_bytes' registered.")


# --- CORS Configuration ---
# Allow requests from your Angular app's origin
# TODO: Replace 'http://localhost:4200' with your Angular app's actual URL in production
# Use "*" for development testing only if necessary, as it's less secure.
allowed_origins = "http://localhost:4200" # Or use "*" for testing
# allowed_origins = "*"

# Initialize CORS with the app and specific origins
CORS(app, resources={r"/*": {"origins": allowed_origins}})
logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins}")
# --- END CORS Configuration ---


logging.info("Flask application initialized.")

# Stores progress data for ongoing uploads, keyed by upload_id
upload_progress_data: Dict[str, Any] = {}

# Stores state for download preparation tasks, keyed by prep_id
download_prep_data: Dict[str, Any] = {}

logging.info("Global state variables initialized (upload_progress_data, download_prep_data).")
import logging
import os # Needed for checking directories before running

# --- Import App and Config ---
from app_setup import app
# Import routes to ensure they are registered with the app
import routes
# Import directories from config to check them here before running
# Alternatively, these checks could remain entirely within config.py
# from config import LOG_DIR, UPLOADS_TEMP_DIR # Checks now done robustly in config

# --- Application Runner ---
if __name__ == '__main__':
    # Directory checks are now handled more robustly within config.py
    # during initial loading. If config loading fails, the app won't start.

    logging.info("Starting Flask development server...")
    # Use host='0.0.0.0' to make it accessible on your network
    # debug=True is useful for development (enables auto-reloading, debugger),
    # but MUST be False in production for security and performance.
    # Set use_reloader=False if auto-reloading causes issues with threads/state.
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True) # Reloading enabled