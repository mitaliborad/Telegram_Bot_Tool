# --- START OF FILE app_setup.py ---

# --- Imports ---
from flask import Flask
import logging
from flask_cors import CORS # <--- IMPORT CORS HERE

# --- Import Utilities ---
from utils import format_bytes

# --- Flask Application Setup ---
app = Flask(__name__, template_folder='.') # Assuming templates are in the root
app.secret_key = 'a_simple_secret_key_for_now' # Important for flashing messages

# --- CORS Configuration ---
# This tells the browser that requests FROM 'http://localhost:4200'
# are allowed to access resources on this Flask server ('http://localhost:5000').
# Replace 'http://localhost:4200' with the actual origin of your Angular app.
# For production, replace it with your deployed Angular app's domain.
# Using "*" allows any origin, which is simpler for initial testing but less secure.
# Use "*" cautiously, especially in production.
allowed_origins = "http://localhost:4200" # Or use "*" for development testing
#allowed_origins = "*" 

# Initialize CORS with the app and specific origins
CORS(app, resources={r"/*": {"origins": allowed_origins}})
# The 'resources={r"/*": ...}' part applies CORS to all routes (paths starting with /)
# The {"origins": ...} specifies which frontend origins are allowed to make requests.

logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins}")
# --- END CORS Configuration ---

# Register the custom filter with Jinja
app.jinja_env.filters['format_bytes'] = format_bytes
logging.info("Flask application initialized.")

# --- Global State Variables ---
# These might be better managed with Flask contexts or dedicated state managers
# in a larger application, but for direct translation, we keep them global here.
upload_progress_data = {}
download_prep_data = {}

logging.info("Global state variables initialized.")
