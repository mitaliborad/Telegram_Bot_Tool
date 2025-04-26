# --- Imports ---
from flask import Flask
import logging

# --- Import Utilities ---
from utils import format_bytes

# --- Flask Application Setup ---
app = Flask(__name__, template_folder='.') # Assuming templates are in the root
app.secret_key = 'a_simple_secret_key_for_now' # Important for flashing messages

# Register the custom filter with Jinja
app.jinja_env.filters['format_bytes'] = format_bytes
logging.info("Flask application initialized.")

# --- Global State Variables ---
# These might be better managed with Flask contexts or dedicated state managers
# in a larger application, but for direct translation, we keep them global here.
upload_progress_data = {}
download_prep_data = {}

logging.info("Global state variables initialized.")