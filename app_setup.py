from flask import Flask
import logging
from flask_cors import CORS 
from typing import Dict, Any
from utils import format_bytes

# --- Flask Application Setup ---
app = Flask(__name__, template_folder='.') 
app.secret_key = 'a_simple_secret_key_for_now' 

# Register custom Jinja filters
app.jinja_env.filters['format_bytes'] = format_bytes
logging.info("Custom Jinja filter 'format_bytes' registered.")

allowed_origins = "http://localhost:4200" 
#allowed_origins = "*" 

# Initialize CORS with the app and specific origins
CORS(app, resources={r"/*": {"origins": allowed_origins}})
logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins}")
logging.info("Flask application initialized.") 

# Stores progress data for ongoing uploads, keyed by upload_id
upload_progress_data: Dict[str, Any] = {}

# Stores state for download preparation tasks, keyed by prep_id
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