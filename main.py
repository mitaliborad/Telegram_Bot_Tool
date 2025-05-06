# main.py

import logging
import os

# --- Import App object from app_setup ---
# This will execute the code in app_setup.py once, creating and configuring the app.
from app_setup import app

# --- Import Routes AFTER app is initialized ---
# This registers your blueprints/routes with the app instance.
import routes # Assuming your routes.py defines blueprints or uses @app.route

# --- Application Runner (for local development only) ---
if __name__ == '__main__':
    logging.info("Starting Flask development server (for local use only)...")
    # Load .env for local development if you're not using flask run with --env-file
    try:
        from dotenv import load_dotenv
        load_dotenv()
        logging.info(".env file loaded for local development.")
    except ImportError:
        logging.info("python-dotenv not installed, .env file not loaded automatically for local dev.")

    # Re-check FRONTEND_URL for local dev after .env load
    local_frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:4200")
    logging.info(f"Local dev: Flask-CORS allowing origins (potentially from .env): {local_frontend_url}")


    # The port should ideally also come from an env var for flexibility
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False) # use_reloader=False is often better with debuggers