# --- Imports ---
import logging
import os # Needed for checking directories before running

# --- Import App and Config ---
from app_setup import app
import routes
# Import directories from config to check them here before running
# Alternatively, these checks could remain entirely within config.py
from config import LOG_DIR, UPLOADS_TEMP_DIR

# --- Application Runner ---
if __name__ == '__main__':
    # Ensure Log Directory Exists (Optional check here, already done in config)
    # if not os.path.exists(LOG_DIR):
    #     try:
    #         os.makedirs(LOG_DIR)
    #         logging.info(f"Created log directory: {LOG_DIR}")
    #     except OSError as e:
    #         logging.error(f"Could not create log directory {LOG_DIR}: {e}", exc_info=True)

    # Ensure Temp Directory Exists (Optional check here, already done in config)
    # if not os.path.exists(UPLOADS_TEMP_DIR):
    #     try:
    #         os.makedirs(UPLOADS_TEMP_DIR)
    #         logging.info(f"Created temporary upload directory: {UPLOADS_TEMP_DIR}")
    #     except OSError as e:
    #         logging.error(f"Could not create temporary upload directory {UPLOADS_TEMP_DIR}: {e}", exc_info=True)

    logging.info("Starting Flask development server...")
    # Use host='0.0.0.0' to make it accessible on your network
    # debug=True is useful for development, but should be False in production
    app.run(host='0.0.0.0', port=5000, debug=True) # Consider debug=False for production