# --- Imports ---
import os
import logging
from datetime import datetime
import pytz 

# --- Configuration ---
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' # Replace with your actual token
TELEGRAM_CHAT_IDS = ['-4603853425','-1002614397019','-4641852757'] 
PRIMARY_TELEGRAM_CHAT_ID = TELEGRAM_CHAT_IDS[0] if TELEGRAM_CHAT_IDS else None
METADATA_FILE = 'metadata.json'
CHUNK_SIZE = 20 * 1024 * 1024  

# --- Logging and Temp Directories ---
LOG_DIR = "Selenium-Logs"
UPLOADS_TEMP_DIR = "uploads_temp"

# --- Ensure Log Directory Exists ---
if not os.path.exists(LOG_DIR):
    try:
        os.makedirs(LOG_DIR)
        
        print(f"Created log directory: {LOG_DIR}")
    except OSError as e:
        print(f"Could not create log directory {LOG_DIR}: {e}")
        

# --- Ensure Temp Directory Exists ---
if not os.path.exists(UPLOADS_TEMP_DIR):
    try:
        os.makedirs(UPLOADS_TEMP_DIR)
        print(f"Created temporary upload directory: {UPLOADS_TEMP_DIR}")
    except OSError as e:
        print(f"Could not create temporary upload directory {UPLOADS_TEMP_DIR}: {e}")

# --- Logging Setup ---
log_filename = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler() 
    ],
    force=True
)
logging.info("Logging configured. Log file: %s", log_filename)


if 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN or 'YOUR_CHAT_ID' in TELEGRAM_CHAT_IDS:
    logging.warning("="*60)
    logging.warning("!!! WARNING: Please replace placeholder BOT TOKEN and CHAT ID !!!")
    logging.warning("="*60)

logging.info("Configuration loaded.")