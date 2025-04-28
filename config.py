# --- Imports ---
import os
import logging
from datetime import datetime
import pytz
# --- REMOVED: from utils import format_bytes --- # <-- REMOVE THIS IMPORT

# --- Configuration ---
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' # Replace with your actual token
TELEGRAM_CHAT_IDS = ['-4603853425','-1002614397019','-4641852757'] # Example: ['-1001234567890', '@my_channel']

# --- Critical Check ---
if not TELEGRAM_CHAT_IDS: raise ValueError("Config Error: TELEGRAM_CHAT_IDS empty.")
if not TELEGRAM_BOT_TOKEN or 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN: raise ValueError("Config Error: TELEGRAM_BOT_TOKEN not set.")

PRIMARY_TELEGRAM_CHAT_ID = str(TELEGRAM_CHAT_IDS[0])
METADATA_FILE = 'metadata.json'
CHUNK_SIZE = 20 * 1024 * 1024 # 20 MiB

# --- Telegram API Settings ---
TELEGRAM_API_TIMEOUTS = { 'connect': 10, 'read': 60, 'send_document': 120, 'get_file': 30, 'download_file': 180 }
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 2

# --- Concurrency Settings ---
MAX_UPLOAD_WORKERS = min(len(TELEGRAM_CHAT_IDS) + 2, (os.cpu_count() or 1) * 2 + 4) if TELEGRAM_CHAT_IDS else 1
MAX_DOWNLOAD_WORKERS = (os.cpu_count() or 1) * 2 + 4

# --- Directory Settings ---
LOG_DIR = "Selenium-Logs"
UPLOADS_TEMP_DIR = "uploads_temp"

# --- Ensure Directories Exist ---
# NOTE: Cannot use format_bytes here due to import cycle avoidance
for dir_path in [LOG_DIR, UPLOADS_TEMP_DIR]:
    if not os.path.exists(dir_path):
        try: os.makedirs(dir_path); print(f"Created directory: {dir_path}")
        except OSError as e: print(f"Error creating directory {dir_path}: {e}") # Consider logging/exiting

# --- Logging Setup ---
log_filename = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] [%(funcName)s] - %(message)s'
log_level = logging.INFO

logging.basicConfig(
    level=log_level,
    format=log_format,
    handlers=[ logging.FileHandler(log_filename, encoding='utf-8'), logging.StreamHandler() ],
    force=True
)
logging.info("="*60)
logging.info(f"Logging configured. Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}. Log file: {log_filename}")
logging.info(f"Primary Chat ID: {PRIMARY_TELEGRAM_CHAT_ID}")
logging.info(f"All Target Chat IDs: {TELEGRAM_CHAT_IDS}")
# --- MODIFIED: Log chunk size without format_bytes ---
logging.info(f"Chunk Size: {CHUNK_SIZE} bytes") # <-- MODIFIED THIS LINE
logging.info(f"API Timeouts (Connect/Read): {TELEGRAM_API_TIMEOUTS['connect']}s / {TELEGRAM_API_TIMEOUTS['read']}s")
logging.info(f"API Retries: {API_RETRY_ATTEMPTS} attempts, {API_RETRY_DELAY}s delay")
logging.info(f"Max Upload Workers: {MAX_UPLOAD_WORKERS}")
logging.info(f"Max Download Workers: {MAX_DOWNLOAD_WORKERS}")
logging.info("="*60)

# --- Configuration Loaded ---
logging.info("Configuration loaded successfully.")