# --- START OF FILE config.py ---

# --- Imports ---
import os
import logging
from datetime import datetime
# import pytz # Not used directly here, can remove if not needed elsewhere later

# --- Configuration ---
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' # Consider loading from environment
TELEGRAM_CHAT_IDS = ['-4603853425'] # Consider loading from environment or DB
#,'-1002614397019','-4641852757'

# --- Critical Check ---
if not TELEGRAM_CHAT_IDS: raise ValueError("Config Error: TELEGRAM_CHAT_IDS empty.")
if not TELEGRAM_BOT_TOKEN or 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN: raise ValueError("Config Error: TELEGRAM_BOT_TOKEN not set.")

PRIMARY_TELEGRAM_CHAT_ID = str(TELEGRAM_CHAT_IDS[0]) if TELEGRAM_CHAT_IDS else None # Handle empty list case
# METADATA_FILE = 'metadata.json' # <<< REMOVED THIS LINE
CHUNK_SIZE = 20 * 1024 * 1024

# --- Telegram API Settings ---
TELEGRAM_API_TIMEOUTS = { 'connect': 10, 'read': 60, 'send_document': 300, 'get_file': 30, 'download_file': 180 }
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 2

# --- Concurrency Settings ---
MAX_UPLOAD_WORKERS = min(len(TELEGRAM_CHAT_IDS) + 2, (os.cpu_count() or 1) * 2 + 4) if TELEGRAM_CHAT_IDS else 1
MAX_DOWNLOAD_WORKERS = (os.cpu_count() or 1) * 2 + 4

# --- Directory Settings ---
LOG_DIR = "Selenium-Logs"
UPLOADS_TEMP_DIR = "uploads_temp"

# --- Ensure Directories Exist ---
for dir_path in [LOG_DIR, UPLOADS_TEMP_DIR]:
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            # Use logging instead of print for consistency
            logging.info(f"Created directory: {dir_path}")
        except OSError as e:
            logging.error(f"Error creating directory {dir_path}: {e}", exc_info=True) # Log exception info
            # Depending on severity, you might want to exit the application here
            # exit(f"Failed to create essential directory: {dir_path}")

# --- Logging Setup ---
log_filename = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] [%(funcName)s] - %(message)s'
log_level = logging.INFO # Consider making this configurable (e.g., via environment variable)

logging.basicConfig(
    level=log_level,
    format=log_format,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler() # Outputs logs to console as well
    ],
    force=True # Useful if logging might be configured elsewhere (e.g., Flask default)
)

# Add a simple utility function directly here or keep in utils.py
# This is from utils.py originally, moved here since it's used by logging above
def format_bytes(size_bytes):
    """Converts bytes to a human-readable format (KB, MB, GB)."""
    if size_bytes is None or size_bytes < 0:
        return "N/A"
    if size_bytes == 0:
        return "0 B"
    try:
        # Handle potential math domain error for log(0)
        if size_bytes == 0: return "0 B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        # Use max(0, ...) to prevent log of value <= 0
        i = int(math.floor(math.log(max(1, size_bytes), 1024)))
        # Ensure i doesn't exceed tuple bounds
        i = min(i, len(size_name) - 1)
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"
    except Exception as e:
        logging.warning(f"Error formatting bytes ({size_bytes}): {e}")
        return f"{size_bytes} B" 

logging.info("="*60)
logging.info(f"Logging configured. Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}. Log file: {log_filename}")
if PRIMARY_TELEGRAM_CHAT_ID: # Check if it was set
    logging.info(f"Primary Chat ID: {PRIMARY_TELEGRAM_CHAT_ID}")
logging.info(f"All Target Chat IDs: {TELEGRAM_CHAT_IDS}")
logging.info(f"Chunk Size: {format_bytes(CHUNK_SIZE)} ({CHUNK_SIZE} bytes)") # Use format_bytes here too
logging.info(f"API Timeouts (Connect/Read): {TELEGRAM_API_TIMEOUTS['connect']}s / {TELEGRAM_API_TIMEOUTS['read']}s")
logging.info(f"API Retries: {API_RETRY_ATTEMPTS} attempts, {API_RETRY_DELAY}s delay")
logging.info(f"Max Upload Workers: {MAX_UPLOAD_WORKERS}")
logging.info(f"Max Download Workers: {MAX_DOWNLOAD_WORKERS}")
logging.info("="*60)


logging.info("Configuration loaded successfully.")

# Need math import for format_bytes if moved here
import math

def format_time(seconds):
    """Converts seconds into HH:MM:SS or MM:SS string."""
    if not isinstance(seconds, (int, float)) or seconds < 0 or not math.isfinite(seconds): # Added type/finite check
        logging.debug(f"Invalid input to format_time: {seconds}. Returning '--:--'.")
        return "--:--"
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"

