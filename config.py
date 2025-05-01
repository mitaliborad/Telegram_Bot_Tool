import os
import logging
from datetime import datetime

# --- Configuration ---
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' 
TELEGRAM_CHAT_IDS = ['-4603853425'] 
#,'-1002614397019','-4641852757'

# --- Critical Check ---
if not TELEGRAM_CHAT_IDS: raise ValueError("Config Error: TELEGRAM_CHAT_IDS empty.")
if not TELEGRAM_BOT_TOKEN or 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN: raise ValueError("Config Error: TELEGRAM_BOT_TOKEN not set.")

PRIMARY_TELEGRAM_CHAT_ID = str(TELEGRAM_CHAT_IDS[0]) if TELEGRAM_CHAT_IDS else None
CHUNK_SIZE = 20 * 1024 * 1024

# --- Telegram API Settings ---
TELEGRAM_API_TIMEOUTS = { 'connect': 10, 'read': 60, 'send_document': 1000, 'get_file': 30, 'download_file': 180 }
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
            logging.info(f"Created directory: {dir_path}")
        except OSError as e:
            logging.error(f"Error creating directory {dir_path}: {e}", exc_info=True) 
            
# --- Logging Setup ---
log_filename = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] [%(funcName)s] - %(message)s'
log_level = logging.INFO 

logging.basicConfig(
    level=log_level,
    format=log_format,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler() 
    ],
    force=True 
)

def format_bytes(size_bytes):
    """Converts bytes to a human-readable format (KB, MB, GB)."""
    if size_bytes is None or size_bytes < 0:
        return "N/A"
    if size_bytes == 0:
        return "0 B"
    try:
        
        if size_bytes == 0: return "0 B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(max(1, size_bytes), 1024)))
        i = min(i, len(size_name) - 1)
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"
    except Exception as e:
        logging.warning(f"Error formatting bytes ({size_bytes}): {e}")
        return f"{size_bytes} B" 

logging.info("="*60)
logging.info(f"Logging configured. Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}. Log file: {log_filename}")
if PRIMARY_TELEGRAM_CHAT_ID: 
    logging.info(f"Primary Chat ID: {PRIMARY_TELEGRAM_CHAT_ID}")
logging.info(f"All Target Chat IDs: {TELEGRAM_CHAT_IDS}")
logging.info(f"Chunk Size: {format_bytes(CHUNK_SIZE)} ({CHUNK_SIZE} bytes)") 
logging.info(f"API Timeouts (Connect/Read): {TELEGRAM_API_TIMEOUTS['connect']}s / {TELEGRAM_API_TIMEOUTS['read']}s")
logging.info(f"API Retries: {API_RETRY_ATTEMPTS} attempts, {API_RETRY_DELAY}s delay")
logging.info(f"Max Upload Workers: {MAX_UPLOAD_WORKERS}")
logging.info(f"Max Download Workers: {MAX_DOWNLOAD_WORKERS}")
logging.info("="*60)


logging.info("Configuration loaded successfully.")

import math

def format_time(seconds):
    """Converts seconds into HH:MM:SS or MM:SS string."""
    if not isinstance(seconds, (int, float)) or seconds < 0 or not math.isfinite(seconds): 
        logging.debug(f"Invalid input to format_time: {seconds}. Returning '--:--'.")
        return "--:--"
    seconds = int(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"

