import os
import logging
from datetime import datetime
import math
from flask import Flask
from flask_mail import Mail
import urllib.parse
from dotenv import load_dotenv # << ADD THIS AT THE TOP OF config.py
load_dotenv()

# TELEGRAM_MAX_SINGLE_FILE_UPLOAD_SIZE_BYTES = 1900 * 1024 * 1024
# ANONYMOUS_UPLOAD_LIMIT_BYTES = 5 * 1024 * 1024 * 1024
# FILE_BUFFER_RW_SIZE = 10 * 1024 * 1024

# --- Configuration ---
TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' 
TELEGRAM_CHAT_IDS = ['-4641852757'] 
#-4603853425','-1002614397019',
# --- Critical Check ---
if not TELEGRAM_CHAT_IDS: raise ValueError("Config Error: TELEGRAM_CHAT_IDS empty.")
if not TELEGRAM_BOT_TOKEN or 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN: raise ValueError("Config Error: TELEGRAM_BOT_TOKEN not set.")
PRIMARY_TELEGRAM_CHAT_ID = str(TELEGRAM_CHAT_IDS[0]) if TELEGRAM_CHAT_IDS else None
CHUNK_SIZE = 20 * 1024 * 1024
TELEGRAM_MAX_CHUNK_SIZE_BYTES = 18 * 1024 * 1024

# --- Telegram API Settings ---
TELEGRAM_API_TIMEOUTS = { 'connect': 10, 'read': 60, 'send_document': 600, 'get_file': 180, 'download_file': 600 }
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 2

# --- Concurrency Settings ---
MAX_UPLOAD_WORKERS = min(len(TELEGRAM_CHAT_IDS) + 2, (os.cpu_count() or 1) * 2 + 4) if TELEGRAM_CHAT_IDS else 1
MAX_DOWNLOAD_WORKERS = (os.cpu_count() or 1) * 2 + 4

# --- Directory Settings ---
LOG_DIR = "Selenium-Logs"
UPLOADS_TEMP_DIR = "uploads_temp"

ATLAS_USER = os.getenv("ATLAS_USER")
ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD")
ATLAS_CLUSTER_HOST = os.getenv("ATLAS_CLUSTER_HOST")
encoded_user = urllib.parse.quote_plus(ATLAS_USER)
encoded_password = urllib.parse.quote_plus(ATLAS_PASSWORD)
cluster_host_for_uri = ATLAS_CLUSTER_HOST if ATLAS_CLUSTER_HOST else "your.default.cluster.host.for.error"


app = Flask(__name__, template_folder='templates')
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = False

app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY') or 'a-default-fallback-secret-key-for-dev-ONLY' # CHANGE THIS!
MONGO_URI = f"mongodb+srv://{encoded_user}:{encoded_password}@{ATLAS_CLUSTER_HOST}/?retryWrites=true&w=majority&appName=Telegrambot"
app.config['MONGO_URI'] = MONGO_URI 

# --- Flask-Mail Configuration ---
app.config['MAIL_SERVER'] = os.environ.get('MAIL_SERVER', 'smtp-relay.brevo.com')
app.config['MAIL_PORT'] = int(os.environ.get('MAIL_PORT', 587)) # Use 587 for TLS
app.config['MAIL_USE_TLS'] = os.environ.get('MAIL_USE_TLS', 'true').lower() in ['true', '1', 't'] # Should be True for port 587
app.config['MAIL_USE_SSL'] = False # Explicitly set to False if using TLS on 587

# This is usually your Brevo account email address
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME_BREVO') # Your Brevo login email

# This is the SMTP Key you get from Brevo dashboard
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD_BREVO') # Your Brevo SMTP Key

# The sender email address. This email MUST be validated/verified within your Brevo account.
# Brevo will likely only let you send from addresses you've proven you own.
app.config['MAIL_DEFAULT_SENDER'] = (
    os.environ.get('MAIL_SENDER_NAME', 'Your App Name'), # e.g., "Telegram Bot Tool"
    os.environ.get('MAIL_SENDER_EMAIL_BREVO')          # e.g., "no-reply@yourdomain.com" or your verified Brevo sender
)

# --- Initialize Flask-Mail ---
mail = Mail(app) # Initialize Mail with your app instance

# --- Password Reset Token Expiration (Optional, defaults below are reasonable) ---
# Set the maximum age for the password reset token in seconds (e.g., 1 hour = 3600 seconds)
app.config['PASSWORD_RESET_TOKEN_MAX_AGE'] = int(os.environ.get('PASSWORD_RESET_TOKEN_MAX_AGE', 3600))


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
        logging.StreamHandler() # Outputs logs to console as well
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
logging.info(f"Chunk Size: {format_bytes(CHUNK_SIZE)} ({CHUNK_SIZE} bytes)") # Use format_bytes here too
logging.info(f"API Timeouts (Connect/Read): {TELEGRAM_API_TIMEOUTS['connect']}s / {TELEGRAM_API_TIMEOUTS['read']}s")
logging.info(f"API Retries: {API_RETRY_ATTEMPTS} attempts, {API_RETRY_DELAY}s delay")
logging.info(f"Max Upload Workers: {MAX_UPLOAD_WORKERS}")
logging.info(f"Max Download Workers: {MAX_DOWNLOAD_WORKERS}")
logging.info("="*60)


logging.info("Configuration loaded successfully.")
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

