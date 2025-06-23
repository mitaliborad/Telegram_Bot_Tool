import os
import logging
from datetime import datetime
import math
from flask import Flask
from flask_mail import Mail
import urllib.parse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Directory Settings (defined early for logging setup) ---
UPLOADS_TEMP_DIR = "uploads_temp"

# --- Logging Setup ---
app_env = os.environ.get('APP_ENV', 'development').lower()
log_level = logging.DEBUG if app_env == 'development' else logging.INFO
log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] [%(module)s:%(lineno)d] [%(funcName)s] - %(message)s'

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.StreamHandler() 
    ],
    force=True 
)
#logging.info(f"Console-only logging configured from config.py. Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}.")

app_logger = logging.getLogger('app_setup')
app_logger.setLevel(log_level)

logging.info(f"Root logging level set to INFO. App-specific logging level set to {logging.getLevelName(log_level)}.")

# --- Application Constants & Settings ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'YOUR_BOT_TOKEN_DEFAULT_IF_NOT_SET')
TELEGRAM_CHAT_IDS_STR = os.getenv('TELEGRAM_CHAT_IDS', '-1002801989208') 
TELEGRAM_CHAT_IDS = [chat_id.strip() for chat_id in TELEGRAM_CHAT_IDS_STR.split(',') if chat_id.strip()]

# --- Critical Checks ---
if not TELEGRAM_CHAT_IDS:
    logging.critical("Config Error: TELEGRAM_CHAT_IDS is empty or not set correctly in environment variables.")
    raise ValueError("Config Error: TELEGRAM_CHAT_IDS empty.")
if not TELEGRAM_BOT_TOKEN or 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN:
    logging.critical("Config Error: TELEGRAM_BOT_TOKEN not set or using a placeholder value.")
    raise ValueError("Config Error: TELEGRAM_BOT_TOKEN not set.")

PRIMARY_TELEGRAM_CHAT_ID = str(TELEGRAM_CHAT_IDS[0])
CHUNK_SIZE = 20 * 1024 * 1024  
TELEGRAM_MAX_CHUNK_SIZE_BYTES = 18 * 1024 * 1024 

# --- Telegram API Settings ---
TELEGRAM_API_TIMEOUTS = {
    'connect': 20,         
    'read': 90,            
    'send_document': 600,  
    'get_file': 300,        
    'download_file': 600    
}
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 5  

# --- Concurrency Settings ---
MAX_UPLOAD_WORKERS = min(len(TELEGRAM_CHAT_IDS) + 2, (os.cpu_count() or 1) * 2 + 4)
MAX_DOWNLOAD_WORKERS = 4  

# --- Flask Application Initialization ---
app = Flask(__name__, template_folder='templates')

# --- Security and Session Configuration ---
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'a-very-strong-default-secret-key-for-dev-only')
if app.config['SECRET_KEY'] == 'a-very-strong-default-secret-key-for-dev-only':
    logging.warning("SECURITY WARNING: Using default FLASK_SECRET_KEY. Set a strong, unique key in your environment for production.")

# --- JWT Configuration ---
app.config["JWT_SECRET_KEY"] = os.environ.get('JWT_SECRET_KEY', 'a-default-jwt-secret-key-CHANGE-THIS')
if app.config["JWT_SECRET_KEY"] == 'a-default-jwt-secret-key-CHANGE-THIS':
    logging.warning("SECURITY WARNING: Using default JWT_SECRET_KEY. Set a strong, unique key in your environment.")
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = False 

# --- Database Configuration ---
ATLAS_USER = os.getenv("ATLAS_USER")
ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD")
ATLAS_CLUSTER_HOST = os.getenv("ATLAS_CLUSTER_HOST")

if not all([ATLAS_USER, ATLAS_PASSWORD, ATLAS_CLUSTER_HOST]):
    logging.critical("Database configuration error: ATLAS_USER, ATLAS_PASSWORD, or ATLAS_CLUSTER_HOST not set.")
    MONGO_URI = None 
else:
    encoded_user = urllib.parse.quote_plus(ATLAS_USER)
    encoded_password = urllib.parse.quote_plus(ATLAS_PASSWORD)
    MONGO_URI = f"mongodb+srv://{encoded_user}:{encoded_password}@{ATLAS_CLUSTER_HOST}/?retryWrites=true&w=majority&appName=Telegrambot"

app.config['MONGO_URI'] = MONGO_URI
if not MONGO_URI:
    logging.warning("MONGO_URI is not set due to missing Atlas credentials. Database functionalities will be unavailable.")


# --- Flask-Mail Configuration ---
app.config['MAIL_SERVER'] = os.environ.get('MAIL_SERVER', 'smtp-relay.brevo.com')
app.config['MAIL_PORT'] = int(os.environ.get('MAIL_PORT', 587))
app.config['MAIL_USE_TLS'] = os.environ.get('MAIL_USE_TLS', 'true').lower() in ['true', '1', 't']
app.config['MAIL_USE_SSL'] = os.environ.get('MAIL_USE_SSL', 'false').lower() in ['true', '1', 't'] 
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME_BREVO')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD_BREVO')
app.config['MAIL_DEFAULT_SENDER'] = (
    os.environ.get('MAIL_SENDER_NAME', 'Your App Name'),
    os.environ.get('MAIL_SENDER_EMAIL_BREVO') 
)
app.config['MAIL_SENDER_EMAIL_BREVO'] = os.environ.get('MAIL_SENDER_EMAIL_BREVO') 

# Validate essential mail config
if not all([app.config['MAIL_USERNAME'], app.config['MAIL_PASSWORD'], app.config['MAIL_DEFAULT_SENDER'][1]]):
    logging.warning("Mail configuration is incomplete. Email functionalities may not work.")

mail = Mail(app)

# --- Password Reset Token Expiration ---
app.config['PASSWORD_RESET_TOKEN_MAX_AGE'] = int(os.environ.get('PASSWORD_RESET_TOKEN_MAX_AGE', 86400)) 

if not os.path.exists(UPLOADS_TEMP_DIR):
    try:
        os.makedirs(UPLOADS_TEMP_DIR)
        logging.info(f"Created directory: {UPLOADS_TEMP_DIR}")
    except OSError as e:
        logging.error(f"Error creating directory {UPLOADS_TEMP_DIR}: {e}", exc_info=True)

# --- Utility Functions (can be here or in a separate utils.py) ---
def format_bytes(size_bytes):
    """Converts bytes to a human-readable format (KB, MB, GB)."""
    if size_bytes is None or not isinstance(size_bytes, (int, float)) or size_bytes < 0:
        return "N/A"
    if size_bytes == 0:
        return "0 B"
    try:
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(max(1, size_bytes), 1024)))
        i = min(i, len(size_name) - 1) 
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"
    except (ValueError, TypeError, OverflowError) as e: 
        logging.warning(f"Error formatting bytes ({size_bytes}): {e}")
        return f"{size_bytes} B (Error)"

def format_time(seconds):
    """Converts seconds into HH:MM:SS or MM:SS string."""
    if not isinstance(seconds, (int, float)) or seconds < 0 or not math.isfinite(seconds):
        logging.debug(f"Invalid input to format_time: {seconds}. Returning '--:--'.")
        return "--:--"
    seconds_int = int(round(seconds)) 
    minutes, sec = divmod(seconds_int, 60)
    hours, min = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{min:02d}:{sec:02d}"
    else:
        return f"{min:02d}:{sec:02d}"

# --- Log Final Configuration Summary ---
logging.info("="*60)
logging.info("Application Configuration Summary (from config.py):")
logging.info(f"  Primary Telegram Chat ID: {PRIMARY_TELEGRAM_CHAT_ID}")
logging.info(f"  All Target Chat IDs: {TELEGRAM_CHAT_IDS}")
logging.info(f"  Chunk Size for Uploads: {format_bytes(CHUNK_SIZE)} ({CHUNK_SIZE} bytes)")
logging.info(f"  Max Telegram Chunk Size: {format_bytes(TELEGRAM_MAX_CHUNK_SIZE_BYTES)}")
logging.info(f"  API Timeouts (Connect/Read): {TELEGRAM_API_TIMEOUTS['connect']}s / {TELEGRAM_API_TIMEOUTS['read']}s")
logging.info(f"  API Retries: {API_RETRY_ATTEMPTS} attempts, {API_RETRY_DELAY}s delay")
logging.info(f"  Max Upload Workers: {MAX_UPLOAD_WORKERS}")
logging.info(f"  Max Download Workers: {MAX_DOWNLOAD_WORKERS}")
logging.info(f"  Flask Secret Key: {'SET' if app.config['SECRET_KEY'] != 'a-very-strong-default-secret-key-for-dev-only' else 'USING DEFAULT (INSECURE)'}")
logging.info(f"  JWT Secret Key: {'SET' if app.config['JWT_SECRET_KEY'] != 'a-default-jwt-secret-key-CHANGE-THIS' else 'USING DEFAULT (INSECURE)'}")
logging.info(f"  Mongo URI: {'SET' if app.config['MONGO_URI'] else 'NOT SET (DB issues expected)'}")
logging.info(f"  Mail Server: {app.config['MAIL_SERVER']}:{app.config['MAIL_PORT']}")
logging.info(f"  Mail Default Sender: {app.config['MAIL_DEFAULT_SENDER']}")
logging.info("="*60)

logging.info("config.py loaded and initialized successfully.")