# import os
# import logging
# from datetime import datetime
# import math
# from flask import Flask
# from flask_mail import Mail
# import urllib.parse
# from dotenv import load_dotenv # << ADD THIS AT THE TOP OF config.py
# load_dotenv()

# # TELEGRAM_MAX_SINGLE_FILE_UPLOAD_SIZE_BYTES = 1900 * 1024 * 1024
# # ANONYMOUS_UPLOAD_LIMIT_BYTES = 5 * 1024 * 1024 * 1024
# # FILE_BUFFER_RW_SIZE = 10 * 1024 * 1024

# # --- Configuration ---
# TELEGRAM_BOT_TOKEN = '7812479394:AAFrzOcHGKfc-1iOUbVEkptJkooaJrXHAxs' 
# TELEGRAM_CHAT_IDS = ['-4641852757'] 
# #-4603853425','-1002614397019',
# # --- Critical Check ---
# if not TELEGRAM_CHAT_IDS: raise ValueError("Config Error: TELEGRAM_CHAT_IDS empty.")
# if not TELEGRAM_BOT_TOKEN or 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN: raise ValueError("Config Error: TELEGRAM_BOT_TOKEN not set.")
# PRIMARY_TELEGRAM_CHAT_ID = str(TELEGRAM_CHAT_IDS[0]) if TELEGRAM_CHAT_IDS else None
# CHUNK_SIZE = 20 * 1024 * 1024
# TELEGRAM_MAX_CHUNK_SIZE_BYTES = 18 * 1024 * 1024

# # --- Telegram API Settings ---
# TELEGRAM_API_TIMEOUTS = { 'connect': 20, 'read': 90, 'send_document': 600, 'get_file': 300, 'download_file': 600 }
# API_RETRY_ATTEMPTS = 3
# API_RETRY_DELAY = 5

# # --- Concurrency Settings ---
# MAX_UPLOAD_WORKERS = min(len(TELEGRAM_CHAT_IDS) + 2, (os.cpu_count() or 1) * 2 + 4) if TELEGRAM_CHAT_IDS else 1
# MAX_DOWNLOAD_WORKERS = 4

# # --- Directory Settings ---
# LOG_DIR = "Selenium-Logs"
# UPLOADS_TEMP_DIR = "uploads_temp"

# ATLAS_USER = os.getenv("ATLAS_USER")
# ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD")
# ATLAS_CLUSTER_HOST = os.getenv("ATLAS_CLUSTER_HOST")
# encoded_user = urllib.parse.quote_plus(ATLAS_USER)
# encoded_password = urllib.parse.quote_plus(ATLAS_PASSWORD)
# cluster_host_for_uri = ATLAS_CLUSTER_HOST if ATLAS_CLUSTER_HOST else "your.default.cluster.host.for.error"


# app = Flask(__name__, template_folder='templates')
# app.config["JWT_ACCESS_TOKEN_EXPIRES"] = False

# app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY') or 'a-default-fallback-secret-key-for-dev-ONLY' 
# MONGO_URI = f"mongodb+srv://{encoded_user}:{encoded_password}@{ATLAS_CLUSTER_HOST}/?retryWrites=true&w=majority&appName=Telegrambot"
# app.config['MONGO_URI'] = MONGO_URI 

# # --- Flask-Mail Configuration ---
# app.config['MAIL_SERVER'] = os.environ.get('MAIL_SERVER', 'smtp-relay.brevo.com')
# app.config['MAIL_PORT'] = int(os.environ.get('MAIL_PORT', 587)) 
# app.config['MAIL_USE_TLS'] = os.environ.get('MAIL_USE_TLS', 'true').lower() in ['true', '1', 't'] 
# app.config['MAIL_USE_SSL'] = False 

# # This is usually your Brevo account email address
# app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME_BREVO') 

# # This is the SMTP Key you get from Brevo dashboard
# app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD_BREVO') 

# # The sender email address. This email MUST be validated/verified within your Brevo account.
# # Brevo will likely only let you send from addresses you've proven you own.
# app.config['MAIL_DEFAULT_SENDER'] = (
#     os.environ.get('MAIL_SENDER_NAME', 'Your App Name'), 
#     os.environ.get('MAIL_SENDER_EMAIL_BREVO')         
# )

# # --- Initialize Flask-Mail ---
# mail = Mail(app) 

# # --- Password Reset Token Expiration (Optional, defaults below are reasonable) ---
# app.config['PASSWORD_RESET_TOKEN_MAX_AGE'] = int(os.environ.get('PASSWORD_RESET_TOKEN_MAX_AGE', 3600))


# # --- Ensure Directories Exist ---
# for dir_path in [LOG_DIR, UPLOADS_TEMP_DIR]:
#     if not os.path.exists(dir_path):
#         try:
#             os.makedirs(dir_path)
            
#             logging.info(f"Created directory: {dir_path}")
#         except OSError as e:
#             logging.error(f"Error creating directory {dir_path}: {e}", exc_info=True) 
           
# # --- Logging Setup ---
# log_filename = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
# log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] [%(funcName)s] - %(message)s'
# log_level = logging.INFO 


# logging.basicConfig(
#     level=log_level,
#     format=log_format,
#     handlers=[
#         logging.FileHandler(log_filename, encoding='utf-8'),
#         logging.StreamHandler() # Outputs logs to console as well
#     ],
#     force=True 
# )

# def format_bytes(size_bytes):
#     """Converts bytes to a human-readable format (KB, MB, GB)."""
#     if size_bytes is None or size_bytes < 0:
#         return "N/A"
#     if size_bytes == 0:
#         return "0 B"
#     try:
        
#         if size_bytes == 0: return "0 B"
#         size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
#         i = int(math.floor(math.log(max(1, size_bytes), 1024)))
#         i = min(i, len(size_name) - 1)
#         p = math.pow(1024, i)
#         s = round(size_bytes / p, 2)
#         return f"{s} {size_name[i]}"
#     except Exception as e:
#         logging.warning(f"Error formatting bytes ({size_bytes}): {e}")
#         return f"{size_bytes} B" 

# logging.info("="*60)
# logging.info(f"Logging configured. Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}. Log file: {log_filename}")
# if PRIMARY_TELEGRAM_CHAT_ID: 
#     logging.info(f"Primary Chat ID: {PRIMARY_TELEGRAM_CHAT_ID}")
# logging.info(f"All Target Chat IDs: {TELEGRAM_CHAT_IDS}")
# logging.info(f"Chunk Size: {format_bytes(CHUNK_SIZE)} ({CHUNK_SIZE} bytes)") 
# logging.info(f"API Timeouts (Connect/Read): {TELEGRAM_API_TIMEOUTS['connect']}s / {TELEGRAM_API_TIMEOUTS['read']}s")
# logging.info(f"API Retries: {API_RETRY_ATTEMPTS} attempts, {API_RETRY_DELAY}s delay")
# logging.info(f"Max Upload Workers: {MAX_UPLOAD_WORKERS}")
# logging.info(f"Max Download Workers: {MAX_DOWNLOAD_WORKERS}")
# logging.info("="*60)


# logging.info("Configuration loaded successfully.")
# import math

# def format_time(seconds):
#     """Converts seconds into HH:MM:SS or MM:SS string."""
#     if not isinstance(seconds, (int, float)) or seconds < 0 or not math.isfinite(seconds): 
#         logging.debug(f"Invalid input to format_time: {seconds}. Returning '--:--'.")
#         return "--:--"
#     seconds = int(seconds)
#     minutes, seconds = divmod(seconds, 60)
#     hours, minutes = divmod(minutes, 60)
#     if hours > 0:
#         return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
#     else:
#         return f"{minutes:02d}:{seconds:02d}"




# config.py
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
LOG_DIR = "Selenium-Logs"
UPLOADS_TEMP_DIR = "uploads_temp"

# --- Ensure LOG_DIR Exists (for logging setup below) ---
# This ensures the directory for log files is available before basicConfig is called.
if not os.path.exists(LOG_DIR):
    try:
        os.makedirs(LOG_DIR)
        # We can't use logging here yet as it's not configured,
        # so a simple print or pass is fine for this bootstrap phase.
        print(f"Bootstrap: Created directory: {LOG_DIR}")
    except OSError as e:
        print(f"Bootstrap: Error creating directory {LOG_DIR}: {e}")
        # If this fails, file logging might fail, but console logging should still work.

# --- Logging Setup ---
# This will be effective when config.py is imported.
# If app_setup.py runs with reloader disabled, this will be the primary logging config.
log_filename = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] [%(module)s:%(lineno)d] [%(funcName)s] - %(message)s'
log_level = logging.INFO

logging.basicConfig(
    level=log_level,
    format=log_format,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()  # Outputs logs to console as well
    ],
    force=True # Override any pre-existing default handlers
)
logging.info(f"Logging configured from config.py. Level: {logging.getLevelName(logging.getLogger().getEffectiveLevel())}. Log file: {log_filename}")

# --- Application Constants & Settings ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'YOUR_BOT_TOKEN_DEFAULT_IF_NOT_SET')
TELEGRAM_CHAT_IDS_STR = os.getenv('TELEGRAM_CHAT_IDS', '-4641852757') # Example default
TELEGRAM_CHAT_IDS = [chat_id.strip() for chat_id in TELEGRAM_CHAT_IDS_STR.split(',') if chat_id.strip()]

# --- Critical Checks ---
if not TELEGRAM_CHAT_IDS:
    logging.critical("Config Error: TELEGRAM_CHAT_IDS is empty or not set correctly in environment variables.")
    raise ValueError("Config Error: TELEGRAM_CHAT_IDS empty.")
if not TELEGRAM_BOT_TOKEN or 'YOUR_BOT_TOKEN' in TELEGRAM_BOT_TOKEN:
    logging.critical("Config Error: TELEGRAM_BOT_TOKEN not set or using a placeholder value.")
    raise ValueError("Config Error: TELEGRAM_BOT_TOKEN not set.")

PRIMARY_TELEGRAM_CHAT_ID = str(TELEGRAM_CHAT_IDS[0])
CHUNK_SIZE = 20 * 1024 * 1024  # 20 MB
TELEGRAM_MAX_CHUNK_SIZE_BYTES = 18 * 1024 * 1024 # Slightly less than Telegram's practical limit per part

# --- Telegram API Settings ---
TELEGRAM_API_TIMEOUTS = {
    'connect': 20,          # Timeout for establishing a connection
    'read': 90,             # Timeout for reading data from a connection
    'send_document': 600,   # Extended timeout for sending documents (parts)
    'get_file': 300,        # Timeout for getFile method
    'download_file': 600    # Timeout for downloading file content
}
API_RETRY_ATTEMPTS = 3
API_RETRY_DELAY = 5  # seconds

# --- Concurrency Settings ---
# Adjust based on the number of chat IDs to distribute uploads/downloads
# Ensure at least 1 worker even if os.cpu_count() is None
MAX_UPLOAD_WORKERS = min(len(TELEGRAM_CHAT_IDS) + 2, (os.cpu_count() or 1) * 2 + 4)
MAX_DOWNLOAD_WORKERS = 4  # Can be tuned based on performance

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
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = False # Or a timedelta object, e.g., timedelta(hours=1)

# --- Database Configuration ---
ATLAS_USER = os.getenv("ATLAS_USER")
ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD")
ATLAS_CLUSTER_HOST = os.getenv("ATLAS_CLUSTER_HOST")

if not all([ATLAS_USER, ATLAS_PASSWORD, ATLAS_CLUSTER_HOST]):
    logging.critical("Database configuration error: ATLAS_USER, ATLAS_PASSWORD, or ATLAS_CLUSTER_HOST not set.")
    # Depending on strictness, you might raise an error here or allow fallback if app can run without DB
    # For now, we proceed, but DB operations will fail.
    MONGO_URI = None # Explicitly set to None if creds are missing
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
app.config['MAIL_USE_SSL'] = os.environ.get('MAIL_USE_SSL', 'false').lower() in ['true', '1', 't'] # Typically false if TLS is true
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME_BREVO')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD_BREVO')
app.config['MAIL_DEFAULT_SENDER'] = (
    os.environ.get('MAIL_SENDER_NAME', 'Your App Name'),
    os.environ.get('MAIL_SENDER_EMAIL_BREVO') # This should be a verified sender email
)
app.config['MAIL_SENDER_EMAIL_BREVO'] = os.environ.get('MAIL_SENDER_EMAIL_BREVO') # For direct use if needed

# Validate essential mail config
if not all([app.config['MAIL_USERNAME'], app.config['MAIL_PASSWORD'], app.config['MAIL_DEFAULT_SENDER'][1]]):
    logging.warning("Mail configuration is incomplete. Email functionalities may not work.")

mail = Mail(app)

# --- Password Reset Token Expiration ---
app.config['PASSWORD_RESET_TOKEN_MAX_AGE'] = int(os.environ.get('PASSWORD_RESET_TOKEN_MAX_AGE', 86400)) # 24 hours

# --- Ensure Other Directories Exist (e.g., UPLOADS_TEMP_DIR) ---
for dir_path in [UPLOADS_TEMP_DIR]: # LOG_DIR already handled
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            logging.info(f"Created directory: {dir_path}")
        except OSError as e:
            logging.error(f"Error creating directory {dir_path}: {e}", exc_info=True)

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
        i = min(i, len(size_name) - 1) # Ensure index is within bounds
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_name[i]}"
    except (ValueError, TypeError, OverflowError) as e: # More specific exceptions
        logging.warning(f"Error formatting bytes ({size_bytes}): {e}")
        return f"{size_bytes} B (Error)"

def format_time(seconds):
    """Converts seconds into HH:MM:SS or MM:SS string."""
    if not isinstance(seconds, (int, float)) or seconds < 0 or not math.isfinite(seconds):
        logging.debug(f"Invalid input to format_time: {seconds}. Returning '--:--'.")
        return "--:--"
    seconds_int = int(round(seconds)) # Round to nearest second then convert to int
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