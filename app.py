import os
import json
import math
import logging
import tempfile
import zipfile
from flask import Flask, request, jsonify, render_template, send_file, abort
from werkzeug.utils import secure_filename
import telegram
import asyncio

bot = None

# --- Configuration ---
TELEGRAM_BOT_TOKEN = "7812479394:AAFhPxoHysfTUf710a7ShQbaSCYi-0r7e7E" 

TELEGRAM_CHAT_ID = -4603853425 
print(f"DEBUG: The value of TELEGRAM_CHAT_ID is {TELEGRAM_CHAT_ID}") 

if TELEGRAM_CHAT_ID == -4603853425:
    print("successfully found")
else:
    print("error getting chat id")


CHUNK_SIZE_MB = 1500 # Split files into chunks of this size (slightly below Telegram's 2GB limit)
CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024
UPLOAD_FOLDER = 'uploads_temp' # Temporary folder for uploads
DOWNLOAD_FOLDER = 'downloads_temp' # Temporary folder for downloads
METADATA_FILE = 'metadata.json'
COMPRESSION_ENABLED = True # Set to False to disable zipping

# --- Flask App Setup ---
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 4 * 1024 * 1024 * 1024 # Example: Allow up to 4GB uploads

# Ensure temporary directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Telegram Bot Setup ---
# try:
#     bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
#     logging.info(f"Telegram Bot initialized. Bot Name: {bot.get_me().username}")
# except Exception as e:
#     logging.error(f"Failed to initialize Telegram Bot: {e}")
#     bot = None # Set bot to None if initialization fails

# --- Metadata Handling ---
# metadata Json read
def load_metadata():
    """Loads metadata from the JSON file."""
    logging.info("Attempting to load metadata...") 
    if not os.path.exists(METADATA_FILE):
        logging.warning(f"Metadata file '{METADATA_FILE}' not found. Returning empty dictionary.") 
        return {}
    try:
        logging.debug(f"Opening metadata file '{METADATA_FILE}' for reading.")
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
            logging.info(f"Successfully loaded and parsed metadata from '{METADATA_FILE}'.")
            logging.debug(f"Loaded metadata type: {type(metadata)}")

            if isinstance(metadata, dict):
                logging.debug(f"Number of top-level keys in metadata: {len(metadata.keys())}")
            return metadata
    except (json.JSONDecodeError, IOError) as e:
        logging.error(f"Error loading metadata: {e}")
        logging.warning("Returning empty dictionary due to JSON decoding error.")
        return {} 
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading metadata from '{METADATA_FILE}': {e}", exc_info=True)
        logging.warning("Returning empty dictionary due to unexpected error.")
        return {}
        
async def initialize_bot(): 
    global bot
    logging.info("Attempting to initialize Telegram Bot...") 
    try:
        temp_bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
        logging.info("Bot instance created. Validating token with get_me()...")
        user = await temp_bot.get_me() 
        logging.info(f"Token validated successfully. Bot Name: {user.username}")
        bot = temp_bot 
        logging.info("Global 'bot' variable assigned.")
        return True
    
    except Exception as e:
        logging.error("Failed to initialize Telegram Bot: Invalid Token provided.")
        bot = None 
        return False
    except Exception as e:
        logging.error(f"Failed to initialize Telegram Bot. Error: {e}", exc_info=True)
        bot = None
        return False

def save_metadata(metadata):
    """Saves metadata to the JSON file."""
    logging.info("Attempting to save metadata...")
    logging.debug(f"Target metadata file path: {os.path.abspath(METADATA_FILE)}")
    try:
        with open(METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=4)
        logging.info(f"Successfully saved metadata to '{METADATA_FILE}'.")
        logging.info("saved metadata")
    except IOError as e:
        logging.error(f"Error saving metadata: {e}")
    except Exception as e:
        # Catch any other unexpected errors during saving/dumping
        logging.error(f"An unexpected error occurred while saving metadata to '{METADATA_FILE}': {e}", exc_info=True)

# --- Helper Functions ---
def get_file_metadata(username, original_filename):
    """Retrieve metadata for a specific file and user."""
    logging.info(f"Attempting to get metadata for file '{original_filename}' for user '{username}'.") 

    metadata = load_metadata()

    user_files = metadata.get(username, [])
    logging.debug(f"Found {len(user_files)} file entries for user '{username}'.")

    if not user_files:
        logging.warning(f"No file metadata found for user '{username}' in loaded data.")

    for file_info in user_files:
        logging.debug(f"Checking file entry: {file_info.get('original_filename', 'N/A')}")
        try:
            if file_info['original_filename'] == original_filename:
                logging.info(f"Found matching metadata for file '{original_filename}' for user '{username}'.")
                return file_info
        except KeyError:
            # Log an error if a file_info dict is missing the expected key
            logging.error(f"Metadata format error: file entry missing 'original_filename' key for user '{username}'. Entry: {file_info}")
            continue
        logging.warning(f"Could not find metadata for specific file '{original_filename}' for user '{username}' (after checking {len(user_files)} entries).")
    return None

def add_file_metadata(username, original_filename, chunk_message_ids, compressed, file_size_bytes):
    """Adds file metadata for a user."""
    logging.info(f"Attempting to add/update metadata for file '{original_filename}' for user '{username}'.")
    metadata = load_metadata()
    if username not in metadata:
        metadata[username] = []
    else:
        logging.debug(f"User '{username}' found in metadata.")

    # Remove existing entry if it exists (e.g., re-uploading)
    metadata[username] = [f for f in metadata[username] if f['original_filename'] != original_filename]

    new_file_info = {
        'original_filename': original_filename,
        'chunk_message_ids': chunk_message_ids,
        'chat_id': TELEGRAM_CHAT_ID,
        'compressed': compressed,
        'upload_timestamp': telegram.Timestamp.now().isoformat(), # Use telegram's Timestamp or datetime
        'size_bytes': file_size_bytes
    }
    logging.debug(f"Created new file_info: {new_file_info}")
    metadata[username].append(new_file_info)
    logging.debug(f"Appended new file_info for '{original_filename}' to user '{username}' list.")
    logging.info(f"Calling save_metadata to write updated data for user '{username}'.")
    save_metadata(metadata)
    

def compress_file(input_path, output_path):
    """Compresses a single file into a zip archive."""
    logging.info(f"Attempting to compress file '{input_path}' to '{output_path}'.")
    logging.debug(f"Full input path: {os.path.abspath(input_path)}")
    logging.debug(f"Full output path: {os.path.abspath(output_path)}")

    try:
        if not os.path.exists(input_path):
            logging.error(f"Compression failed: Input file not found at '{input_path}'.")
            return False
        if not os.path.isfile(input_path):
             logging.error(f"Compression failed: Input path '{input_path}' is not a file.")
             return False

        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            arcname = os.path.basename(input_path)
            logging.debug(f"Writing '{input_path}' to zip archive as '{arcname}' using compression method ZIP_DEFLATED.")

            zipf.write(input_path, os.path.basename(input_path))
        logging.info(f"Successfully compressed '{input_path}' to '{output_path}'.") # INFO: Success
        return True
    
    except FileNotFoundError:
        # This might be redundant due to the check above, but good practice
        logging.error(f"Error during compression: Input file not found at '{input_path}'.")
        return False
    except PermissionError as e:
        logging.error(f"Error during compression of '{input_path}': Permission denied. Cannot read input or write output. Error: {e}")
        return False
    except zipfile.BadZipFile as e:
         logging.error(f"Error during compression of '{input_path}': Bad zip file error potentially related to output path '{output_path}'. Error: {e}")
         return False
    except Exception as e:
        logging.error(f"Error during compression of {input_path}: {e}")
        return False

import zipfile
import logging
import os # Needed for path operations
import math # Needed in upload_file
import tempfile # Needed in download_file_route
import asyncio # Needed in __main__
import json # Needed in upload_file, list_files

# Assuming Flask, telegram, secure_filename etc. are imported elsewhere
from flask import Flask, request, jsonify, render_template, send_file, abort
from werkzeug.utils import secure_filename
import telegram


# Assume bot, configuration variables (TELEGRAM_CHAT_ID, etc.),
# and helper functions (load_metadata, save_metadata, etc.) are defined elsewhere
# and logging is already configured (e.g., logging.basicConfig)

# --- Existing Function Definitions (with added logs) ---

def decompress_file(input_zip_path, output_dir):
    """Decompresses a zip archive containing a single file."""
    logging.info(f"Attempting to decompress '{input_zip_path}' into directory '{output_dir}'.") # INFO: Start decompression
    logging.debug(f"Full input zip path: {os.path.abspath(input_zip_path)}") # DEBUG: Show full input path
    logging.debug(f"Full output directory: {os.path.abspath(output_dir)}") # DEBUG: Show full output path
    try:
        # Check if input zip exists
        if not os.path.exists(input_zip_path):
            logging.error(f"Decompression failed: Input zip file not found at '{input_zip_path}'.")
            return None
        if not os.path.isfile(input_zip_path):
             logging.error(f"Decompression failed: Input path '{input_zip_path}' is not a file.")
             return None

        # Open the input zip file in read mode ('r')
        with zipfile.ZipFile(input_zip_path, 'r') as zipf:
            # Assuming only one file per archive for this tool
            member_list = zipf.namelist()
            if not member_list:
                logging.error(f"Decompression failed: Zip file '{input_zip_path}' is empty.")
                return None

            member_name = member_list[0]
            logging.debug(f"Identified member to extract: '{member_name}' from zip '{input_zip_path}'.") # DEBUG: Member name

            # Extract the member file to the specified output directory
            logging.debug(f"Extracting '{member_name}' to path '{output_dir}'.") # DEBUG: Before extract call
            extracted_path = zipf.extract(member_name, path=output_dir)
            logging.debug(f"Extraction successful. Full path of extracted file: {extracted_path}") # DEBUG: Success path

            # Log success after the 'with' block ensures the file is closed
            logging.info(f"Successfully decompressed '{input_zip_path}' -> '{extracted_path}'.") # INFO: Success
            return extracted_path # Return the full path to the extracted file

    except zipfile.BadZipFile as e:
        logging.error(f"Error during decompression of '{input_zip_path}': Invalid zip file. Error: {e}")
        return None
    except IndexError:
        # This case should be caught by the empty member_list check above, but included for robustness
        logging.error(f"Error during decompression of '{input_zip_path}': Cannot access member list (IndexError), possibly empty or corrupted.")
        return None
    except PermissionError as e:
        logging.error(f"Error during decompression of '{input_zip_path}': Permission denied. Cannot read input or write output to '{output_dir}'. Error: {e}")
        return None
    except Exception as e:
         # Catch any other unexpected errors during file access or unzipping
        logging.error(f"An unexpected error occurred during decompression of '{input_zip_path}': {e}", exc_info=True) # ERROR: Log other errors with traceback
        return None

# --- Flask Routes (with added logs) ---
# Assume 'app' is Flask app instance, 'bot' is initialized bot instance,
# 'render_template', 'jsonify', 'request', 'abort', 'send_file' are imported
# 'secure_filename' is imported
# Config vars like UPLOAD_FOLDER, DOWNLOAD_FOLDER, CHUNK_SIZE_BYTES are defined
# COMPRESSION_ENABLED is defined
# Helper funcs compress_file, add_file_metadata, load_metadata, get_file_metadata are defined

@app.route('/')
def index():
    """Renders the main HTML page."""
    logging.info("Route '/' accessed. Rendering index.html.") # INFO: Route access
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles file uploads, compression, splitting, and sending to Telegram."""
    logging.info("Route '/upload' accessed (POST request).") # INFO: Route access

    if not bot:
        logging.error("Upload rejected: Telegram Bot is not available (None).") # ERROR: Bot not ready
        return jsonify({"error": "Telegram Bot is not configured or failed to initialize."}), 500

    logging.debug(f"Request form data: {request.form}") # DEBUG: Show form data
    logging.debug(f"Request files data: {request.files}") # DEBUG: Show file data

    if 'file' not in request.files:
        logging.warning("Upload rejected: 'file' part missing in request.files.") # WARNING: Bad request
        return jsonify({"error": "No file part in the request"}), 400
    file = request.files['file']
    username = request.form.get('username')

    if not username:
        logging.warning("Upload rejected: 'username' missing in request form.") # WARNING: Bad request
        return jsonify({"error": "Username is required"}), 400
    if file.filename == '':
        logging.warning("Upload rejected: No file selected (filename is empty).") # WARNING: Bad request
        return jsonify({"error": "No selected file"}), 400

    original_filename = secure_filename(file.filename)
    logging.info(f"Processing upload for user '{username}', original filename: '{original_filename}'.") # INFO: Starting processing

    # Define temporary paths
    temp_upload_path = os.path.join(app.config['UPLOAD_FOLDER'], original_filename)
    file_to_process_path = temp_upload_path
    is_compressed = False
    sent_chunk_paths = [] # Initialize here for cleanup in outer except

    try:
        # 1. Save the uploaded file temporarily
        logging.debug(f"Attempting to save uploaded file to temporary path: '{temp_upload_path}'") # DEBUG: Before save
        file.save(temp_upload_path)
        logging.info(f"User '{username}' uploaded '{original_filename}'. Saved temporarily to: {temp_upload_path}") # INFO: Save success
        original_file_size = os.path.getsize(temp_upload_path)
        logging.debug(f"Original temporary file size: {original_file_size} bytes.") # DEBUG: File size

        # 2. Compress (Optional)
        if COMPRESSION_ENABLED:
            logging.info("Compression is enabled. Attempting to compress...") # INFO: Compression enabled
            compressed_filename = original_filename + ".zip"
            compressed_path = os.path.join(app.config['UPLOAD_FOLDER'], compressed_filename)
            logging.debug(f"Attempting compression: '{temp_upload_path}' -> '{compressed_path}'") # DEBUG: Paths for compression
            if compress_file(temp_upload_path, compressed_path): # compress_file logs its own success/failure
                file_to_process_path = compressed_path
                is_compressed = True
                logging.info(f"Compression successful. Using compressed file for processing: '{file_to_process_path}'") # INFO: Compression success
                # Clean up original uncompressed temp file if compression succeeded
                try:
                    logging.debug(f"Removing original uncompressed temp file: '{temp_upload_path}'") # DEBUG: Removing original
                    os.remove(temp_upload_path)
                except OSError as remove_err:
                    logging.warning(f"Could not remove original temp file '{temp_upload_path}' after compression: {remove_err}") # WARNING: Cleanup failed
            else:
                 # compress_file already logged the error
                 logging.warning(f"Compression failed for '{original_filename}', proceeding with the original uncompressed file.") # WARNING: Compression failed
                 is_compressed = False # Ensure flag is correct
                 # file_to_process_path remains temp_upload_path
        else:
            logging.info("Compression is disabled. Processing original file.") # INFO: Compression disabled

        # Calculate chunks for the file being processed (original or compressed)
        file_size = os.path.getsize(file_to_process_path)
        num_chunks = math.ceil(file_size / CHUNK_SIZE_BYTES)
        logging.info(f"File to send size: {file_size} bytes ('{os.path.basename(file_to_process_path)}'). Splitting into {num_chunks} chunks (Max chunk size: {CHUNK_SIZE_BYTES} bytes).") # INFO: Chunk calculation

        chunk_message_ids = []
        # sent_chunk_paths defined above try block

        # 3. Split and Send Chunks
        logging.debug(f"Opening file '{file_to_process_path}' for reading chunks.") # DEBUG: Before opening file
        with open(file_to_process_path, 'rb') as f:
            for i in range(num_chunks):
                current_chunk_num = i + 1
                logging.debug(f"Reading chunk {current_chunk_num}/{num_chunks}...") # DEBUG: Reading chunk
                chunk_data = f.read(CHUNK_SIZE_BYTES)
                if not chunk_data:
                    logging.warning(f"Read empty chunk data at chunk {current_chunk_num}, expected {num_chunks} chunks. Stopping.") # WARNING: Unexpected end of file
                    break # Should not happen with correct calculation, but good practice

                logging.debug(f"Read {len(chunk_data)} bytes for chunk {current_chunk_num}.") # DEBUG: Chunk size read

                # Create a temporary file for the chunk to send via telegram library
                chunk_filename = f"{os.path.basename(file_to_process_path)}.part{current_chunk_num:03d}"
                chunk_temp_path = os.path.join(app.config['UPLOAD_FOLDER'], chunk_filename)
                logging.debug(f"Writing chunk {current_chunk_num} data to temporary file '{chunk_temp_path}'.") # DEBUG: Writing temp chunk
                try:
                    with open(chunk_temp_path, 'wb') as chunk_f:
                        chunk_f.write(chunk_data)
                    sent_chunk_paths.append(chunk_temp_path) # Add to list *after* successful write
                    logging.debug(f"Temporary chunk file '{chunk_temp_path}' created.") # DEBUG: Temp chunk write success
                except IOError as write_err:
                    logging.error(f"Failed to write temporary chunk file '{chunk_temp_path}': {write_err}") # ERROR: Failed to write temp chunk
                    # Cleanup already created temp files before failing
                    if os.path.exists(file_to_process_path): os.remove(file_to_process_path)
                    for p in sent_chunk_paths: # Includes chunks written before this error
                        if os.path.exists(p): os.remove(p)
                    return jsonify({"error": f"Failed to create temporary file for chunk {current_chunk_num}"}), 500


                logging.info(f"Attempting to send chunk {current_chunk_num}/{num_chunks} ('{chunk_filename}') to Telegram chat ID {TELEGRAM_CHAT_ID}...") # INFO: Attempting send
                try:
                    # Caption includes metadata for easier identification in Telegram
                    caption = (f"User: {username}\n"
                               f"Original File: {original_filename}\n"
                               f"Chunk: {current_chunk_num}/{num_chunks}\n"
                               f"Compressed: {'Yes' if is_compressed else 'No'}")
                    logging.debug(f"Sending document with caption:\n{caption}") # DEBUG: Show caption

                    with open(chunk_temp_path, 'rb') as chunk_to_send:
                        # THE ACTUAL TELEGRAM SEND CALL
                        message = bot.send_document(
                            chat_id=TELEGRAM_CHAT_ID,
                            document=chunk_to_send,
                            filename=chunk_filename, # Use chunk filename for TG message
                            caption=caption,
                            timeout=300 # Increase timeout for potentially large chunks (5 mins)
                        )
                    chunk_message_ids.append(message.message_id)
                    logging.info(f"Chunk {current_chunk_num} sent successfully. Message ID: {message.message_id}") # INFO: Send success

                except Exception as e:
                    # Log the error related to sending this specific chunk
                    logging.error(f"Failed to send chunk {current_chunk_num} ('{chunk_filename}') to Telegram: {e}", exc_info=True) # ERROR: Telegram send failed with traceback
                    # Cleanup local files before raising error
                    logging.warning("Cleaning up local files due to Telegram send failure.") # WARNING: Starting cleanup on error
                    if os.path.exists(file_to_process_path):
                         try: os.remove(file_to_process_path)
                         except OSError as rm_err: logging.error(f"Error removing process file on fail: {rm_err}")
                    for p in sent_chunk_paths: # Includes the chunk that failed to send (locally exists)
                        if os.path.exists(p):
                             try: os.remove(p)
                             except OSError as rm_err: logging.error(f"Error removing chunk file {p} on fail: {rm_err}")
                    # Return error response to client
                    return jsonify({"error": f"Failed to send chunk {current_chunk_num} to Telegram: {e}"}), 500

        # 4. Store Metadata (after loop completes successfully)
        logging.info("All chunks sent successfully. Storing metadata...") # INFO: Preparing to store metadata
        add_file_metadata(username, original_filename, chunk_message_ids, is_compressed, original_file_size) # add_file_metadata logs its own activity
        logging.info(f"Metadata saved for '{original_filename}' under user '{username}'.") # INFO: Metadata save called (relies on add_file_metadata success)

        # 5. Cleanup temporary files (after successful upload and metadata save)
        logging.info("Upload complete. Cleaning up temporary upload files...") # INFO: Starting final cleanup
        if file_to_process_path != temp_upload_path and os.path.exists(temp_upload_path):
             # This case should only happen if compression failed and original was kept
             logging.warning(f"Attempting cleanup of original temp file '{temp_upload_path}' which might not have been deleted earlier.")
             try:
                 os.remove(temp_upload_path)
                 logging.info(f"Removed original temp file: {temp_upload_path}")
             except OSError as rm_err:
                 logging.error(f"Error removing original temp file during final cleanup: {rm_err}")

        if os.path.exists(file_to_process_path):
            try:
                os.remove(file_to_process_path)
                logging.info(f"Removed processed file: {file_to_process_path}") # INFO: Removed main temp file (original or compressed)
            except OSError as rm_err:
                 logging.error(f"Error removing processed file '{file_to_process_path}' during final cleanup: {rm_err}")

        for chunk_path in sent_chunk_paths:
             if os.path.exists(chunk_path):
                 try:
                    os.remove(chunk_path)
                    logging.info(f"Removed temporary chunk file: {chunk_path}") # INFO: Removed temp chunk
                 except OSError as rm_err:
                    logging.error(f"Error removing temporary chunk file '{chunk_path}' during final cleanup: {rm_err}")

        # Return success response
        logging.info(f"Upload process for '{original_filename}' completed successfully.") # INFO: Overall success
        return jsonify({
            "message": f"File '{original_filename}' uploaded successfully.",
            "filename": original_filename,
            "username": username,
            "num_chunks": num_chunks
        }), 200

    except Exception as e:
        # Catch-all for errors *outside* the chunk sending loop (e.g., initial save, compression, chunk calculation)
        logging.error(f"An unexpected error occurred during the upload process for '{original_filename}' (user: '{username}'): {e}", exc_info=True) # ERROR: Outer exception with traceback
        # Attempt cleanup even on outer error
        logging.warning("Attempting cleanup after unexpected error during upload.") # WARNING: Cleanup on outer error
        if 'temp_upload_path' in locals() and os.path.exists(temp_upload_path):
            try: os.remove(temp_upload_path)
            except OSError as rm_err: logging.error(f"Error removing temp_upload_path on outer fail: {rm_err}")
        if 'file_to_process_path' in locals() and os.path.exists(file_to_process_path) and file_to_process_path != temp_upload_path:
            try: os.remove(file_to_process_path)
            except OSError as rm_err: logging.error(f"Error removing file_to_process_path on outer fail: {rm_err}")
        # Cleanup any chunks created before error
        if 'sent_chunk_paths' in locals():
            for p in sent_chunk_paths:
                if os.path.exists(p):
                     try: os.remove(p)
                     except OSError as rm_err: logging.error(f"Error removing chunk {p} on outer fail: {rm_err}")
        # Return error response
        return jsonify({"error": f"An unexpected server error occurred during upload: {e}"}), 500


@app.route('/files/<username>', methods=['GET'])
def list_files(username):
    """Lists files stored for a specific username."""
    logging.info(f"Route '/files/{username}' accessed (GET request).") # INFO: Route access
    metadata = load_metadata() # Logs its own activity
    user_files = metadata.get(username, [])
    logging.debug(f"Found {len(user_files)} entries for user '{username}' in metadata.") # DEBUG: Count entries

    # Return only essential info to the frontend
    files_info = []
    for f in user_files:
        # Add basic check for expected key before appending
        if 'original_filename' in f:
            files_info.append({"original_filename": f['original_filename']})
        else:
            logging.warning(f"Metadata entry for user '{username}' missing 'original_filename': {f}") # WARNING: Malformed entry

    logging.info(f"Returning list of {len(files_info)} files for user '{username}'.") # INFO: Return count
    return jsonify(files_info)

@app.route('/download/<username>/<filename>', methods=['GET'])
def download_file_route(username, filename):
    """Handles downloading, merging, and decompressing files."""
    logging.info(f"Route '/download/{username}/{filename}' accessed (GET request).") # INFO: Route access

    if not bot:
        logging.error("Download rejected: Telegram Bot is not available (None).") # ERROR: Bot not ready
        # Use abort(503) for service unavailable might be more appropriate than 500
        abort(503, description="Service temporarily unavailable: Telegram Bot not initialized.")

    safe_filename = secure_filename(filename) # Sanitize filename just in case
    logging.debug(f"Sanitized filename for download: '{safe_filename}'.") # DEBUG: Sanitized name

    # Get metadata for the specific file
    file_info = get_file_metadata(username, safe_filename) # get_file_metadata logs its own activity

    if not file_info:
        logging.warning(f"Download request failed: File '{safe_filename}' not found in metadata for user '{username}'.") # WARNING: File not found
        abort(404, description="File not found for this user.")

    # Extract details from metadata
    try:
        chunk_message_ids = file_info['chunk_message_ids']
        chat_id = file_info['chat_id']
        is_compressed = file_info['compressed']
        original_filename = file_info['original_filename'] # Use the stored original filename
        logging.debug(f"Found metadata: {len(chunk_message_ids)} chunks, chat_id={chat_id}, compressed={is_compressed}, original_filename='{original_filename}'.") # DEBUG: Show found metadata details
    except KeyError as meta_err:
        logging.error(f"Download failed: Metadata for '{safe_filename}' (user: '{username}') is incomplete. Missing key: {meta_err}. Metadata: {file_info}", exc_info=True) # ERROR: Incomplete metadata
        abort(500, description=f"Server error: Metadata for file '{safe_filename}' is corrupted.")

    # Use a temporary directory for this download operation
    logging.debug(f"Creating temporary directory for download in '{app.config['DOWNLOAD_FOLDER']}'.") # DEBUG: Creating temp dir
    with tempfile.TemporaryDirectory(dir=app.config['DOWNLOAD_FOLDER'], prefix=f"download_{username}_") as temp_dir:
        logging.info(f"Created temporary directory for download: '{temp_dir}'.") # INFO: Temp dir created
        downloaded_chunk_paths = []
        # Determine the path for the merged file (original or compressed name)
        merged_filename_base = original_filename + ".zip" if is_compressed else original_filename
        merged_file_path = os.path.join(temp_dir, "merged_" + merged_filename_base)
        logging.debug(f"Target path for merged file: '{merged_file_path}'.") # DEBUG: Merged path

        try:
            # 1. Download chunks from Telegram
            logging.info(f"Starting download of {len(chunk_message_ids)} chunks for '{original_filename}' from chat {chat_id}...") # INFO: Start download loop

            for i, message_id in enumerate(chunk_message_ids):
                current_chunk_num = i + 1
                logging.info(f"Downloading chunk {current_chunk_num}/{len(chunk_message_ids)} (Message ID: {message_id})...") # INFO: Downloading chunk
                try:
                    # Get file object from Telegram
                    logging.debug(f"Calling bot.get_file for message_id {message_id} in chat {chat_id}.") # DEBUG: Before get_file
                    tg_file = bot.get_file(message_id=message_id, chat_id=chat_id, timeout=300)
                    logging.debug(f"Got telegram file object: {tg_file}") # DEBUG: Got file object

                    # Define path for the downloaded chunk within the temp directory
                    chunk_download_path = os.path.join(temp_dir, f"part{current_chunk_num:03d}")
                    logging.debug(f"Attempting to download Telegram file to '{chunk_download_path}'.") # DEBUG: Before download call

                    # Download the file using the library's download method
                    tg_file.download(custom_path=chunk_download_path)
                    downloaded_chunk_paths.append(chunk_download_path)
                    logging.info(f"Chunk {current_chunk_num} downloaded successfully to: {chunk_download_path}") # INFO: Chunk download success
                    logging.debug(f"Chunk {current_chunk_num} file size on disk: {os.path.getsize(chunk_download_path)} bytes.") # DEBUG: Chunk size

                except telegram.error.TelegramError as tg_err:
                    # Catch specific Telegram errors if possible
                    logging.error(f"Failed to download chunk {current_chunk_num} (Message ID {message_id}) due to Telegram error: {tg_err}", exc_info=True) # ERROR: Telegram download failed
                    abort(502, description=f"Failed to download chunk {current_chunk_num} from storage: {tg_err}") # 502 Bad Gateway might fit
                except Exception as e:
                    # Catch other errors during download
                    logging.error(f"An unexpected error occurred downloading chunk {current_chunk_num} (Message ID {message_id}): {e}", exc_info=True) # ERROR: Other download error
                    abort(500, description=f"Failed to download chunk {current_chunk_num} due to server error.")

            # Ensure chunks are sorted correctly (although they should be in order)
            logging.debug("Sorting downloaded chunk paths...") # DEBUG: Sorting
            downloaded_chunk_paths.sort()
            logging.debug(f"Sorted paths: {downloaded_chunk_paths}") # DEBUG: Show sorted list

            # 2. Merge chunks
            logging.info(f"Merging {len(downloaded_chunk_paths)} downloaded chunks into '{merged_file_path}'...") # INFO: Start merge
            try:
                with open(merged_file_path, 'wb') as merged_f:
                    for chunk_path in downloaded_chunk_paths:
                        logging.debug(f"Appending chunk '{chunk_path}' to merged file.") # DEBUG: Appending chunk
                        with open(chunk_path, 'rb') as chunk_f:
                            merged_f.write(chunk_f.read())
                logging.info(f"Merging complete. Merged file created at '{merged_file_path}'.") # INFO: Merge success
                logging.debug(f"Merged file size: {os.path.getsize(merged_file_path)} bytes.") # DEBUG: Merged size
            except IOError as merge_err:
                 logging.error(f"Failed to merge chunks into '{merged_file_path}'. IOError: {merge_err}", exc_info=True) # ERROR: Merge failure
                 abort(500, description="Failed to merge downloaded file parts.")

            # 3. Decompress (if needed)
            final_file_path = merged_file_path # Assume merged path initially
            if is_compressed:
                logging.info(f"File was compressed. Attempting to decompress '{merged_file_path}'...") # INFO: Start decompress
                # Decompress into the same temp directory
                decompressed_path = decompress_file(merged_file_path, temp_dir) # decompress_file logs its own activity
                if decompressed_path:
                    final_file_path = decompressed_path
                    logging.info(f"Decompression successful. Final file path: '{final_file_path}'") # INFO: Decompress success
                    logging.debug(f"Decompressed file size: {os.path.getsize(final_file_path)} bytes.") # DEBUG: Decompressed size
                else:
                    # decompress_file already logged the error
                    logging.error(f"Decompression failed for '{merged_file_path}'. Aborting download.") # ERROR: Decompress failed (re-log here for context)
                    abort(500, description="Failed to decompress file.")
            else:
                 logging.info("File was not compressed. Skipping decompression.") # INFO: No decompression needed

            # 4. Send file to user
            logging.info(f"Download processing complete. Sending final file '{original_filename}' (path: '{final_file_path}') to user...") # INFO: Sending file
            # Use the original filename for the download prompt in the browser
            return send_file(
                final_file_path,
                as_attachment=True,
                download_name=original_filename
            )

        except Exception as e:
            # Catch errors occurring within the temporary directory block but outside specific try/excepts
            logging.error(f"An unexpected error occurred during download/processing of '{original_filename}' for user '{username}': {e}", exc_info=True) # ERROR: Outer download processing error
            # The temporary directory will be cleaned up automatically by the 'with' statement
            abort(500, description=f"An unexpected error occurred during file retrieval: {e}")
        # No explicit finally needed for cleanup here because TemporaryDirectory handles it
        finally:
             logging.debug(f"Exiting temporary directory block for download of '{original_filename}'. Cleanup should occur automatically.") # DEBUG: Exiting temp dir

# --- Main Execution (with added logs) ---
if __name__ == '__main__':
    logging.info("Script execution started in __main__ block.") # INFO: Start main block

    # Initialize Bot
    logging.info("Attempting asynchronous bot initialization...") # INFO: Before async call
    init_success = asyncio.run(initialize_bot()) # initialize_bot logs its own details

    if not init_success:
        # initialize_bot already logged the specific error
        logging.warning("-----------------------------------------------------") # WARNING: Separator
        logging.warning("WARNING: Telegram Bot failed to initialize. ") # WARNING: Summary
        logging.warning("         Upload/Download functionality will not work.") # WARNING: Consequence
        logging.warning("         Check your TELEGRAM_BOT_TOKEN and network.") # WARNING: Hint
        logging.warning("-----------------------------------------------------") # WARNING: Separator
        # Consider exiting if the bot is essential
        # logging.critical("Exiting application due to failed bot initialization.")
        # exit(1)
    else:
         logging.info("Telegram Bot initialized successfully.") # INFO: Bot init success summary

    # Double-check bot status after initialization attempt (paranoid check)
    if not bot:
        # This case should ideally be covered by init_success check, but added for robustness
        logging.error("-----------------------------------------------------") # ERROR: Separator
        logging.error("ERROR: Bot object is still None after initialization attempt.") # ERROR: Unexpected state
        logging.error("       Upload/Download functionality WILL NOT WORK.") # ERROR: Consequence
        logging.error("       Review initialization logs for errors.") # ERROR: Hint
        logging.error("-----------------------------------------------------") # ERROR: Separator

    # Check Chat ID configuration (Assuming TELEGRAM_CHAT_ID is defined)
    # Using a placeholder check value example - adapt if necessary
    PLACEHOLDER_CHAT_ID = -1002693341109 # Example placeholder ID, adjust if needed
    if TELEGRAM_CHAT_ID == PLACEHOLDER_CHAT_ID: # Check against the specific placeholder
         logging.error("-----------------------------------------------------") # ERROR: Separator
         logging.error(f"ERROR: TELEGRAM_CHAT_ID is set to the placeholder value ({PLACEHOLDER_CHAT_ID}).") # ERROR: Config error
         logging.error("       Please obtain your numeric channel ID and update the script.") # ERROR: Instruction
         logging.error("-----------------------------------------------------") # ERROR: Separator
         # Optionally prevent server start:
         # logging.critical("Exiting application due to unconfigured TELEGRAM_CHAT_ID.")
         # exit(1)
    else:
        logging.info(f"TELEGRAM_CHAT_ID is configured to: {TELEGRAM_CHAT_ID}") # INFO: Chat ID is set


    logging.info("Starting Flask server...") # INFO: Before app.run
    print("Open http://127.0.0.1:5000 in your browser.") # Keep user-facing print
    # Use host='0.0.0.0' to make accessible on your network (use with caution)
    # Setting debug=True enables Flask's debugger and auto-reloader
    app.run(debug=True, host='127.0.0.1', port=5000) # debug=True for development ONLY