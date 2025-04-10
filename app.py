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
TELEGRAM_BOT_TOKEN = "7812479394:AAFhPxoHysfTUf710a7ShQbaSCYi-0r7e7E" # YOUR BOT TOKEN HERE
# IMPORTANT: Replace with your *NUMERIC* Chat ID (likely negative)
TELEGRAM_CHAT_ID = -1002693341109 # GET YOUR NUMERIC CHAT ID AND PUT IT HERE
if TELEGRAM_CHAT_ID == -1002693341109:
    print("ERROR: Please update TELEGRAM_CHAT_ID in app.py with your numeric channel/chat ID.")
    # You might want to exit here in a real application
    # exit(1)

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
try:
    bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
    logging.info(f"Telegram Bot initialized. Bot Name: {bot.get_me().username}")
except Exception as e:
    logging.error(f"Failed to initialize Telegram Bot: {e}")
    bot = None # Set bot to None if initialization fails

# --- Metadata Handling ---
def load_metadata():
    """Loads metadata from the JSON file."""
    if not os.path.exists(METADATA_FILE):
        return {}
    try:
        with open(METADATA_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logging.error(f"Error loading metadata: {e}")
        return {} # Return empty dict on error

async def initialize_bot(): # <<< CREATE ASYNC FUNCTION
    global bot
    try:
        temp_bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
        # Use await to call the asynchronous get_me() method
        user = await temp_bot.get_me() # <<< USE AWAIT
        logging.info(f"Telegram Bot initialized. Bot Name: {user.username}") # <<< Use user.username
        bot = temp_bot # Assign to global bot variable if successful
        return True
    except Exception as e:
        logging.error(f"Failed to initialize Telegram Bot: {e}")
        bot = None # Ensure bot is None if initialization fails
        return False

def save_metadata(metadata):
    """Saves metadata to the JSON file."""
    try:
        with open(METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=4)
    except IOError as e:
        logging.error(f"Error saving metadata: {e}")

# --- Helper Functions ---
def get_file_metadata(username, original_filename):
    """Retrieve metadata for a specific file and user."""
    metadata = load_metadata()
    user_files = metadata.get(username, [])
    for file_info in user_files:
        if file_info['original_filename'] == original_filename:
            return file_info
    return None

def add_file_metadata(username, original_filename, chunk_message_ids, compressed, file_size_bytes):
    """Adds file metadata for a user."""
    metadata = load_metadata()
    if username not in metadata:
        metadata[username] = []

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
    metadata[username].append(new_file_info)
    save_metadata(metadata)

def compress_file(input_path, output_path):
    """Compresses a single file into a zip archive."""
    try:
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add file with its base name inside the archive
            zipf.write(input_path, os.path.basename(input_path))
        logging.info(f"File compressed: {input_path} -> {output_path}")
        return True
    except Exception as e:
        logging.error(f"Error during compression of {input_path}: {e}")
        return False

def decompress_file(input_zip_path, output_dir):
    """Decompresses a zip archive containing a single file."""
    try:
        with zipfile.ZipFile(input_zip_path, 'r') as zipf:
            # Assuming only one file per archive for this tool
            member_name = zipf.namelist()[0]
            extracted_path = zipf.extract(member_name, path=output_dir)
            logging.info(f"File decompressed: {input_zip_path} -> {extracted_path}")
            return extracted_path
    except Exception as e:
        logging.error(f"Error during decompression of {input_zip_path}: {e}")
        return None

# --- Flask Routes ---
@app.route('/')
def index():
    """Renders the main HTML page."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles file uploads, compression, splitting, and sending to Telegram."""
    if not bot:
        return jsonify({"error": "Telegram Bot is not configured or failed to initialize."}), 500
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    file = request.files['file']
    username = request.form.get('username')

    if not username:
        return jsonify({"error": "Username is required"}), 400
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    original_filename = secure_filename(file.filename)
    temp_upload_path = os.path.join(app.config['UPLOAD_FOLDER'], original_filename)
    file_to_process_path = temp_upload_path
    is_compressed = False

    try:
        # 1. Save the uploaded file temporarily
        file.save(temp_upload_path)
        logging.info(f"User '{username}' uploaded '{original_filename}'. Saved to: {temp_upload_path}")
        original_file_size = os.path.getsize(temp_upload_path)

        # 2. Compress (Optional)
        if COMPRESSION_ENABLED:
            compressed_filename = original_filename + ".zip"
            compressed_path = os.path.join(app.config['UPLOAD_FOLDER'], compressed_filename)
            if compress_file(temp_upload_path, compressed_path):
                file_to_process_path = compressed_path
                is_compressed = True
                logging.info(f"Using compressed file: {file_to_process_path}")
                # Clean up original uncompressed temp file if compression succeeded
                os.remove(temp_upload_path)
            else:
                 logging.warning(f"Compression failed for {original_filename}, using original.")
                 # Keep original file path if compression fails
                 is_compressed = False # Ensure flag is correct
        else:
            logging.info("Compression is disabled.")


        file_size = os.path.getsize(file_to_process_path)
        num_chunks = math.ceil(file_size / CHUNK_SIZE_BYTES)
        logging.info(f"File size: {file_size} bytes. Splitting into {num_chunks} chunks (Chunk size: {CHUNK_SIZE_BYTES} bytes).")

        chunk_message_ids = []
        sent_chunk_paths = [] # Keep track of sent chunks for potential cleanup

        # 3. Split and Send Chunks
        with open(file_to_process_path, 'rb') as f:
            for i in range(num_chunks):
                chunk_data = f.read(CHUNK_SIZE_BYTES)
                if not chunk_data:
                    break # Should not happen with correct calculation, but good practice

                # Create a temporary file for the chunk to send via telegram library
                chunk_filename = f"{os.path.basename(file_to_process_path)}.part{i+1:03d}"
                chunk_temp_path = os.path.join(app.config['UPLOAD_FOLDER'], chunk_filename)
                with open(chunk_temp_path, 'wb') as chunk_f:
                    chunk_f.write(chunk_data)
                sent_chunk_paths.append(chunk_temp_path)

                logging.info(f"Sending chunk {i+1}/{num_chunks} ({chunk_filename}) to Telegram...")
                try:
                    # Caption includes metadata for easier identification in Telegram
                    caption = (f"User: {username}\n"
                               f"Original File: {original_filename}\n"
                               f"Chunk: {i+1}/{num_chunks}\n"
                               f"Compressed: {'Yes' if is_compressed else 'No'}")

                    with open(chunk_temp_path, 'rb') as chunk_to_send:
                        message = bot.send_document(
                            chat_id=TELEGRAM_CHAT_ID,
                            document=chunk_to_send,
                            filename=chunk_filename, # Use chunk filename for TG message
                            caption=caption,
                            timeout=300 # Increase timeout for potentially large chunks (5 mins)
                        )
                    chunk_message_ids.append(message.message_id)
                    logging.info(f"Chunk {i+1} sent. Message ID: {message.message_id}")

                except Exception as e:
                    logging.error(f"Failed to send chunk {i+1} to Telegram: {e}")
                    # Cleanup already sent chunks from Telegram? (More complex)
                    # Cleanup local files before raising error
                    if os.path.exists(file_to_process_path): os.remove(file_to_process_path)
                    for p in sent_chunk_paths:
                        if os.path.exists(p): os.remove(p)
                    return jsonify({"error": f"Failed to send chunk {i+1} to Telegram: {e}"}), 500

        # 4. Store Metadata
        add_file_metadata(username, original_filename, chunk_message_ids, is_compressed, original_file_size)
        logging.info(f"Metadata saved for '{original_filename}' under user '{username}'.")

        # 5. Cleanup temporary files
        logging.info("Cleaning up temporary upload files...")
        if os.path.exists(file_to_process_path):
            os.remove(file_to_process_path)
            logging.info(f"Removed: {file_to_process_path}")
        for chunk_path in sent_chunk_paths:
             if os.path.exists(chunk_path):
                 os.remove(chunk_path)
                 logging.info(f"Removed chunk: {chunk_path}")

        return jsonify({
            "message": f"File '{original_filename}' uploaded successfully.",
            "filename": original_filename,
            "username": username,
            "num_chunks": num_chunks
        }), 200

    except Exception as e:
        logging.error(f"An error occurred during upload for {original_filename}: {e}", exc_info=True)
        # Attempt cleanup even on error
        if os.path.exists(temp_upload_path) and file_to_process_path != temp_upload_path : os.remove(temp_upload_path)
        if os.path.exists(file_to_process_path): os.remove(file_to_process_path)
        # Cleanup any chunks created before error
        if 'sent_chunk_paths' in locals():
            for p in sent_chunk_paths:
                if os.path.exists(p): os.remove(p)

        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500


@app.route('/files/<username>', methods=['GET'])
def list_files(username):
    """Lists files stored for a specific username."""
    metadata = load_metadata()
    user_files = metadata.get(username, [])
    # Return only essential info to the frontend
    files_info = [{"original_filename": f['original_filename']} for f in user_files]
    return jsonify(files_info)

@app.route('/download/<username>/<filename>', methods=['GET'])
def download_file_route(username, filename):
    """Handles downloading, merging, and decompressing files."""
    if not bot:
        return jsonify({"error": "Telegram Bot is not configured or failed to initialize."}), 500

    safe_filename = secure_filename(filename) # Sanitize filename just in case
    file_info = get_file_metadata(username, safe_filename)

    if not file_info:
        logging.warning(f"File '{safe_filename}' not found for user '{username}'.")
        abort(404, description="File not found for this user.")

    chunk_message_ids = file_info['chunk_message_ids']
    chat_id = file_info['chat_id']
    is_compressed = file_info['compressed']
    original_filename = file_info['original_filename'] # Use the stored original filename

    # Use a temporary directory for this download operation
    with tempfile.TemporaryDirectory(dir=app.config['DOWNLOAD_FOLDER']) as temp_dir:
        downloaded_chunk_paths = []
        merged_file_path = os.path.join(temp_dir, "merged_" + (original_filename + ".zip" if is_compressed else original_filename))

        try:
            # 1. Download chunks from Telegram
            logging.info(f"Starting download for '{original_filename}' ({len(chunk_message_ids)} chunks)...")
            for i, message_id in enumerate(chunk_message_ids):
                logging.info(f"Downloading chunk {i+1}/{len(chunk_message_ids)} (Message ID: {message_id})...")
                try:
                    # Get file object from Telegram
                    tg_file = bot.get_file(message_id=message_id, chat_id=chat_id, timeout=300)
                    # Define path for the downloaded chunk
                    chunk_download_path = os.path.join(temp_dir, f"part{i+1:03d}")
                    # Download the file
                    tg_file.download(custom_path=chunk_download_path)
                    downloaded_chunk_paths.append(chunk_download_path)
                    logging.info(f"Chunk {i+1} downloaded to: {chunk_download_path}")
                except Exception as e:
                    logging.error(f"Failed to download chunk with Message ID {message_id}: {e}")
                    abort(500, description=f"Failed to download chunk {i+1}.")

            # Ensure chunks are sorted correctly (although they should be in order)
            downloaded_chunk_paths.sort()

            # 2. Merge chunks
            logging.info(f"Merging {len(downloaded_chunk_paths)} chunks into {merged_file_path}...")
            with open(merged_file_path, 'wb') as merged_f:
                for chunk_path in downloaded_chunk_paths:
                    with open(chunk_path, 'rb') as chunk_f:
                        merged_f.write(chunk_f.read())
            logging.info("Merging complete.")

            # 3. Decompress (if needed)
            final_file_path = merged_file_path
            if is_compressed:
                logging.info(f"Decompressing {merged_file_path}...")
                # Decompress into the same temp directory
                decompressed_path = decompress_file(merged_file_path, temp_dir)
                if decompressed_path:
                    final_file_path = decompressed_path
                    logging.info(f"Decompression successful: {final_file_path}")
                else:
                    logging.error("Decompression failed.")
                    abort(500, description="Failed to decompress file.")

            # 4. Send file to user
            logging.info(f"Sending final file '{original_filename}' to user...")
            return send_file(
                final_file_path,
                as_attachment=True,
                download_name=original_filename # Send with the original filename
            )

        except Exception as e:
            logging.error(f"Error during download/processing of '{original_filename}': {e}", exc_info=True)
            # The temporary directory will be cleaned up automatically by the 'with' statement
            abort(500, description=f"An error occurred during file retrieval: {e}")
        # No explicit finally needed for cleanup here because TemporaryDirectory handles it

# --- Main Execution ---
if __name__ == '__main__':

    if not asyncio.run(initialize_bot()): # <<< RUN THE ASYNC FUNCTION
        print("-----------------------------------------------------")
        print("WARNING: Telegram Bot failed to initialize. ")
        print("         Upload/Download functionality will not work.")
        print("         Check your TELEGRAM_BOT_TOKEN.")
        print("-----------------------------------------------------")
        # Consider exiting if the bot is essential
        # exit(1)

    if not bot:
        print("-----------------------------------------------------")
        print("WARNING: Telegram Bot failed to initialize. ")
        print("         Upload/Download functionality will not work.")
        print("         Check your TELEGRAM_BOT_TOKEN.")
        print("-----------------------------------------------------")
    if TELEGRAM_CHAT_ID == -1002693341109:
         print("-----------------------------------------------------")
         print("ERROR: TELEGRAM_CHAT_ID is not set in app.py.")
         print("       Please obtain your numeric channel ID and update the script.")
         print("-----------------------------------------------------")
         # Optionally prevent server start:
         # exit(1)

    print("Starting Flask server...")
    print("Open http://127.0.0.1:5000 in your browser.")
    # Use host='0.0.0.0' to make accessible on your network (use with caution)
    app.run(debug=True, host='127.0.0.1', port=5000) # debug=True for development ONLY