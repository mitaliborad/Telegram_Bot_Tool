"""Flask routes and core logic for the Telegram File Storage tool."""
import io
import os
import uuid
import time
import json
import zipfile
import tempfile
import shutil
import logging
from bson import ObjectId
from database import User
from app_setup import app, login_manager
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, List, Generator, Union
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from flask import ( Flask, request, render_template, flash, redirect, url_for,
    make_response, jsonify, send_file, Response, stream_with_context )
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone
import re
from dateutil import parser as dateutil_parser

from flask_jwt_extended import jwt_required, get_jwt_identity
import threading
import database
from database import (
    save_file_metadata,
    find_metadata_by_username,
    find_metadata_by_access_id,
    delete_metadata_by_filename,
    find_user_by_email,
    save_user
)
from config import format_time
from flask_cors import CORS
from app_setup import app, login_manager
from app_setup import app, upload_progress_data, download_prep_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID, CHUNK_SIZE,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, MAX_DOWNLOAD_WORKERS,
    format_bytes
)
from telegram_api import send_file_to_telegram, download_telegram_file_content
from flask_jwt_extended import create_access_token

# Initialize CORS for registration endpoint
# CORS(app, resources={r"/register": {"origins": "http://127.0.0.1:5000"}})

# --- Type Aliases ---
Metadata = Dict[str, List[Dict[str, Any]]]
UploadProgressData = Dict[str, Any]; DownloadPrepData = Dict[str, Any]
SseEvent = str; ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]]
ChunkDataResult = Tuple[int, Optional[bytes], Optional[str]]

# --- Constants ---
DEFAULT_CHUNK_READ_SIZE = 4 * 1024 * 1024; STREAM_CHUNK_SIZE = 65536

# ----- STEP 1D Start: Define user_loader -----
@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    """Flask-Login user loader callback."""
    logging.debug(f"Attempting to load user with ID: {user_id}")
    if not user_id:
        return None
    try:
        # Convert the string ID back to ObjectId for MongoDB query
        user_doc, error = database.find_user_by_id(ObjectId(user_id)) # <<< --- NEW DB FUNCTION NEEDED
        if error:
             logging.error(f"Error loading user by ID {user_id}: {error}")
             return None
        if user_doc:
             # Create and return a User object
             return User(user_doc)
        else:
             logging.warning(f"User ID {user_id} not found in database.")
             return None
    except Exception as e:
         # Handle potential ObjectId conversion errors or other issues
         logging.error(f"Exception loading user {user_id}: {e}", exc_info=True)
         return None

@app.route('/register', methods=['GET'])
def show_register_page():
    """Displays the registration page."""
    logging.info("Serving registration page.")
    try:
        return render_template('register.html')
    except Exception as e:
        logging.error(f"Error rendering register.html: {e}", exc_info=True)
        return make_response("Error loading page.", 500)

@app.route('/register', methods=['POST', 'OPTIONS'])
def register_user(): # Keep the original function name if preferred
    """Handles user registration submission with username."""
    logging.info("Received POST request for /register")

    if request.method == 'OPTIONS':
        # If Flask-CORS is working, this part might not even be reached.
        # Return a simple 204 No Content if manual handling were needed.
        return make_response(), 204
    
    # 1. --- Get Data ---
    try:
        data = request.get_json()
        if not data:
            logging.warning("Registration failed: No JSON data received.")
            return make_response(jsonify({"error": "Invalid request format. Expected JSON."}), 400)

        logging.info(f"Received registration data: {data}") # Be careful logging sensitive data

        # Extract data using .get() for safety
        username = data.get('username', '').strip() # <<< Get username
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        confirmPassword = data.get('confirmPassword', '')
        agreeTerms = data.get('agreeTerms', False)
        understand_privacy = data.get('understandPrivacy', False)

    except Exception as e:
        logging.error(f"Error parsing registration JSON data: {e}", exc_info=True)
        return make_response(jsonify({"error": "Invalid request data received."}), 400)

    # 2. --- Validation ---
    # Required fields check
    # <<< Updated check to include username and remove firstName/lastName >>>
    if not all([username, email, password, confirmPassword]):
        logging.warning("Registration failed: Missing required fields.")
        return make_response(jsonify({"error": "Username, email, and passwords are required."}), 400)

    # Username format check (example: alphanumeric + underscore, min 3 chars)
    if not re.match(r"^[a-zA-Z0-9_]{3,}$", username):
        logging.warning(f"Registration failed: Invalid username format '{username}'.")
        return make_response(jsonify({"error": "Invalid username format (letters, numbers, _, min 3 chars)."}), 400)

    # Email format check
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        logging.warning(f"Registration failed: Invalid email format '{email}'.")
        return make_response(jsonify({"error": "Invalid email format."}), 400)

    # Password match check
    if password != confirmPassword:
        logging.warning("Registration failed: Passwords do not match.")
        return make_response(jsonify({"error": "Passwords do not match."}), 400)

    # Terms agreement check
    if not (agreeTerms and understand_privacy):
        logging.warning("Registration failed: Terms not agreed.")
        return make_response(jsonify({"error": "You must agree to all terms and privacy conditions."}), 400)

    # 3. --- Check Uniqueness ---
    # Check Username (Requires find_user_by_username in database.py)
    try:
        # <<< Using the specific database function >>>
        existing_user_by_username, db_error_uname = database.find_user_by_username(username)
        if db_error_uname:
            raise Exception(db_error_uname) # Treat DB errors seriously
        if existing_user_by_username:
            logging.warning(f"Registration failed: Username '{username}' already exists.")
            return make_response(jsonify({"error": "Username is already taken."}), 409) # 409 Conflict
    except Exception as e:
        logging.error(f"Database error checking username '{username}': {e}", exc_info=True)
        return make_response(jsonify({"error": "Server error during registration check (username)."}), 500)

    # Check Email
    try:
        # <<< Using the specific database function >>>
        existing_user_by_email, db_error_email = database.find_user_by_email(email)
        if db_error_email:
            raise Exception(db_error_email)
        if existing_user_by_email:
            logging.warning(f"Registration failed: Email '{email}' already exists.")
            return make_response(jsonify({"error": "An account with this email address already exists."}), 409) # 409 Conflict
    except Exception as e:
         logging.error(f"Database error checking email '{email}': {e}", exc_info=True)
         return make_response(jsonify({"error": "Server error during registration check (email)."}), 500)

    # 4. --- Hash Password ---
    try:
        hashed_pw = generate_password_hash(password, method='pbkdf2:sha256')
    except Exception as e:
        logging.error(f"Password hashing failed: {e}", exc_info=True)
        return make_response(jsonify({"error": "Server error during registration processing."}), 500)

    # 5. --- Prepare User Document ---
    # <<< Updated document structure >>>
    new_user_data = {
        "username": username,
        "email": email,
        "password_hash": hashed_pw,
        "created_at": datetime.now(timezone.utc),
        "agreed_terms": agreeTerms,
        "understand_privacy": understand_privacy
    }

    # 6. --- Save User to Database ---
    try:
        # <<< Using the specific database function >>>
        save_success, save_msg = database.save_user(new_user_data)

        if not save_success:
            logging.error(f"Failed to save new user '{username}' / '{email}': {save_msg}")
            # Enhanced duplicate key checking
            if "duplicate key error" in save_msg.lower():
                 if "username_1" in save_msg: # Check for username index name
                     return make_response(jsonify({"error": "Username is already taken."}), 409)
                 elif "email_1" in save_msg: # Check for email index name
                     return make_response(jsonify({"error": "An account with this email address already exists."}), 409)
                 else: # Fallback if index name unknown
                      return make_response(jsonify({"error": "Username or Email already exists."}), 409)
            else:
                 return make_response(jsonify({"error": "Server error saving registration. Please try again."}), 500)

    except Exception as e:
        logging.error(f"Unexpected error during database save for user '{username}': {e}", exc_info=True)
        return make_response(jsonify({"error": "Critical server error during final registration step."}), 500)

    # 7. --- Success Response ---
    logging.info(f"User '{username}' registered successfully.")
    return make_response(jsonify({
        "message": "Registration successful!",
        # <<< Return username >>>
        "user": {"username": username, "email": email}
    }), 201) # 201 Created

    
# --- Flask Routes ---
@app.route('/')
def index() -> str:
    logging.info("Serving index page.")
    try:
        # Pass current_user to the template so it knows if someone is logged in
        return render_template('index.html', current_user=current_user)
    except Exception as e:
        logging.error(f"Error rendering index.html: {e}", exc_info=True)
        return make_response("Error loading page.", 500)
    
# In routes.py

@app.route('/initiate-upload', methods=['POST'])
def initiate_upload() -> Response:
    upload_id = str(uuid.uuid4())
    log_prefix = f"[{upload_id}]"
    logging.info(f"{log_prefix} Request to initiate upload.")

    # Get the list of files. This handles one or more files.
    uploaded_files = request.files.getlist('files[]')

    # Check if any files were actually sent
    if not uploaded_files or all(not f.filename for f in uploaded_files):
        logging.warning(f"{log_prefix} Initiate upload failed: No files provided or files have no names.")
        return jsonify({"error": "No files selected or files are invalid"}), 400

    # --- User Information ---
    # is_anonymous_upload = not current_user.is_authenticated # This check is fine
    # For a @login_required route, current_user.is_authenticated will always be true.
    user_email = current_user.email
    display_username = current_user.username
    logging.info(f"{log_prefix} Authenticated upload initiated by user: Email='{user_email}', Username='{display_username}'")
    # --- End User Information ---

    batch_temp_dir = os.path.join(UPLOADS_TEMP_DIR, f"batch_{upload_id}")
    original_filenames_in_batch = []

    try:
        os.makedirs(batch_temp_dir, exist_ok=True)
        logging.info(f"{log_prefix} Created batch temporary directory: {batch_temp_dir}")

        for file_storage_item in uploaded_files:
            if file_storage_item and file_storage_item.filename:
                # IMPORTANT: For security, you should sanitize filenames in production
                # from werkzeug.utils import secure_filename
                # original_filename = secure_filename(file_storage_item.filename)
                # if not original_filename: # Handle cases where filename becomes empty after sanitization
                #     original_filename = f"unnamed_file_{uuid.uuid4().hex[:6]}"
                original_filename = file_storage_item.filename # Using directly for now

                individual_temp_file_path = os.path.join(batch_temp_dir, original_filename)
                file_storage_item.save(individual_temp_file_path)
                original_filenames_in_batch.append(original_filename)
                logging.info(f"{log_prefix} Saved '{original_filename}' to batch directory: {batch_temp_dir}")
            else:
                logging.warning(f"{log_prefix} Skipped an invalid file item in the batch.")

        if not original_filenames_in_batch:
            _safe_remove_directory(batch_temp_dir, log_prefix, "empty batch temp dir")
            logging.warning(f"{log_prefix} No valid files were saved in the batch.")
            return jsonify({"error": "No valid files were processed in the batch."}), 400

        batch_display_name = f"batch_{upload_id}.zip"
        # For the SSE 'start' event, it's better if the client knows it's a batch.
        # The actual name of the file (the zip) will be determined by the server.
        # So, batch_display_name is good for the response here.
        # If you want to be more specific for single file uploads (even if handled as a batch of 1):
        # if len(original_filenames_in_batch) == 1:
        #    batch_display_name = original_filenames_in_batch[0] # Or keep it as batch_upload_id.zip
                                                                # The client will get the actual zip name
                                                                # from 'start' event in process_upload_...

        upload_progress_data[upload_id] = {
            "status": "initiated",
            "is_batch": True, # Crucial flag for the next step
            "batch_directory_path": batch_temp_dir,
            "original_filenames_in_batch": original_filenames_in_batch,
            "batch_display_name": batch_display_name, # This will be the name of the zip file to be created
            "username": display_username,
            "user_email": user_email,
            "is_anonymous": False, # Since route is @login_required
            "error": None,
            "start_time": time.time()
        }
        logging.debug(f"{log_prefix} Initial batch progress data stored: {upload_progress_data[upload_id]}")

        return jsonify({"upload_id": upload_id, "filename": batch_display_name})

    except Exception as e:
        logging.error(f"{log_prefix} Error processing batch upload: {e}", exc_info=True)
        if os.path.exists(batch_temp_dir): # Ensure batch_temp_dir is defined
            _safe_remove_directory(batch_temp_dir, log_prefix, "failed batch temp dir")
        if upload_id in upload_progress_data:
            del upload_progress_data[upload_id]
        return jsonify({"error": f"Server error processing batch: {str(e)}"}), 500
    
@app.route('/stream-progress/<upload_id>')
def stream_progress(upload_id: str) -> Response:
    logging.info(f"SSE connect request for upload_id: {upload_id}")
    status = upload_progress_data.get(upload_id, {}).get('status', 'unknown')
    if upload_id not in upload_progress_data or status in ['completed', 'error', 'completed_metadata_error']:
        logging.warning(f"Upload ID '{upload_id}' unknown or finalized (Status:{status}).")
        def stream_gen(): yield _yield_sse_event('error', {'message': f'Upload ID {upload_id} unknown/finalized.'})
        return Response(stream_with_context(stream_gen()), mimetype='text/event-stream')
    return Response(stream_with_context(process_upload_and_generate_updates(upload_id)), mimetype='text/event-stream')

# # Example simplification in server.py (TEMPORARY)
# @app.route('/stream-progress/<upload_id>')
# def stream_progress(upload_id):
#     print(f"SSE stream requested for ID: {upload_id}") # Log request
#     def event_stream():
#                 # Just send a connected message and keep connection open (for testing)
#                 yield f"event: status\ndata: {json.dumps({'message': 'SSE Connected'})}\n\n"
# # Keep connection open without doing real work
#                 while True:
#                     time.sleep(30) # Keep alive ping essentially
#                     yield ": keepalive\n\n" # Send a comment to keep connection alive
#                     try:
#                         return Response(event_stream(), mimetype='text/event-stream')
#                     except Exception as e:
#                         print(f"Error during SSE stream for {upload_id}: {e}")
# # Return an error response if the stream setup fails
#                     return jsonify({"message": f"Failed to start stream: {e}"}), 500


# --- Helper Functions ---
def _yield_sse_event(event_type: str, data: Dict[str, Any]) -> SseEvent:
    json_data = json.dumps(data); return f"event: {event_type}\ndata: {json_data}\n\n"

def _send_single_file_task(file_bytes: bytes, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
    try:
        buffer = io.BytesIO(file_bytes)
        logging.info(f"[{upload_id}] T> Sending '{filename}' ({len(file_bytes)}b) to {chat_id}")
        result = send_file_to_telegram(buffer, filename, chat_id)
        buffer.close()
        logging.info(f"[{upload_id}] T> Sent '{filename}' to {chat_id}. Success: {result[0]}")
        return str(chat_id), result
    except Exception as e: logging.error(f"[{upload_id}] T> Err send single file to {chat_id}: {e}", exc_info=True); return str(chat_id), (False, f"Thread error: {e}", None)

# In routes.py, usually with other helper functions

def _safe_remove_directory(dir_path: str, log_prefix: str, description: str):
    """Safely removes a directory and its contents."""
    if not dir_path or not isinstance(dir_path, str):
        logging.warning(f"[{log_prefix}] Attempted to remove invalid directory path for {description}: {dir_path}")
        return
    if os.path.isdir(dir_path): # Check if it's actually a directory
        try:
            shutil.rmtree(dir_path) # shutil.rmtree removes a directory and all its contents
            logging.info(f"[{log_prefix}] Cleaned up {description}: {dir_path}")
        except OSError as e:
            logging.error(f"[{log_prefix}] Error deleting {description} directory '{dir_path}': {e}", exc_info=True)
    elif os.path.exists(dir_path): # It exists but is not a directory
        logging.warning(f"[{log_prefix}] Cleanup skipped for {description}, path exists but is not a directory: {dir_path}")
    else:
        logging.debug(f"[{log_prefix}] Cleanup skipped, {description} directory not found: {dir_path}")

def _schedule_cleanup(temp_id: str, path: Optional[str]):
    """Safely cleans up temporary download file and state data."""
    log_prefix = f"Cleanup-{temp_id}"
    logging.info(f"[{log_prefix}] Scheduled cleanup executing for path: {path}")
    if path:
        _safe_remove_file(path, log_prefix, "final dl") # Reuse existing safe remove

    if temp_id in download_prep_data:
        logging.debug(f"[{log_prefix}] Removing prep data.")
        try:
            del download_prep_data[temp_id]
            logging.info(f"[{log_prefix}] Prep data removed.")
        except KeyError:
            logging.warning(f"[{log_prefix}] Prep data already removed before cleanup task.")
    else:
        logging.warning(f"[{log_prefix}] Prep data not found during scheduled cleanup.")

def _send_chunk_task(chunk_data: bytes, filename: str, chat_id: str, upload_id: str, chunk_num: int) -> Tuple[str, ApiResult]:
    try:
        buffer = io.BytesIO(chunk_data); logging.info(f"[{upload_id}] T> Sending chunk {chunk_num} ('{filename}') to {chat_id}")
        result = send_file_to_telegram(buffer, filename, chat_id); buffer.close()
        logging.info(f"[{upload_id}] T> Sent chunk {chunk_num} to {chat_id}. Success: {result[0]}")
        return str(chat_id), result
    except Exception as e: logging.error(f"[{upload_id}] T> Err send chunk {chunk_num} to {chat_id}: {e}", exc_info=True); return str(chat_id), (False, f"Thread error: {e}", None)

def _download_chunk_task(file_id: str, part_num: int, prep_id: str) -> ChunkDataResult:
    logging.info(f"[{prep_id}] T> Starting download chunk {part_num} (id: {file_id})")
    try:
        content, err_msg = download_telegram_file_content(file_id)
        if err_msg: logging.error(f"[{prep_id}] T> API Err dl chunk {part_num}: {err_msg}"); return part_num, None, err_msg
        elif not content: logging.error(f"[{prep_id}] T> Err dl chunk {part_num}: Empty content."); return part_num, None, "Empty chunk content."
        else: logging.info(f"[{prep_id}] T> OK dl chunk {part_num} ({len(content)} bytes)."); return part_num, content, None
    except Exception as e: logging.error(f"[{prep_id}] T> Unexp err dl chunk {part_num}: {e}", exc_info=True); return part_num, None, f"Thread error: {e}"

def _parse_send_results(log_prefix: str, send_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    all_chat_details = []
    for res in send_results:
        detail: Dict[str, Union[str, int, bool, None]] = {"chat_id": res["chat_id"], "success": res["success"]}
        if res["success"] and res["tg_response"]:
            res_data = res["tg_response"].get('result', {})
            msg_id = res_data.get('message_id'); doc_data = res_data.get('document', {})
            f_id = doc_data.get('file_id'); f_uid = doc_data.get('file_unique_id'); f_size = doc_data.get('file_size')
            if msg_id and f_id and f_uid:
                detail["message_id"] = msg_id; detail["file_id"] = f_id; detail["file_unique_id"] = f_uid
                if f_size is not None: detail["file_size"] = f_size
            else: detail["success"] = False; detail["error"] = "Missing IDs in TG response"; logging.warning(f"[{log_prefix}] Missing IDs: {res['tg_response']}")
        elif not res["success"]: detail["error"] = res["message"]
        all_chat_details.append(detail)
    return all_chat_details

def _calculate_progress(start_time: float, bytes_done: int, total_bytes: int) -> Dict[str, Any]:
    progress = {"bytesSent": bytes_done, "totalBytes": total_bytes, "percentage": 0, "speedMBps": 0, "etaFormatted": "--:--", "etaSeconds": -1}
    if total_bytes <= 0: return progress
    progress["percentage"] = min((bytes_done / total_bytes) * 100, 100)
    elapsed = time.time() - start_time
    if elapsed > 0.1 and bytes_done > 0:
        speed_bps = bytes_done / elapsed; progress["speedMBps"] = speed_bps / (1024 * 1024)
        remaining = total_bytes - bytes_done
        if remaining > 0 and speed_bps > 0:
            eta_sec = remaining / speed_bps; progress["etaSeconds"] = eta_sec; progress["etaFormatted"] = format_time(eta_sec)
        else: progress["percentage"] = 100; progress["etaSeconds"] = 0; progress["etaFormatted"] = "00:00"
    elif bytes_done == total_bytes: progress["percentage"] = 100; progress["etaSeconds"] = 0; progress["etaFormatted"] = "00:00"
    return progress

def _find_best_telegram_file_id(locations: List[Dict[str, Any]], primary_chat_id: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    pf, ff = None, None; pcid_str = str(primary_chat_id) if primary_chat_id else None
    for loc in locations:
        if loc.get('success') and loc.get('file_id'):
            cid = str(loc.get('chat_id')); fid = loc.get('file_id')
            if pcid_str and cid == pcid_str: pf = (fid, cid); break
            elif not ff: ff = (fid, cid)
    if pf: return pf;
    elif ff: return ff;
    else: return None, None
def _find_filename_in_zip(zf: zipfile.ZipFile, expected: str, prefix: str) -> str:
    names = zf.namelist();
    if not names: raise ValueError("Zip empty.")
    if expected in names: return expected
    if len(names) == 1: actual = names[0]; logging.warning(f"[{prefix}] Expected '{expected}' not in zip. Using only entry: '{actual}'"); return actual
    base, _ = os.path.splitext(expected);
    for name in names:
        if name == base: logging.warning(f"[{prefix}] Expected '{expected}' not found. Using match: '{name}'"); return name
    raise ValueError(f"Cannot find '{expected}' in zip ({names})")

def _calculate_download_fetch_progress(start: float, fetched: int, total_fetch: int, done_count: int, total_count: int, base_perc: float, final_size: int) -> Dict[str, Any]:
    prog = {'percentage': base_perc, 'bytesProcessed': fetched, 'totalBytes': total_fetch if total_fetch > 0 else 0, 'speedMBps': 0, 'etaFormatted': '--:--', 'displayTotalBytes': final_size }
    elapsed = time.time() - start
    if elapsed > 0.1 and fetched > 0:
        speed_bps = fetched / elapsed; prog['speedMBps'] = speed_bps / (1024*1024)
        if total_fetch > 0 and speed_bps > 0:
            remaining_bytes = total_fetch - fetched;
            if remaining_bytes > 0: prog['etaFormatted'] = format_time(remaining_bytes / speed_bps)
            else: prog['etaFormatted'] = "00:00"
        elif speed_bps > 0 and done_count > 0:
            remaining_chunks = total_count - done_count;
            if remaining_chunks > 0:
                time_per_chunk = elapsed / done_count;
                eta_seconds = remaining_chunks * time_per_chunk; prog['etaFormatted'] = format_time(eta_seconds)
            else: prog['etaFormatted'] = "00:00"
    return prog

def _safe_remove_file(path: str, prefix: str, desc: str):
    if not path or not isinstance(path, str):
         logging.warning(f"[{prefix}] Attempted remove invalid path for {desc}: {path}")
         return
    if os.path.exists(path):
        try: os.remove(path); logging.info(f"[{prefix}] Cleaned up {desc}: {path}")
        except OSError as e: logging.error(f"[{prefix}] Error deleting {desc} '{path}': {e}", exc_info=True)
    else: logging.debug(f"[{prefix}] Cleanup skipped, {desc} file not found: {path}")

# --- Upload Core Logic ---
# In routes.py

# In routes.py

def process_upload_and_generate_updates(upload_id: str) -> Generator[SseEvent, None, None]:
    try:
        log_prefix = f"[{upload_id}]"
        logging.info(f"{log_prefix} Starting processing generator (Batch Individual File Send)...")
        upload_data = upload_progress_data.get(upload_id)

        if not upload_data:
            logging.error(f"{log_prefix} Critical: Upload data missing for ID.")
            yield _yield_sse_event('error', {'message': 'Internal error: Upload data not found.'})
            return

        # --- Get data prepared by initiate_upload ---
        is_batch_upload = upload_data.get("is_batch", False) # Should always be True now
        username = upload_data['username']
        batch_directory_path = upload_data.get("batch_directory_path")
        original_filenames_in_batch = upload_data.get("original_filenames_in_batch", [])
        # Use a more descriptive name for the batch itself, maybe based on first file?
        batch_display_name = f"Upload ({len(original_filenames_in_batch)} files)"
        if original_filenames_in_batch:
            batch_display_name = f"{original_filenames_in_batch[0]} (+{len(original_filenames_in_batch)-1} others)" if len(original_filenames_in_batch) > 1 else original_filenames_in_batch[0]


        if not is_batch_upload or not batch_directory_path or not os.path.isdir(batch_directory_path) or not original_filenames_in_batch:
            # If initiate_upload was modified correctly, this shouldn't happen for multi-file uploads
            logging.error(f"{log_prefix} Invalid batch data. is_batch={is_batch_upload}, dir={batch_directory_path}, files={original_filenames_in_batch}")
            yield _yield_sse_event('error', {'message': 'Internal error: Invalid batch data.'})
            if batch_directory_path and os.path.isdir(batch_directory_path): # Cleanup if dir exists
                _safe_remove_directory(batch_directory_path, log_prefix, "invalid batch dir")
            return

        logging.info(f"{log_prefix} Processing batch: User='{username}', Dir='{batch_directory_path}', Files={original_filenames_in_batch}")
        upload_data['status'] = 'processing_telegram'

        access_id: Optional[str] = upload_data.get('access_id') # Get access_id generated earlier
        if not access_id:
            access_id = uuid.uuid4().hex[:10] # Generate if missing (shouldn't happen ideally)
            upload_data['access_id'] = access_id
            logging.warning(f"{log_prefix} Access ID was missing, generated new one: {access_id}")

        executor: Optional[ThreadPoolExecutor] = None
        if len(TELEGRAM_CHAT_IDS) > 1:
            executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'Upload_{upload_id[:4]}')
            logging.info(f"{log_prefix} Initialized Upload Executor (max={MAX_UPLOAD_WORKERS})")

        # --- Calculate total original size for progress ---
        total_original_bytes_in_batch = 0
        files_to_process_details = []
        for filename in original_filenames_in_batch:
            file_path = os.path.join(batch_directory_path, filename)
            if os.path.exists(file_path):
                try:
                    size = os.path.getsize(file_path)
                    total_original_bytes_in_batch += size
                    files_to_process_details.append({"path": file_path, "name": filename, "size": size})
                except OSError as e:
                    logging.warning(f"{log_prefix} Could not get size for {filename}, skipping. Error: {e}")
            else:
                logging.warning(f"{log_prefix} File {filename} not found in batch dir, skipping.")

        if not files_to_process_details:
            yield _yield_sse_event('error', {'message': 'No valid files found to upload in the batch.'})
            _safe_remove_directory(batch_directory_path, log_prefix, "empty batch dir")
            return

        # Yield start event based on the batch
        yield _yield_sse_event('start', {'filename': batch_display_name, 'totalSize': total_original_bytes_in_batch})
        yield _yield_sse_event('status', {'message': f'Uploading {len(files_to_process_details)} files...'})

        # --- Process and send each file individually ---
        overall_start_time = time.time()
        bytes_sent_so_far = 0
        all_files_metadata_for_db = [] # Store metadata for each successfully processed file
        all_succeeded = True # Assume success until a primary send fails

        for file_detail in files_to_process_details:
            current_file_path = file_detail["path"]
            current_filename = file_detail["name"]
            current_file_size = file_detail["size"]
            log_file_prefix = f"{log_prefix} File '{current_filename}'"

            logging.info(f"{log_file_prefix} Starting send process.")

            if current_file_size == 0:
                logging.warning(f"{log_file_prefix} is empty, skipping send but including in metadata.")
                # Add placeholder metadata indicating it was skipped due to size
                all_files_metadata_for_db.append({
                    "original_filename": current_filename,
                    "original_size": 0,
                    "skipped": True,
                    "reason": "File is empty",
                    "send_locations": []
                })
                # Update progress immediately (consider 0-byte file as 'sent' for progress count)
                bytes_sent_so_far += 0 # Add 0 bytes
                progress_data = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
                yield _yield_sse_event('progress', progress_data)
                continue # Move to the next file

            # --- Send this individual file to all chats ---
            file_send_start_time = time.time()
            file_specific_futures: Dict[Future, str] = {}
            file_specific_results: Dict[str, ApiResult] = {}
            primary_send_success_for_this_file = False
            primary_send_message = "Primary send not attempted or failed."

            try:
                with open(current_file_path, 'rb') as f_current:
                    file_bytes = f_current.read() # Read the whole file (adjust if files can be > memory)

                if executor:
                    for chat_id_str in TELEGRAM_CHAT_IDS:
                        cid = str(chat_id_str)
                        # Use _send_single_file_task as file size < CHUNK_SIZE assumption for individual files might be wrong.
                        # If individual files can be > CHUNK_SIZE, this needs chunking logic *here* per file.
                        # Assuming individual files are reasonably sized for now.
                        # *** If individual files can exceed CHUNK_SIZE, this part needs replacement with chunking logic ***
                        if current_file_size > CHUNK_SIZE:
                            # TODO: Implement chunking logic here for the individual file `file_bytes`
                            logging.error(f"{log_file_prefix} is larger than CHUNK_SIZE ({current_file_size} > {CHUNK_SIZE}), chunking not implemented for individual batch files yet. Aborting file.")
                            # Mark this file as failed
                            file_specific_results[cid] = (False, "File too large for single send, chunking not implemented here", None)
                            # Maybe don't raise, just record failure for this file?
                            # raise NotImplementedError("Chunking for large individual files within a batch is not yet implemented.")
                            # Let's record failure and continue to next file for now
                            all_succeeded = False # Mark overall batch potentially incomplete
                            break # Break inner chat_id loop for this large file

                        fut = executor.submit(_send_single_file_task, file_bytes, current_filename, cid, upload_id)
                        file_specific_futures[fut] = cid
                else: # No executor
                    cid = str(TELEGRAM_CHAT_IDS[0])
                    _, res = _send_single_file_task(file_bytes, current_filename, cid, upload_id)
                    file_specific_results[cid] = res
                    if cid == str(PRIMARY_TELEGRAM_CHAT_ID):
                        primary_send_success_for_this_file = res[0]
                        primary_send_message = res[1]
                        if not primary_send_success_for_this_file:
                            all_succeeded = False # Primary failed for this file

                # --- Wait for results (similar to previous logic) ---
                if executor:
                    primary_fut: Optional[Future] = None
                    primary_cid = str(PRIMARY_TELEGRAM_CHAT_ID)
                    for fut_key, chat_id_val in file_specific_futures.items():
                        if chat_id_val == primary_cid: primary_fut = fut_key; break
                    
                    if primary_fut:
                        logging.debug(f"{log_file_prefix} Waiting for primary send...")
                        cid_res, res = primary_fut.result(); file_specific_results[cid_res] = res
                        primary_send_success_for_this_file = res[0]
                        primary_send_message = res[1]
                        if not primary_send_success_for_this_file:
                            all_succeeded = False # Mark overall failure if primary fails
                            logging.error(f"{log_file_prefix} Primary send failed: {primary_send_message}")
                            # Decide if you want to stop processing the rest of the files in the batch
                            # For now, we'll continue and record which files failed.
                    else:
                        logging.warning(f"{log_file_prefix} Primary future not found.")
                        # If primary future wasn't found, we cannot guarantee overall success based on it
                        all_succeeded = False

                    logging.debug(f"{log_file_prefix} Waiting for backup sends...")
                    for fut_completed in as_completed(file_specific_futures):
                        cid_res, res = fut_completed.result()
                        if cid_res not in file_specific_results: file_specific_results[cid_res] = res

                # --- Process results for *this specific file* ---
                current_file_send_report = [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k, r in file_specific_results.items()]
                
                if primary_send_success_for_this_file:
                    bytes_sent_so_far += current_file_size # Increment progress only if primary succeeded
                    parsed_locations = _parse_send_results(f"{log_prefix}-{current_filename}", current_file_send_report)
                    # Check if parsing marked primary as failed due to missing IDs
                    primary_parsed_loc = next((loc for loc in parsed_locations if loc.get('chat_id') == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                    if not primary_parsed_loc or not primary_parsed_loc.get('success'):
                        logging.error(f"{log_file_prefix} Primary send reported OK by API but parsing failed or IDs missing. Marking as failed.")
                        all_succeeded = False # Treat as overall failure if primary metadata is bad
                        # Add failure metadata? Or just skip adding this file? Let's skip adding successful record.
                        # Add failure record instead?
                        all_files_metadata_for_db.append({
                            "original_filename": current_filename, "original_size": current_file_size,
                            "skipped": False, "failed": True,
                            "reason": "Primary send metadata parsing failed.",
                            "send_locations": parsed_locations # Include potentially partial data
                        })

                    else:
                        # Primary succeeded AND metadata looks okay
                        all_files_metadata_for_db.append({
                            "original_filename": current_filename,
                            "original_size": current_file_size,
                            "send_locations": parsed_locations
                            # Add other per-file details if needed later
                        })
                        logging.info(f"{log_file_prefix} Successfully processed and recorded.")
                else:
                    # Primary send failed for this file
                    all_succeeded = False # Mark the whole batch potentially incomplete
                    logging.error(f"{log_file_prefix} Failed primary send. Reason: {primary_send_message}")
                    # Add failure record for this file
                    all_files_metadata_for_db.append({
                        "original_filename": current_filename, "original_size": current_file_size,
                        "skipped": False, "failed": True,
                        "reason": f"Primary send failed: {primary_send_message}",
                        "send_locations": _parse_send_results(f"{log_prefix}-{current_filename}", current_file_send_report) # Include attempts
                    })
                    # Optional: break loop here if one failure should stop the whole batch?

                # Update progress after each file is processed (based on primary success)
                progress_data = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
                yield _yield_sse_event('progress', progress_data)
                yield _yield_sse_event('status', {'message': f'Processed {len(all_files_metadata_for_db)} of {len(files_to_process_details)} files...'})

            except FileNotFoundError:
                logging.error(f"{log_file_prefix} not found during processing loop.", exc_info=True)
                all_succeeded = False
                all_files_metadata_for_db.append({ "original_filename": current_filename, "failed": True, "reason": "File not found during send phase."})
            except NotImplementedError as nie:
                logging.error(f"{log_file_prefix} - {nie}")
                all_succeeded = False
                all_files_metadata_for_db.append({ "original_filename": current_filename, "failed": True, "reason": str(nie)})
            except Exception as file_loop_error:
                logging.error(f"{log_file_prefix} Unexpected error during send: {file_loop_error}", exc_info=True)
                all_succeeded = False # Mark overall failure
                all_files_metadata_for_db.append({ "original_filename": current_filename, "failed": True, "reason": f"Unexpected error: {str(file_loop_error)}"})
                # Optional: break here? Or try next file? Continue for now.

            # --- Batch Upload Finished ---
            total_batch_duration = time.time() - overall_start_time
            logging.info(f"{log_prefix} Finished processing all files in batch. Duration: {total_batch_duration:.2f}s. Overall Success (based on primary sends): {all_succeeded}")

            if not all_files_metadata_for_db:
                raise RuntimeError("Processing finished but no metadata was generated for any file.")

            # --- Save Batch Metadata ---
            db_batch_timestamp = datetime.now(timezone.utc).isoformat()
            db_batch_record = {
                "access_id": access_id,
                "username": username,
                "upload_timestamp": db_batch_timestamp,
                "is_batch": True,
                "batch_display_name": batch_display_name, # User-friendly name for the batch
                "files_in_batch": all_files_metadata_for_db, # List of dicts per file
                "total_original_size": total_original_bytes_in_batch, # Sum of original file sizes
                "total_upload_duration_seconds": round(total_batch_duration, 2),
                # Note: 'original_filename' at top level doesn't make sense for batch.
                # 'is_split', 'is_compressed', 'original_size' now live inside 'files_in_batch' if needed per file,
                # but for this model, the batch record itself isn't split/compressed in the same way.
            }

            save_success, save_msg = save_file_metadata(db_batch_record)
            if not save_success:
                logging.error(f"{log_prefix} DB CRITICAL: Failed to save batch metadata: {save_msg}")
                # Yield error even if Telegram sends were okay? Yes, link won't work.
                raise IOError(f"Failed to save batch metadata: {save_msg}")
            else:
                logging.info(f"{log_prefix} DB: Successfully saved batch metadata.")

            # --- Yield Completion Event (Pointing to the new browser route) ---
            browser_url = url_for('browse_batch', access_id=access_id, _external=True) # New route needed
            yield _yield_sse_event('complete', {
                'message': f'Batch upload ({len(files_to_process_details)} files) complete!',
                'download_url': browser_url, # URL to the file browser page
                'filename': batch_display_name # Display name of the batch
            })
            upload_data['status'] = 'completed' if all_succeeded else 'completed_with_errors'


    except Exception as e:
        error_msg_final = f"Batch upload processing failed: {str(e) or type(e).__name__}"
        logging.error(f"{log_prefix} {error_msg_final}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_msg_final})
        if upload_id in upload_progress_data:
                upload_data['status'] = 'error'
                upload_data['error'] = error_msg_final
    finally:
        logging.info(f"{log_prefix} Batch upload generator final cleanup.")
        if executor:
            executor.shutdown(wait=False)
            logging.info(f"{log_prefix} Upload executor shutdown.")
        
        # Cleanup: The original batch directory should be gone.
        # If we created an intermediate zip (which we removed in this version), it would be cleaned here.
        # Check if the batch dir still exists for some reason (e.g., error before cleanup)
        if batch_directory_path and os.path.exists(batch_directory_path):
             logging.warning(f"{log_prefix} Cleaning up batch directory that might have been left over: {batch_directory_path}")
             _safe_remove_directory(batch_directory_path, log_prefix, "lingering batch dir")

        final_status_report = upload_progress_data.get(upload_id, {}).get('status', 'unknown')
        logging.info(f"{log_prefix} Batch processing generator finished. Final Status: {final_status_report}")

# Download Preparation Route ---
@app.route('/prepare-download/<username>/<path:filename>') # <-- Use path converter
def prepare_download_stream(username: str, filename: str) -> Response:
    """
    SSE endpoint. Initiates download preparation for a given user/filename
    and streams status updates. Allows slashes in filename.
    """
    prep_id = str(uuid.uuid4())

    logging.info(f"[{prep_id}] SSE download prep request: User='{username}', File='{filename}'")
    download_prep_data[prep_id] = {
        "prep_id": prep_id, "status": "initiated", "username": username,
        "requested_filename": filename, "original_filename": filename,
        "access_id": None, "error": None, "final_temp_file_path": None,
        "final_file_size": 0, "start_time": time.time()
    }
    logging.debug(f"[{prep_id}] Stored initial download prep data.")

    return Response(stream_with_context(
        _prepare_download_and_generate_updates(prep_id)
    ), mimetype='text/event-stream')

@app.route('/stream-download/<access_id>')
def stream_download_by_access_id(access_id: str) -> Response:
    """
    SSE endpoint to stream download preparation status initiated by access_id.
    """
    prep_id = str(uuid.uuid4()) # Unique ID for *this* preparation task
    logging.info(f"[{prep_id}] SSE connection request for dl prep via access_id: {access_id}")
    file_info, error_msg = find_metadata_by_access_id(access_id)
    username = file_info.get('username')
    # --- Basic Metadata Lookup (to ensure ID is valid before streaming) ---

    if not file_info or not username:
         # Handle case where access_id is not found
         logging.warning(f"[{prep_id}] Invalid access_id '{access_id}' for SSE stream.")
         def error_stream(): yield _yield_sse_event('error', {'message':'Invalid or expired download link.'})
         return Response(stream_with_context(error_stream()), mimetype='text/event-stream')

    # Store initial info needed by the generator, using the new prep_id
    download_prep_data[prep_id] = {
         "prep_id": prep_id,
         "status": "initiated",
         "username": username, # Found from metadata
         "requested_filename": file_info.get('original_filename', 'unknown'), # Use filename from metadata
         "original_filename": file_info.get('original_filename', 'unknown'), # Store original name
         "access_id": access_id, # Store the access_id that initiated this
         "error": None,
         "final_temp_file_path": None,
         "final_file_size": 0,
         "start_time": time.time()
    }
    logging.debug(f"[{prep_id}] Stored initial prep data for access_id lookup. Calling generator.")

    # Call the main generator function using the prep_id
    # This generator will handle the actual download/prep and yield SSE events
    return Response(stream_with_context(
         _prepare_download_and_generate_updates(prep_id)
    ), mimetype='text/event-stream')
# --- End of new route function ---

# In routes.py

def _prepare_download_and_generate_updates(prep_id: str) -> Generator[SseEvent, None, None]:
    """
    Generator handling download preparation and yielding SSE updates.
    Handles:
    1. Lookup by access_id/username+filename (for ZIPs or single-file records).
    2. Direct download of a single file using a pre-supplied telegram_file_id.
    """
    log_prefix = f"[DLPrep-{prep_id}]"
    logging.info(f"{log_prefix} Download prep generator started.")
    prep_data = download_prep_data.get(prep_id)

    # --- Initial Checks ---
    if not prep_data:
        logging.error(f"{log_prefix} Critical: Prep data missing at generator start.")
        yield _yield_sse_event('error', {'message': 'Internal Server Error: Prep data lost.'})
        return

    if prep_data.get('status') != 'initiated':
        logging.warning(f"{log_prefix} Prep already running/finished (Status: {prep_data.get('status')}). Aborting.")
        # Optionally yield an error/status event?
        # yield _yield_sse_event('status', {'message': 'Preparation already in progress or finished.'})
        return

    prep_data['status'] = 'preparing'

    # --- Initialize variables ---
    temp_reassembled_zip_path: Optional[str] = None
    temp_decompressed_path: Optional[str] = None # Primarily used if we decompress a downloaded zip
    temp_final_file_path: Optional[str] = None # Path to the file that will be served

    file_info: Optional[Dict[str, Any]] = None # Holds metadata if looked up
    username = prep_data.get('username')
    access_id = prep_data.get('access_id')
    requested_filename = prep_data.get('requested_filename') # Filename requested by user/link

    # Check if called for single file download directly (via /download-single/...)
    direct_telegram_file_id = prep_data.get('telegram_file_id')
    is_direct_single_file_download = bool(direct_telegram_file_id)

    # --- Set initial values, potentially overridden by metadata lookup ---
    original_filename_from_meta = requested_filename or "download" # Default/fallback
    final_expected_size = prep_data.get('final_expected_size', 0) # Expected size (original size)
    is_split = False # Default
    # Default to False for single file downloads, True for looked-up records (usually zips/parts)
    is_compressed = False if is_direct_single_file_download else True

    total_bytes_to_fetch = 0 # For progress reporting during multi-chunk downloads
    download_executor: Optional[ThreadPoolExecutor] = None

    try:
        # --- Phase 1: Get File Info (Lookup or Direct) ---
        if not is_direct_single_file_download:
            # --- Metadata Lookup Path (for /prepare-download or /stream-download) ---
            yield _yield_sse_event('status', {'message': 'Looking up file info...'}); time.sleep(0.1)
            lookup_error_msg = ""
            if access_id:
                logging.debug(f"{log_prefix} Finding metadata by access_id: {access_id}")
                file_info, lookup_error_msg = find_metadata_by_access_id(access_id)
                if not file_info and not lookup_error_msg: lookup_error_msg = f"Access ID '{access_id}' not found."
                elif file_info and not file_info.get('username'): # Check if username exists in found record
                     lookup_error_msg = "Record found but missing username field."
                     file_info = None # Treat as error
            elif username and requested_filename: # Fallback to username/filename
                logging.debug(f"{log_prefix} Finding metadata by username/filename: User='{username}', File='{requested_filename}'")
                all_user_files, lookup_error_msg = find_metadata_by_username(username)
                if not lookup_error_msg and all_user_files is not None:
                    file_info = next((f for f in all_user_files if f.get('original_filename') == requested_filename), None)
                    if not file_info: lookup_error_msg = f"File '{requested_filename}' not found for user '{username}'."
                elif not lookup_error_msg: lookup_error_msg = "Internal error: Failed to retrieve user file list."
            else:
                lookup_error_msg = "Insufficient information for metadata lookup."

            if lookup_error_msg or not file_info:
                final_error_message = lookup_error_msg or "File metadata not found."
                logging.error(f"{log_prefix} Metadata lookup failed: {final_error_message}")
                raise FileNotFoundError(final_error_message)

            # Extract details from looked-up metadata
            original_filename_from_meta = file_info.get('original_filename', requested_filename or 'unknown');
            final_expected_size = file_info.get('original_size', 0)
            is_split = file_info.get('is_split', False)
            is_compressed = file_info.get('is_compressed', True) # Assume looked-up files were compressed unless specified otherwise
            prep_data['original_filename'] = original_filename_from_meta # Update prep_data
            prep_data['final_expected_size'] = final_expected_size
            logging.info(f"{log_prefix} Meta Found: '{original_filename_from_meta}', Size:{final_expected_size}, Split:{is_split}, Comp:{is_compressed}")
            # --- End of Metadata Lookup Path ---

        else:
            # --- Direct Single File Download Path ---
            logging.info(f"{log_prefix} Preparing direct download for file_id: {direct_telegram_file_id}")
            original_filename_from_meta = requested_filename # Use filename passed to the route
            # final_expected_size was already set in download_single_file route
            # Assume individual files are not split or compressed unless filename suggests otherwise
            is_split = False
            is_compressed = original_filename_from_meta.lower().endswith('.zip') # Simple check based on original name
            logging.info(f"{log_prefix} Direct Single File: '{original_filename_from_meta}', Size:{final_expected_size}, Split:{is_split}, Comp:{is_compressed}")
            # --- End of Direct Single File Path ---

        # --- Common Setup: Yield initial events ---
        yield _yield_sse_event('filename', {'filename': original_filename_from_meta})
        yield _yield_sse_event('totalSizeUpdate', {'totalSize': final_expected_size}) # Use original size for total display
        yield _yield_sse_event('progress', {'percentage': 0})
        yield _yield_sse_event('status', {'message': 'Preparing file...'}); time.sleep(0.2)

        # --- Phase 2: File Preparation ---
        if not is_split:
            # --- Prep Non-Split File ---
            logging.info(f"{log_prefix} Preparing non-split file '{original_filename_from_meta}'.")
            yield _yield_sse_event('status', {'message': 'Downloading...'})
            yield _yield_sse_event('progress', {'percentage': 5})

            tg_file_id_to_download = None
            if is_direct_single_file_download:
                tg_file_id_to_download = direct_telegram_file_id
                logging.info(f"{log_prefix} Using direct telegram_file_id: {tg_file_id_to_download}")
            elif file_info:
                locations = file_info.get('send_locations', [])
                tg_file_id_lookup, chat_id_lookup = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID)
                tg_file_id_to_download = tg_file_id_lookup
                if tg_file_id_to_download:
                    logging.info(f"{log_prefix} Using looked-up telegram_file_id: {tg_file_id_to_download} from chat {chat_id_lookup}")

            if not tg_file_id_to_download:
                raise ValueError("Could not determine Telegram file ID to download.")

            start_dl = time.time()
            content_bytes, err_msg = download_telegram_file_content(tg_file_id_to_download)
            dl_duration = time.time() - start_dl
            dl_bytes_count = len(content_bytes) if content_bytes else 0
            dl_speed_mbps = (dl_bytes_count / (1024*1024) / dl_duration) if dl_duration > 0 else 0
            logging.info(f"{log_prefix} TG download ({dl_bytes_count} bytes) in {dl_duration:.2f}s. Speed: {dl_speed_mbps:.2f} MB/s")

            if err_msg: raise ValueError(f"TG download failed: {err_msg}")
            if not content_bytes: raise ValueError("TG download returned empty content.")

            # Use expected size for progress display if known, otherwise use downloaded size
            progress_total = final_expected_size if final_expected_size > 0 else dl_bytes_count
            yield _yield_sse_event('progress', {'percentage': 50, 'bytesProcessed': int(progress_total*0.5), 'totalBytes': progress_total, 'speedMBps': dl_speed_mbps, 'etaFormatted': '00:00'})

            # Handle decompression ONLY if the metadata or filename indicates it's compressed
            if is_compressed:
                yield _yield_sse_event('status', {'message': 'Decompressing...'})
                logging.info(f"{log_prefix} Decompressing downloaded file...")
                # Create final temp file directly
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                    temp_final_file_path = tf.name
                    zf = None
                    try:
                        zip_buffer = io.BytesIO(content_bytes)
                        zf = zipfile.ZipFile(zip_buffer, 'r')
                        # Try to find the expected filename within the zip
                        inner_filename = _find_filename_in_zip(zf, original_filename_from_meta, prep_id)
                        logging.info(f"{log_prefix} Extracting '{inner_filename}' from zip.")
                        with zf.open(inner_filename, 'r') as inner_file_stream:
                            yield _yield_sse_event('progress', {'percentage': 75, 'bytesProcessed': int(progress_total*0.75), 'totalBytes': progress_total})
                            shutil.copyfileobj(inner_file_stream, tf) # Write decompressed content
                    finally:
                        if zf: zf.close()
                # No separate temp_decompressed_path needed, it's the final path
                yield _yield_sse_event('progress', {'percentage': 95, 'bytesProcessed': int(progress_total*0.95), 'totalBytes': progress_total})
            else: # Not compressed, save directly
                yield _yield_sse_event('status', {'message': 'Saving temporary file...'})
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                    temp_final_file_path = tf.name
                    tf.write(content_bytes)
                yield _yield_sse_event('progress', {'percentage': 95, 'bytesProcessed': int(dl_bytes_count*0.95), 'totalBytes': dl_bytes_count})

        else: # is_split is True
            if is_direct_single_file_download:
                 raise RuntimeError("Invalid state: Cannot process split file via direct single file download request.")

            # --- Prep Split File (only reached via metadata lookup) ---
            logging.info(f"{log_prefix} Preparing SPLIT file download for '{original_filename_from_meta}'")
            yield _yield_sse_event('status', {'message': 'Downloading & Reassembling chunks...'})

            chunks_meta = file_info.get('chunks');
            if not chunks_meta or not isinstance(chunks_meta, list): raise ValueError("Invalid 'chunks' metadata.")
            try: chunks_meta.sort(key=lambda c: int(c.get('part_number', 0)))
            except (TypeError, ValueError): raise ValueError("Invalid 'part_number' in chunks metadata.");
            num_chunks = len(chunks_meta)
            if num_chunks == 0: raise ValueError("Chunks list is empty in metadata.")

            total_bytes_to_fetch = file_info.get('compressed_total_size', 0) # Size of the compressed parts
            logging.info(f"{log_prefix} Expecting {num_chunks} chunks. Total compressed size: ~{format_bytes(total_bytes_to_fetch)}.")

            start_fetch_time = time.time()
            fetched_bytes_count = 0
            fetch_percentage_target = 80.0 # Allocate 80% of progress bar to fetching
            downloaded_chunk_count = 0

            # Setup ThreadPoolExecutor for concurrent downloads
            download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DlPrep_{prep_id[:4]}')
            logging.info(f"{log_prefix} Initialized Download Executor (max={MAX_DOWNLOAD_WORKERS})")
            submitted_futures: List[Future] = []
            downloaded_content_map: Dict[int, bytes] = {}
            first_download_error: Optional[str] = None

            logging.info(f"{log_prefix} Submitting {num_chunks} chunk download tasks...")
            for i, chunk_info in enumerate(chunks_meta):
                part_num = chunk_info.get('part_number')
                if part_num is None: raise ValueError(f"Chunk metadata missing 'part_number' at index {i}")
                chunk_locations = chunk_info.get('send_locations', [])
                if not chunk_locations: raise ValueError(f"Chunk {part_num} missing 'send_locations'.")
                chunk_file_id, chunk_chat_id = _find_best_telegram_file_id(chunk_locations, PRIMARY_TELEGRAM_CHAT_ID)
                if not chunk_file_id: raise ValueError(f"No usable source file_id found for chunk {part_num}.")
                submitted_futures.append(download_executor.submit(_download_chunk_task, chunk_file_id, part_num, prep_id))

            logging.info(f"{log_prefix} All {len(submitted_futures)} chunk download tasks submitted.")
            yield _yield_sse_event('status', {'message': f'Downloading {num_chunks} file parts...'})

            # Process completed futures
            progress_stats = {}
            for future in as_completed(submitted_futures):
                try:
                    pnum_result, content_result, err_result = future.result()
                    if err_result:
                        logging.error(f"{log_prefix} Failed download chunk {pnum_result}: {err_result}")
                        if not first_download_error: first_download_error = f"Chunk {pnum_result}: {err_result}"
                        # Continue processing other futures, but record the first error
                    elif content_result:
                        downloaded_chunk_count += 1
                        chunk_length = len(content_result)
                        fetched_bytes_count += chunk_length
                        downloaded_content_map[pnum_result] = content_result
                        logging.debug(f"{log_prefix} Downloaded chunk {pnum_result}. Count:{downloaded_chunk_count}/{num_chunks} ({chunk_length}b)")
                        # Calculate progress based on chunks completed and bytes fetched
                        overall_perc = (downloaded_chunk_count / num_chunks) * fetch_percentage_target
                        progress_stats = _calculate_download_fetch_progress(
                            start_fetch_time, fetched_bytes_count, total_bytes_to_fetch,
                            downloaded_chunk_count, num_chunks, overall_perc, final_expected_size
                        )
                        yield _yield_sse_event('progress', progress_stats)
                    else: # Should not happen if _download_chunk_task is correct
                        logging.error(f"{log_prefix} Task for chunk {pnum_result} returned invalid state (no content, no error).")
                        if not first_download_error: first_download_error = f"Chunk {pnum_result}: Internal task error."
                except Exception as e:
                    logging.error(f"{log_prefix} Error processing download future result: {e}", exc_info=True)
                    if not first_download_error: first_download_error = f"Error processing result: {str(e)}"

            if first_download_error: raise ValueError(f"Download failed: {first_download_error}")
            if downloaded_chunk_count != num_chunks: raise SystemError(f"Chunk download count mismatch. Expected:{num_chunks}, Got:{downloaded_chunk_count}.")

            logging.info(f"{log_prefix} All {num_chunks} chunks downloaded OK. Total fetched: {fetched_bytes_count} bytes.")
            yield _yield_sse_event('status', {'message': 'Reassembling file...'})

            # Reassemble downloaded chunks
            with tempfile.NamedTemporaryFile(suffix=".download.tmp", delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf_reassemble:
                temp_reassembled_zip_path = tf_reassemble.name
                logging.debug(f"{log_prefix} Reassembling into: {temp_reassembled_zip_path}")
                for pnum_write in range(1, num_chunks + 1):
                    chunk_content = downloaded_content_map.get(pnum_write)
                    if not chunk_content: raise SystemError(f"Missing content for chunk {pnum_write} during reassembly.")
                    tf_reassemble.write(chunk_content)
            downloaded_content_map.clear() # Free memory
            logging.info(f"{log_prefix} Finished reassembly.")
            # Update progress to fetch completion percentage
            yield _yield_sse_event('progress', {'percentage': fetch_percentage_target, 'bytesProcessed': fetched_bytes_count, 'totalBytes': total_bytes_to_fetch, 'speedMBps': progress_stats.get('speedMBps',0), 'etaFormatted':'00:00'})

            # Handle decompression if the original metadata indicated the *result* should be decompressed
            if is_compressed: # Check the flag determined from metadata
                yield _yield_sse_event('status', {'message': 'Decompressing...'})
                logging.info(f"{log_prefix} Decompressing reassembled file: {temp_reassembled_zip_path}")
                decomp_start_time = time.time()
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf_final:
                   temp_final_file_path = tf_final.name
                   zf = None
                   try:
                       zf = zipfile.ZipFile(temp_reassembled_zip_path, 'r')
                       # Use original_filename_from_meta (which is the zip name in this case)
                       inner_filename = _find_filename_in_zip(zf, original_filename_from_meta, prep_id)
                       logging.info(f"{log_prefix} Extracting '{inner_filename}' from reassembled zip.")
                       with zf.open(inner_filename, 'r') as inner_stream:
                           yield _yield_sse_event('progress', {'percentage': 90}) # Mid-decompression progress
                           shutil.copyfileobj(inner_stream, tf_final)
                   finally:
                       if zf: zf.close()
                decomp_duration = time.time() - decomp_start_time
                logging.info(f"{log_prefix} Decompression finished in {decomp_duration:.2f}s.")
                yield _yield_sse_event('progress', {'percentage': 98})
            else: # Split, but not compressed (less common, e.g., if original upload skipped compression)
                logging.info(f"{log_prefix} Using reassembled file directly (not compressed).")
                temp_final_file_path = temp_reassembled_zip_path
                temp_reassembled_zip_path = None # Prevent deletion in finally block
                yield _yield_sse_event('progress', {'percentage': 98})

        # --- Phase 3: Complete ---
        if not temp_final_file_path or not os.path.exists(temp_final_file_path):
            raise RuntimeError(f"{log_prefix} Failed to produce final temp file.")

        final_actual_size = os.path.getsize(temp_final_file_path)
        logging.info(f"{log_prefix} Final file ready: '{temp_final_file_path}', Size: {final_actual_size}.")

        # Compare final size with expected original size if available
        if final_expected_size > 0 and final_actual_size != final_expected_size:
            logging.warning(f"{log_prefix} Final size mismatch! Expected original size:{final_expected_size}, Actual final size:{final_actual_size}")
            # Decide if this is an error or just a warning

        # Update prep_data with final details
        prep_data['final_temp_file_path'] = temp_final_file_path
        prep_data['final_file_size'] = final_actual_size
        prep_data['status'] = 'ready'

        # Final progress update
        yield _yield_sse_event('progress', {'percentage': 100, 'bytesProcessed': final_actual_size, 'totalBytes': final_expected_size if final_expected_size else final_actual_size});
        yield _yield_sse_event('status', {'message': 'File ready!'}); time.sleep(0.1)

        # Send ready event
        yield _yield_sse_event('ready', {'temp_file_id': prep_id, 'final_filename': original_filename_from_meta})
        logging.info(f"{log_prefix} Prep complete. Sent 'ready' event.")

    except Exception as e:
        error_message = f"Download preparation failed: {str(e) or type(e).__name__}"
        logging.error(f"{log_prefix} {error_message}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_message})
        if prep_id in download_prep_data:
            download_prep_data[prep_id]['status'] = 'error'
            download_prep_data[prep_id]['error'] = error_message
    finally:
        # --- Cleanup ---
        logging.info(f"{log_prefix} Download prep generator cleanup.")
        if download_executor:
            download_executor.shutdown(wait=False)
            logging.info(f"{log_prefix} Download executor shutdown.")

        # Cleanup intermediate reassembled file only if it exists and IS NOT the final file
        if temp_reassembled_zip_path and temp_reassembled_zip_path != temp_final_file_path and os.path.exists(temp_reassembled_zip_path):
            _safe_remove_file(temp_reassembled_zip_path, prep_id, "intermediate reassembled")

        # Note: temp_final_file_path is NOT cleaned here. It's cleaned by _schedule_cleanup or generate_and_cleanup in serve_temp_file

        logging.info(f"{log_prefix} Download prep generator task ended.")

# In routes.py

@app.route('/initiate-download-all/<access_id>') # Changed route name slightly for clarity
@login_required # Or remove if public
def initiate_download_all(access_id: str):
    """
    Initiates the "Download All" process for a batch.
    It prepares data for the background zipping task and tells the client
    which SSE endpoint to connect to for progress.
    """
    prep_id_for_zip = str(uuid.uuid4()) # Unique ID for this specific zipping operation
    log_prefix = f"[DLAll-Init-{prep_id_for_zip}]"
    logging.info(f"{log_prefix} Request to initiate 'Download All' for batch_access_id: {access_id}")

    # 1. Find the batch metadata record
    batch_info, error_msg = find_metadata_by_access_id(access_id)

    if error_msg or not batch_info or not batch_info.get('is_batch'):
        error_message_for_user = error_msg if error_msg else f"Batch '{access_id}' not found or invalid."
        logging.warning(f"{log_prefix} Batch metadata lookup failed: {error_message_for_user}")
        # For an initiation route, returning JSON error is good for API clients
        status_code = 404 if "not found" in error_message_for_user.lower() else 400
        return jsonify({"error": error_message_for_user, "prep_id": None}), status_code

    # 2. Extract the list of files to include in the zip
    files_to_zip_meta = []
    total_expected_zip_content_size = 0
    for file_item in batch_info.get('files_in_batch', []):
        if not file_item.get('skipped') and not file_item.get('failed'):
            # We need the original filename (to name it inside the zip)
            # and its best Telegram file_id (to download it)
            # and its original size (for progress estimation)
            original_filename = file_item.get('original_filename')
            original_size = file_item.get('original_size', 0)
            send_locations = file_item.get('send_locations', [])
            
            tg_file_id, _ = _find_best_telegram_file_id(send_locations, PRIMARY_TELEGRAM_CHAT_ID)
            
            if original_filename and tg_file_id:
                files_to_zip_meta.append({
                    "original_filename": original_filename,
                    "telegram_file_id": tg_file_id,
                    "original_size": original_size
                })
                total_expected_zip_content_size += original_size
            else:
                logging.warning(f"{log_prefix} Skipping file '{original_filename or 'Unknown'}' due to missing name or TG file ID for zipping.")
    
    if not files_to_zip_meta:
        logging.warning(f"{log_prefix} No valid files found in batch '{access_id}' to include in zip.")
        return jsonify({"error": "No files available to include in the 'Download All' zip.", "prep_id": None}), 404

    # 3. Store this information in download_prep_data for the zipping SSE stream
    # This 'prep_id_for_zip' will be used by the /stream-download-all/<prep_id> route
    download_prep_data[prep_id_for_zip] = {
        "prep_id": prep_id_for_zip,
        "status": "initiated_zip_all", # Distinct status
        "access_id_original_batch": access_id, # Link back to the original batch
        "username": batch_info.get('username'),
        "batch_display_name": batch_info.get('batch_display_name', f"download_all_{access_id}.zip"),
        "files_to_zip_meta": files_to_zip_meta, # List of dicts: {"original_filename", "telegram_file_id", "original_size"}
        "total_expected_content_size": total_expected_zip_content_size, # For progress estimation
        "error": None,
        "final_temp_zip_path": None, # Will be set by the zipping generator
        "final_zip_size": 0,
        "start_time": time.time()
    }
    logging.info(f"{log_prefix} Stored prep data for 'Download All'. {len(files_to_zip_meta)} files to zip. Expected content size: {total_expected_zip_content_size} bytes.")

    # 4. Respond to the client with the prep_id for the SSE stream
    # The client will use this prep_id to connect to '/stream-download-all/<prep_id>'
    return jsonify({
        "message": "Download All initiated. Connect to SSE stream for progress.",
        "prep_id_for_zip": prep_id_for_zip, # Client needs this
        "sse_stream_url": url_for('stream_download_all', prep_id_for_zip=prep_id_for_zip, _external=False) # Relative URL for client
    }), 200
    
# In routes.py

# In routes.py (can be placed with other helper/generator functions)

def _generate_zip_and_stream_progress(prep_id_for_zip: str) -> Generator[SseEvent, None, None]:
    log_prefix = f"[DLAll-ZipGen-{prep_id_for_zip}]"
    logging.info(f"{log_prefix} Starting 'Download All' zipping generator.")
    
    prep_data = download_prep_data.get(prep_id_for_zip)
    if not prep_data: # Should have been checked by the route, but good for safety
        logging.error(f"{log_prefix} Critical error: Prep data missing.")
        yield _yield_sse_event('error', {'message': 'Internal server error: Preparation data lost.'})
        return

    prep_data['status'] = 'zipping_all_fetching' # New status

    files_to_process_meta = prep_data.get('files_to_zip_meta', [])
    batch_display_name_for_zip = prep_data.get('batch_display_name', f"batch_download_{prep_id_for_zip}.zip")
    total_expected_content_size = prep_data.get('total_expected_content_size', 0)
    
    temp_zip_file_path: Optional[str] = None
    download_all_executor: Optional[ThreadPoolExecutor] = None

    try:
        if not files_to_process_meta:
            raise ValueError("No files specified for zipping in 'Download All'.")

        yield _yield_sse_event('status', {'message': f'Starting download of {len(files_to_process_meta)} files for batch archive...'})
        yield _yield_sse_event('start', { # 'start' event for the zipping process
            'filename': batch_display_name_for_zip,
            'totalSize': total_expected_content_size # This is the sum of original file sizes
        })

        # Create a temporary file for the final zip archive
        # Suffix is important if client relies on it for MIME type detection on direct save
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_all_zip_{prep_id_for_zip}_") as tf_zip:
            temp_zip_file_path = tf_zip.name
        logging.info(f"{log_prefix} Created temporary zip file: {temp_zip_file_path}")

        bytes_downloaded_and_zipped = 0
        overall_zip_gen_start_time = time.time()
        
        # Map to store downloaded content: {original_filename: content_bytes}
        # Important for writing to zip in correct order if concurrency messes with completion order
        downloaded_file_contents: Dict[str, bytes] = {} 
        files_processed_count = 0
        
        if files_to_process_meta: # Ensure there are files
            download_all_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DLAllZip_{prep_id_for_zip[:4]}')
            
            # Submit all download tasks
            future_to_filename: Dict[Future, str] = {}
            for file_meta in files_to_process_meta:
                filename = file_meta["original_filename"]
                tg_file_id = file_meta["telegram_file_id"]
                # Pass dummy part_num for _download_chunk_task if it expects it (it does)
                # _download_chunk_task returns (part_num, content, error)
                fut = download_all_executor.submit(_download_chunk_task, tg_file_id, 0, prep_id_for_zip) # part_num can be 0 or filename index
                future_to_filename[fut] = filename
            
            logging.info(f"{log_prefix} Submitted {len(future_to_filename)} individual file download tasks.")

            for future in as_completed(future_to_filename):
                original_filename = future_to_filename[future]
                try:
                    _, content, error_msg = future.result() # part_num is ignored here
                    if error_msg:
                        logging.error(f"{log_prefix} Failed to download '{original_filename}': {error_msg}")
                        # Optionally, yield a specific per-file error, or just let overall fail
                        # For now, we'll let it fail overall if any sub-download fails.
                        raise ValueError(f"Failed to download '{original_filename}': {error_msg}")
                    if not content:
                        raise ValueError(f"Downloaded empty content for '{original_filename}'.")
                    
                    downloaded_file_contents[original_filename] = content
                    bytes_downloaded_and_zipped += len(content) # Or use file_meta["original_size"]
                    files_processed_count += 1
                    
                    progress = _calculate_progress(overall_zip_gen_start_time, bytes_downloaded_and_zipped, total_expected_content_size)
                    yield _yield_sse_event('progress', progress)
                    yield _yield_sse_event('status', {'message': f'Fetched {files_processed_count}/{len(files_to_process_meta)} files... ({original_filename})'})
                    logging.debug(f"{log_prefix} Fetched '{original_filename}' ({len(content)} bytes).")

                except Exception as exc:
                    logging.error(f"{log_prefix} Error processing download for '{original_filename}': {exc}", exc_info=True)
                    raise # Re-raise to be caught by the outer try-except

        # All files (hopefully) downloaded, now write them to the zip
        logging.info(f"{log_prefix} All necessary files fetched. Writing to master zip: {temp_zip_file_path}")
        yield _yield_sse_event('status', {'message': 'Creating archive...'})
        
        with zipfile.ZipFile(temp_zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file_meta in files_to_process_meta: # Iterate in original order
                filename_to_add = file_meta["original_filename"]
                if filename_to_add in downloaded_file_contents:
                    zf.writestr(filename_to_add, downloaded_file_contents[filename_to_add])
                    logging.debug(f"{log_prefix} Added '{filename_to_add}' to zip archive.")
                else:
                    # This would mean a file failed to download but didn't raise an exception stopping the process earlier
                    logging.warning(f"{log_prefix} Content for '{filename_to_add}' not found in downloaded map, skipping zip entry.")
        
        final_zip_actual_size = os.path.getsize(temp_zip_file_path)
        logging.info(f"{log_prefix} Finished writing master zip. Path: {temp_zip_file_path}, Size: {final_zip_actual_size}")

        prep_data['status'] = 'ready'
        prep_data['final_temp_zip_path'] = temp_zip_file_path # Store path to the big zip
        prep_data['final_zip_size'] = final_zip_actual_size

        # Final progress update to 100%
        yield _yield_sse_event('progress', {
            'percentage': 100, 
            'bytesProcessed': total_expected_content_size, # Show based on original content
            'totalBytes': total_expected_content_size, 
            'etaFormatted': '00:00'
        })
        yield _yield_sse_event('status', {'message': 'Archive ready for download!'})
        time.sleep(0.1) # Brief pause

        yield _yield_sse_event('ready', {
            'temp_file_id': prep_id_for_zip, # This is the ID for the zip operation
            'final_filename': batch_display_name_for_zip # The name the user will see for the .zip
        })
        logging.info(f"{log_prefix} 'Download All' zip preparation complete. Sent 'ready' event.")

    except Exception as e:
        error_message = f"Failed to generate 'Download All' zip: {str(e) or type(e).__name__}"
        logging.error(f"{log_prefix} {error_message}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_message})
        if prep_id_for_zip in download_prep_data:
            download_prep_data[prep_id_for_zip]['status'] = 'error_zipping_all'
            download_prep_data[prep_id_for_zip]['error'] = error_message
    finally:
        logging.info(f"{log_prefix} 'Download All' zipping generator cleanup.")
        if download_all_executor:
            download_all_executor.shutdown(wait=False)
            logging.info(f"{log_prefix} Download All executor shutdown.")
        
        # The temp_zip_file_path created by this generator will be cleaned up
        # by the serve_temp_file route or its scheduled cleanup,
        # so we don't delete it here IF it was successfully prepared.
        # If an error occurred *before* prep_data['final_temp_zip_path'] was set, clean it.
        if prep_data.get('status') != 'ready' and temp_zip_file_path and os.path.exists(temp_zip_file_path):
            _safe_remove_file(temp_zip_file_path, log_prefix, "partially created download-all zip")
        
        logging.info(f"{log_prefix} 'Download All' zipping generator task ended. Status: {prep_data.get('status')}")

@app.route('/stream-download-all/<prep_id_for_zip>')
@login_required # Or remove if public
def stream_download_all(prep_id_for_zip: str):
    """
    SSE endpoint that streams the progress of fetching multiple files from Telegram
    and zipping them for a "Download All" operation.
    """
    log_prefix = f"[DLAll-Stream-{prep_id_for_zip}]"
    logging.info(f"{log_prefix} SSE connection established for 'Download All'.")

    # Check if the prep_id_for_zip is valid and initiated by /initiate-download-all
    prep_entry = download_prep_data.get(prep_id_for_zip)
    if not prep_entry or prep_entry.get("status") != "initiated_zip_all":
        logging.warning(f"{log_prefix} Invalid or not initiated prep_id: {prep_id_for_zip}. Current data: {prep_entry}")
        def error_stream(): yield _yield_sse_event('error', {'message': 'Invalid or expired download all session.'})
        return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=400)

    return Response(stream_with_context(
        _generate_zip_and_stream_progress(prep_id_for_zip)
    ), mimetype='text/event-stream')

# --- File Serving, Listing, Deletion Routes ---
@app.route('/serve-temp-file/<temp_id>/<path:filename>')
def serve_temp_file(temp_id: str, filename: str) -> Response:
    """Serves the prepared temporary file and schedules its cleanup."""
    logging.info(f"Request serve temp file ID: {temp_id}, Filename: {filename}")
    prep_info = download_prep_data.get(temp_id)

    if not prep_info:
        logging.warning(f"Serve fail: Prep data not found for ID '{temp_id}'.") # More specific log
        return make_response(f"Error: Invalid or expired link (ID: {temp_id}).", 404)

    # Check status more carefully
    current_status = prep_info.get('status')
    if current_status != 'ready':
        err = prep_info.get('error', f'File not ready (Status: {current_status})')
        logging.error(f"Serve fail: '{temp_id}' status is '{current_status}'. Err:{err}")
        # Don't delete here, let scheduled cleanup handle it eventually if needed
        return make_response(f"Error: {err}", 400 if current_status == 'error' else 409) # 409 Conflict might be suitable

    temp_path = prep_info.get('final_temp_file_path')
    size = prep_info.get('final_file_size')
    dl_name = prep_info.get('original_filename', filename)

    if not temp_path or not os.path.exists(temp_path):
        logging.error(f"Serve fail: Prepared file missing ID '{temp_id}'. Expected Path:{temp_path}")
        # Mark as error, maybe schedule cleanup? Or let timeout handle it.
        prep_info['status'] = 'error'
        prep_info['error'] = 'File missing after preparation'
        # Schedule cleanup immediately in this error case
        cleanup_delay_seconds = 5 # Short delay for immediate error cleanup
        timer = threading.Timer(cleanup_delay_seconds, _schedule_cleanup, args=[temp_id, temp_path])
        timer.daemon = True # Allow program to exit even if timer is waiting
        timer.start()
        logging.info(f"[{temp_id}] Scheduled immediate cleanup due to missing file.")
        return make_response("Error: Prepared file data missing or corrupted.", 500)

    # --- Generator modification ---
    def generate_stream(path: str, pid: str):
        # Schedule cleanup BEFORE starting the stream.
        # If streaming fails, cleanup still happens eventually.
        cleanup_delay_seconds = 120 # Delay in seconds (e.g., 2 minutes)
        timer = threading.Timer(cleanup_delay_seconds, _schedule_cleanup, args=[pid, path])
        timer.daemon = True # Allow program to exit even if timer is waiting
        timer.start()
        logging.info(f"[{pid}] Scheduled cleanup in {cleanup_delay_seconds}s for path: {path}")

        logging.debug(f"[{pid}] Starting stream from: {path}")
        try:
            with open(path, 'rb') as f:
                while True:
                    chunk = f.read(STREAM_CHUNK_SIZE)
                    if not chunk:
                        logging.info(f"[{pid}] Finished streaming file.")
                        break
                    yield chunk
        except Exception as e:
            logging.error(f"[{pid}] Error during streaming {path}: {e}", exc_info=True)
            # Don't re-raise here, let the request finish potentially partially
        # NO finally block here for cleanup - it's scheduled now

    logging.info(f"[{temp_id}] Preparing streaming response for '{dl_name}'.")
    response = Response(stream_with_context(generate_stream(temp_path, temp_id)), mimetype='application/octet-stream')

    try:
        # Try UTF-8 first, then Latin-1 as fallback for filename encoding
        enc_name = filename.encode('utf-8').decode('latin-1', 'ignore')
    except Exception:
        enc_name = f"download_{temp_id}.dat"
        logging.warning(f"[{temp_id}] Could not encode filename '{filename}', using fallback: {enc_name}")

    response.headers.set('Content-Disposition', 'attachment', filename=enc_name)
    if size is not None and size >= 0:
        response.headers.set('Content-Length', str(size))

    return response

# --- API Login Route ---
@app.route('/api/auth/login', methods=['POST', 'OPTIONS'])
def api_login():
    """
    Handles API login requests.
    Authenticates user with email/password and returns a JWT upon success.
    """
    # --- Handle OPTIONS preflight request ---
    if request.method == 'OPTIONS':
        # Flask-CORS (configured globally in app_setup.py) should automatically handle
        # adding the necessary headers to this response.
        # We just need to provide an endpoint that returns an OK status (204 is good).
        logging.debug("Handling OPTIONS request for /api/auth/login")
        response = make_response()
        # Note: You generally *don't* need to manually add CORS headers here
        # if Flask-CORS is properly configured globally. It intercepts the response.
        return response, 204 # 204 No Content indicates success for OPTIONS

    # --- Handle POST request ---
    if request.method == 'POST':
        logging.info("Received POST request for /api/auth/login")
        # 1. Get Data from JSON Body
        try:
            data = request.get_json()
            if not data:
                logging.warning("API Login failed: No JSON data received.")
                return make_response(jsonify({"error": "Invalid request format. Expected JSON."}), 400)

            email = data.get('email', '').strip().lower()
            password = data.get('password', '')
            remember = data.get('remember', False) # Optional: Get remember flag

        except Exception as e:
            logging.error(f"Error parsing API login JSON data: {e}", exc_info=True)
            return make_response(jsonify({"error": "Invalid request data received."}), 400)

        # 2. Basic Validation
        if not email or not password:
            logging.warning("API Login failed: Missing email or password.")
            return make_response(jsonify({"error": "Email and password are required."}), 400)

        logging.info(f"API Login attempt for email: {email}")

        # 3. Find User in Database
        user_doc, db_error = find_user_by_email(email)

        if db_error:
            logging.error(f"Database error during API login for {email}: {db_error}")
            # Don't expose detailed DB errors to the client
            return make_response(jsonify({"error": "Internal server error during login."}), 500)

        # 4. Validate User and Password
        user_obj = None
        if user_doc:
            try:
                # Attempt to create the User object (catches potential data inconsistencies)
                user_obj = User(user_doc)
            except ValueError as e:
                # Log the specific error for debugging
                logging.error(f"Failed to create User object for {email} from doc {user_doc}: {e}", exc_info=True)
                # Return a generic error to the client
                return make_response(jsonify({"error": "Login failed due to inconsistent user data."}), 500)

        # Check if user object was created AND password matches
        if user_obj and user_obj.check_password(password):
            # --- Login Successful - Create JWT ---
            try:
                # Use the user's unique database ID (as a string) as the identity for the token
                identity = user_obj.get_id()
                if not identity:
                     # This should ideally not happen if User object creation is robust
                     logging.error(f"User object for {email} is missing an ID.")
                     return make_response(jsonify({"error": "Internal server error - user identity missing."}), 500)

                # Create the JWT access token
                access_token = create_access_token(identity=identity) # Add `expires_delta` if needed

                logging.info(f"User '{user_obj.username}' ({email}) logged in successfully via API. Token created.")

                # Return success response including the token and user details
                return make_response(jsonify({
                    "message": "Login successful!",
                    "user": {
                        "username": user_obj.username,
                        "email": user_obj.email,
                        "id": identity  # Include user ID
                        # Add any other non-sensitive user details the frontend needs
                    },
                    "token": access_token  # The crucial token field
                }), 200) # 200 OK

            except Exception as e:
                 # Handle potential errors during token creation
                 logging.error(f"Error creating JWT for user {email}: {e}", exc_info=True)
                 return make_response(jsonify({"error": "Internal server error during token generation."}), 500)

        else:
            # --- Login Failed (Invalid Email or Password) ---
            logging.warning(f"Failed API login attempt for email: {email} (Invalid credentials or user not found)")
            return make_response(jsonify({"error": "Invalid email or password."}), 401) # 401 Unauthorized is appropriate

    # Fallback for methods other than POST/OPTIONS (shouldn't be reached with current route config)
    logging.warning(f"Received unexpected method {request.method} for /api/auth/login")
    return make_response(jsonify({"error": "Method Not Allowed"}), 405)

@app.route('/files/<username>', methods=['GET'])
@jwt_required()
def list_user_files(username: str) -> Response:
    logging.info(f"List files request for: '{username}'")
    logging.info(f"Fetching files for user '{username}' from DB...")
    user_files, error_msg = find_metadata_by_username(username)

    if error_msg:
        logging.error(f"DB Error listing files for '{username}': {error_msg}")
        return jsonify({"error": "Server error retrieving file list."}), 500

    if user_files is None:
         user_files = []

    logging.info(f"Found {len(user_files)} records for '{username}'.")

    # --- NEW: Convert ObjectId to string before returning ---
    serializable_files = []
    for file_record in user_files:
        # Convert the '_id' field if it exists and is an ObjectId
        if '_id' in file_record and hasattr(file_record['_id'], 'binary'): # Check it's likely an ObjectId
             file_record['_id'] = str(file_record['_id']) # Convert ObjectId to string

        # --- Optional: Convert datetime objects too if needed ---
        # You might also have datetime objects from 'upload_timestamp'
        # If jsonify has issues with those later, add conversion here:
        # if 'upload_timestamp' in file_record and isinstance(file_record['upload_timestamp'], datetime):
        #     file_record['upload_timestamp'] = file_record['upload_timestamp'].isoformat()

        serializable_files.append(file_record)
    # --- End of NEW block ---

    # Return the modified list
    return jsonify(serializable_files) # Return the list with converted IDs

# In routes.py

@app.route('/browse/<access_id>')
@login_required # Or remove if these pages should be public via the link
def browse_batch(access_id: str):
    """
    Displays a page listing all files within a batch upload,
    identified by the batch's access_id.
    """
    log_prefix = f"[Browse-{access_id}]"
    logging.info(f"{log_prefix} Request to browse batch.")

    # 1. Find the batch metadata record using the access_id
    batch_info, error_msg = find_metadata_by_access_id(access_id)

    if error_msg or not batch_info:
        error_message_for_user = error_msg if error_msg else f"Batch '{access_id}' not found or link expired."
        logging.warning(f"{log_prefix} Metadata lookup failed: {error_message_for_user}")
        status_code = 404 if "not found" in error_message_for_user.lower() else 500
        # Reuse the 404 error template
        return make_response(render_template('404_error.html', message=error_message_for_user), status_code)

    # 2. Validate if it's actually a batch record
    if not batch_info.get('is_batch'):
        logging.error(f"{log_prefix} Access ID exists but does not point to a batch record. Info: {batch_info}")
        return make_response(render_template('404_error.html', message="Invalid link: Does not point to a batch upload."), 400)

    # 3. Extract the list of files within the batch
    files_in_batch = batch_info.get('files_in_batch', [])
    batch_display_name = batch_info.get('batch_display_name', f"Batch {access_id}")
    upload_timestamp_iso = batch_info.get('upload_timestamp')
    total_original_size = batch_info.get('total_original_size') # Sum of original sizes

    # Format timestamp (similar to get_file_by_access_id)
    date_str = "Unknown date"
    if upload_timestamp_iso:
        try:
            dt = dateutil_parser.isoparse(upload_timestamp_iso)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            else: dt = dt.astimezone(timezone.utc)
            date_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        except Exception as e:
            logging.warning(f"{log_prefix} Could not parse timestamp '{upload_timestamp_iso}': {e}")

    logging.info(f"{log_prefix} Found {len(files_in_batch)} files in batch. Rendering browser page.")

    # 4. Render the new template, passing the necessary data
    return render_template(
        'file_browser.html',
        access_id=access_id,
        batch_name=batch_display_name,
        files=files_in_batch,
        upload_date=date_str,
        total_size=total_original_size, # Pass total size if needed
        username=batch_info.get('username', 'Unknown User') # Pass uploader username
    )

# In routes.py

@app.route('/download-single/<access_id>/<path:filename>')
@login_required # Or remove if browse page/downloads are public
def download_single_file(access_id: str, filename: str):
    """
    Initiates the download preparation stream for a single file
    within a batch.
    """
    prep_id = str(uuid.uuid4()) # Unique ID for THIS specific download prep task
    log_prefix = f"[SingleDLPrep-{prep_id}]"
    logging.info(f"{log_prefix} Request to prep single file download: BatchID='{access_id}', Filename='{filename}'")

    # 1. Find the main batch metadata
    batch_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg or not batch_info or not batch_info.get('is_batch'):
        error_message_for_user = error_msg or f"Batch '{access_id}' not found or invalid."
        logging.warning(f"{log_prefix} Batch metadata lookup failed: {error_message_for_user}")
        # Maybe redirect to a specific error page or return 404/400
        # For SSE, we need to return an SSE error stream immediately
        def error_stream(): yield _yield_sse_event('error', {'message': error_message_for_user})
        return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=404 if "not found" in error_message_for_user else 400)

    # 2. Find the specific file's metadata within the batch
    files_in_batch = batch_info.get('files_in_batch', [])
    target_file_info = next((f for f in files_in_batch if f.get('original_filename') == filename and not f.get('skipped') and not f.get('failed')), None)

    if not target_file_info:
        logging.warning(f"{log_prefix} File '{filename}' not found or was skipped/failed in batch '{access_id}'.")
        def error_stream(): yield _yield_sse_event('error', {'message': f"File '{filename}' not found or unavailable in this batch."})
        return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=404)

    # 3. Extract the best Telegram file_id for this specific file
    send_locations = target_file_info.get('send_locations', [])
    telegram_file_id, chat_id = _find_best_telegram_file_id(locations=send_locations, primary_chat_id=PRIMARY_TELEGRAM_CHAT_ID)

    if not telegram_file_id:
        logging.error(f"{log_prefix} No usable Telegram file_id found for '{filename}' in batch '{access_id}'. Locations: {send_locations}")
        def error_stream(): yield _yield_sse_event('error', {'message': f"Could not find a valid source for file '{filename}'. Upload might be incomplete."})
        return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=500)

    logging.info(f"{log_prefix} Found Telegram file_id '{telegram_file_id}' for '{filename}' from chat {chat_id}.")

    # 4. Store necessary info for the preparation generator
    # We need to pass the specific telegram_file_id and the original filename/size
    # Re-use download_prep_data, but store info relevant to *this single file prep*.
    download_prep_data[prep_id] = {
        "prep_id": prep_id,
        "status": "initiated",
        "access_id": access_id, # Keep batch ID for context if needed
        "username": batch_info.get('username'),
        "requested_filename": filename, # The specific file requested
        "original_filename": filename, # The specific file requested
        "telegram_file_id": telegram_file_id, # The ID to download from TG
        "is_compressed": False, # Assume individual files weren't compressed *before* batching
                                # If they *could* be, this logic needs adjustment based on metadata
        "is_split": False, # Individual files are not split in this model
        "final_expected_size": target_file_info.get('original_size', 0), # Use size of the individual file
        "error": None,
        "final_temp_file_path": None,
        "final_file_size": 0,
        "start_time": time.time()
    }
    logging.debug(f"{log_prefix} Stored initial prep data for single file download.")

    # 5. Return the SSE stream response, calling a generator function
    # We can potentially REUSE _prepare_download_and_generate_updates if we make it flexible
    # enough to handle both batch lookups and direct telegram_file_id inputs.
    # Let's try reusing it. It needs modification to check if 'telegram_file_id' is already present.

    return Response(stream_with_context(
        _prepare_download_and_generate_updates(prep_id) # Call the *existing* generator
    ), mimetype='text/event-stream')

@app.route('/get/<access_id>')
def get_file_by_access_id(access_id: str) -> Union[str, Response]:
    logging.info(f"Request dl page via access_id: {access_id}")
    logging.info(f"Looking up access_id '{access_id}' in DB...")
    file_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg or not file_info:
        error_message_for_user = error_msg if error_msg else f"Link '{access_id}' not found or expired."
        logging.warning(f"Failed lookup for access_id '{access_id}': {error_message_for_user}")
    # Distinguish "not found" from server errors for status code
        status_code = 404 if "not found" in error_message_for_user.lower() else 500
        return make_response(render_template('404_error.html', message=error_message_for_user), status_code)
    
    username = file_info.get('username')
    if not username:    
     # This shouldn't happen if records are saved correctly, but handle defensively
        logging.error(f"Record found for access_id '{access_id}' but missing username field.")
        message = "File record found but is incomplete (missing user info)."
        return make_response(render_template('404_error.html', message=message), 500)

    orig_name = file_info.get('original_filename', 'Unknown'); size = file_info.get('original_size')
    ts_iso = file_info.get('upload_timestamp'); date_str = "Unknown date"
    if ts_iso:
        try:
            dt = dateutil_parser.isoparse(ts_iso);
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            else: dt = dt.astimezone(timezone.utc)
            date_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        except Exception as e: logging.warning(f"Could not parse ts '{ts_iso}': {e}")
    logging.info(f"Rendering dl page for '{orig_name}' (id: {access_id}).")
    return render_template('download_page.html', filename=orig_name, filesize=size if size is not None else 0, upload_date=date_str, username=username, access_id=access_id)

@app.route('/delete-file/<username>/<path:filename>', methods=['DELETE'])
def delete_file_record(username: str, filename: str) -> Response:
    logging.info(f"DELETE request user='{username}', file='{filename}'")
    logging.info(f"Attempting delete from DB: User='{username}', File='{filename}'")
    # Using original_filename for deletion as per previous logic.
    # Consider changing frontend/backend to use access_id for guaranteed uniqueness if needed.
    deleted_count, error_msg = delete_metadata_by_filename(username, filename) # Call DB function

    if error_msg:
        logging.error(f"DB Error deleting file record for '{username}/{filename}': {error_msg}")
        # Provide a generic server error message to the user
        return jsonify({"error": "Server error during deletion. Please try again later."}), 500

    if deleted_count == 0:
        logging.warning(f"No file record found to delete for '{username}/{filename}'.")
        return jsonify({"error": f"File '{filename}' not found for user '{username}'."}), 404
    else:
        # It's possible multiple records were deleted if names weren't unique
        logging.info(f"Successfully deleted {deleted_count} record(s) for '{username}/{filename}' from DB.")
        return jsonify({"message": f"Record for '{filename}' deleted successfully."}), 200
logging.info("Flask routes defined using configurable workers and linter fixes.")



@app.route('/login', methods=['GET', 'POST'])
def login():
    """Handles user login."""
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')

        if not email or not password:
            flash('Please enter both email and password.', 'warning')
            return render_template('login.html')

        logging.info(f"Login attempt for email: {email}")

        # Find user by email
        user_doc, db_error = database.find_user_by_email(email)

        if db_error:
            logging.error(f"Database error during login for {email}: {db_error}")
            flash('An internal error occurred. Please try again later.', 'danger')
            return render_template('login.html')

        user_obj = None
        if user_doc:
             try:
                # Create a User object if document found
                user_obj = User(user_doc)
             except ValueError as e:
                 # Handle cases where user doc might be missing required fields
                 logging.error(f"Failed to create User object for {email}: {e}", exc_info=True)
                 flash('Login failed due to inconsistent user data. Please contact support.', 'danger')
                 return render_template('login.html')

        # Check if user exists AND password is correct
        # Use the check_password method from the User object
        if user_obj and user_obj.check_password(password):
            # --- Login successful ---
            # Use Flask-Login's login_user function
            # Pass remember=True if you add a "Remember Me" checkbox
            login_user(user_obj, remember=False)
            logging.info(f"User '{user_obj.username}' ({email}) logged in successfully.")
            flash(f'Welcome back, {user_obj.username}!', 'success')

            # Redirect to the page the user was trying to access, or index
            next_page = request.args.get('next')
            # Basic security check for open redirect vulnerabilities
            if next_page and not next_page.startswith('/'):
                next_page = None # Ignore external redirects
            return redirect(next_page or url_for('index'))
        else:
            # --- Login failed ---
            logging.warning(f"Failed login attempt for email: {email}")
            flash('Invalid email or password. Please try again.', 'danger')
            return render_template('login.html')

    # GET request: just show the login page
    return render_template('login.html')

@app.route('/logout')
@login_required # Make sure only logged-in users can log out
def logout():
    """Logs the current user out."""
    user_email = current_user.email # Get email before logout for logging
    logout_user() # Flask-Login function to clear the session
    logging.info(f"User {user_email} logged out.")
    flash('You have been successfully logged out.', 'success')
    return redirect(url_for('login'))
