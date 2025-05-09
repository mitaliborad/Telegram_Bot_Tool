"""Flask routes and core logic for the Telegram File Storage tool."""
import io
import os
import uuid
import time
import json
import zipfile
import tempfile
import shutil
from flask import send_from_directory
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
# @app.route('/')
# def index() -> str:
#     logging.info("Serving index page.")
#     try:
#         # Pass current_user to the template so it knows if someone is logged in
#         return render_template('index.html', current_user=current_user)
#     except Exception as e:
#         logging.error(f"Error rendering index.html: {e}", exc_info=True)
#         return make_response("Error loading page.", 500)
    
# # In routes.py

@app.route('/initiate-upload', methods=['POST'])
@jwt_required(optional=True)
def initiate_upload() -> Response:
    upload_id = str(uuid.uuid4())
    log_prefix = f"[{upload_id}]"
    logging.info(f"{log_prefix} Request to initiate upload.")
    logging.info(f"{log_prefix} request.files: {request.files}")
    logging.info(f"{log_prefix} request.form: {request.form}")

    
    current_user_jwt_identity = get_jwt_identity()
    
    display_username: Optional[str] = None
    user_email: Optional[str] = None
    is_anonymous: bool = False
    anonymous_id: Optional[str] = None
    
    if current_user_jwt_identity:
        # --- User is Logged In (JWT provided) ---
        logging.info(f"{log_prefix} Authenticated upload attempt. JWT Identity: {current_user_jwt_identity}")
        is_anonymous = False
        try:
            user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))
            if error or not user_doc:
                logging.error(f"{log_prefix} Could not find user for JWT identity '{current_user_jwt_identity}'. Error: {error}")
                # Even though optional, if a token is provided but invalid, it's an error.
                return jsonify({"error": "Invalid user token or user not found"}), 401 # Use 401 for bad tokens

            user_object_from_jwt = User(user_doc)
            display_username = user_object_from_jwt.username
            user_email = user_object_from_jwt.email
            logging.info(f"{log_prefix} User identified via JWT: Username='{display_username}'")

        except ValueError as ve: # Error creating User object
            logging.error(f"{log_prefix} Failed to instantiate User object for JWT identity '{current_user_jwt_identity}':{ve}")
            return jsonify({"error": "User data inconsistency"}), 500
        except Exception as e: # Catch ObjectId errors or other unexpected issues
            logging.error(f"{log_prefix} Error processing JWT identity '{current_user_jwt_identity}': {e}", exc_info=True)
            return jsonify({"error": "Server error processing authentication"}), 500
    else:
        # --- User is Anonymous (No valid JWT) ---
        logging.info(f"{log_prefix} Anonymous upload attempt.")
        is_anonymous = True
        # Frontend MUST send 'anonymous_upload_id' in the form data for anonymous uploads
        anonymous_id = request.form.get('anonymous_upload_id')
        if not anonymous_id:
            logging.warning(f"{log_prefix} Anonymous upload failed: Missing 'anonymous_upload_id' in form data.")
            return jsonify({"error": "Missing required anonymous identifier for anonymous upload."}), 400
    
    
        display_username = f"AnonymousUser-{anonymous_id[:6]}" # Example display name
        user_email = None # No email for anonymous
        logging.info(f"{log_prefix} Anonymous upload identified by temp ID: {anonymous_id}")
    
    if display_username is None:
        logging.error(f"{log_prefix} Internal state error: display_username is None after auth check.")
        return jsonify({"error": "Internal server error processing user identity."}), 500
    
    # Get the list of files. This handles one or more files.
    uploaded_files = request.files.getlist('files[]')
    if not uploaded_files or all(not f.filename for f in uploaded_files):
        logging.warning(f"{log_prefix} Initiate upload failed: No files provided or files have no names.")
        return jsonify({"error": "No files selected or files are invalid"}), 400

    # current_user_jwt_identity = get_jwt_identity()
    # logging.info(f"{log_prefix} JWT Identity: {current_user_jwt_identity}")
    
    # user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))
    # if error or not user_doc:
    #     logging.error(f"{log_prefix} Could not find user for JWT identity '{current_user_jwt_identity}'. Error: {error}")
    #     return jsonify({"error": "Invalid user token or user not found"}), 401
    

    # user_object_from_jwt = User(user_doc)
    # display_username = user_object_from_jwt.username
    # user_email = user_object_from_jwt.email # If needed


    batch_temp_dir = os.path.join(UPLOADS_TEMP_DIR, f"batch_{upload_id}")
    original_filenames_in_batch = []

    try:
        os.makedirs(batch_temp_dir, exist_ok=True)
        logging.info(f"{log_prefix} Created batch temporary directory: {batch_temp_dir}")

        for file_storage_item in uploaded_files:
            if file_storage_item and file_storage_item.filename:
                original_filename = file_storage_item.filename 
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
        if len(original_filenames_in_batch) == 1:
           batch_display_name = original_filenames_in_batch[0]
        elif original_filenames_in_batch:
            batch_display_name = f"{original_filenames_in_batch[0]} (+{len(original_filenames_in_batch)-1} others)"
            
        progress_entry = {
            "status": "initiated",
            "is_batch": True,
            "batch_directory_path": batch_temp_dir,
            "original_filenames_in_batch": original_filenames_in_batch,
            "batch_display_name": batch_display_name, # Name for UI during upload
            "username": display_username, # Real username or Anonymous identifier
            "user_email": user_email,     # Real email or None
            "is_anonymous": is_anonymous, # Boolean flag
            "error": None,
            "start_time": time.time()
        }
        if is_anonymous and anonymous_id:
            progress_entry["anonymous_id"] = anonymous_id
            
        upload_progress_data[upload_id] = progress_entry
        logging.debug(f"{log_prefix} Initial progress data stored: {upload_progress_data[upload_id]}")
        return jsonify({"upload_id": upload_id, "filename": batch_display_name})

    except Exception as e:
        logging.error(f"{log_prefix} Error processing batch upload: {e}", exc_info=True)
        if os.path.exists(batch_temp_dir):
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

# routes.py

# Change signature: accept file_path instead of file_bytes
def _send_single_file_task(file_path: str, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
    """Opens a file and streams it to Telegram via send_file_to_telegram."""
    log_prefix = f"[{upload_id}] Task for '{filename}' to {chat_id}"
    try:
        # Open the file in binary read mode
        with open(file_path, 'rb') as f_handle:
            file_size = os.path.getsize(file_path) # Get size for logging
            logging.info(f"{log_prefix} Sending file ({format_bytes(file_size)}) from path: {file_path}")
            # Call the modified send_file_to_telegram with the file handle
            result = send_file_to_telegram(f_handle, filename, chat_id)
            # f_handle is automatically closed by the 'with' statement

        logging.info(f"{log_prefix} Send result: Success={result[0]}")
        return str(chat_id), result
    except FileNotFoundError:
         logging.error(f"{log_prefix} File not found at path: {file_path}")
         return str(chat_id), (False, f"File not found: {filename}", None)
    except Exception as e:
        logging.error(f"{log_prefix} Unexpected error opening/sending file: {e}", exc_info=True)
        return str(chat_id), (False, f"Thread error processing file: {e}", None)
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

        is_batch_upload = upload_data.get("is_batch", False)
        username = upload_data['username']
        batch_directory_path = upload_data.get("batch_directory_path")
        original_filenames_in_batch = upload_data.get("original_filenames_in_batch", [])
        
        batch_display_name = f"Upload ({len(original_filenames_in_batch)} files)"
        if original_filenames_in_batch:
            batch_display_name = f"{original_filenames_in_batch[0]} (+{len(original_filenames_in_batch)-1} others)" if len(original_filenames_in_batch) > 1 else original_filenames_in_batch[0]

        if not is_batch_upload or not batch_directory_path or not os.path.isdir(batch_directory_path) or not original_filenames_in_batch:
            logging.error(f"{log_prefix} Invalid batch data. is_batch={is_batch_upload}, dir={batch_directory_path}, files={original_filenames_in_batch}")
            yield _yield_sse_event('error', {'message': 'Internal error: Invalid batch data.'})
            if batch_directory_path and os.path.isdir(batch_directory_path):
                _safe_remove_directory(batch_directory_path, log_prefix, "invalid batch dir")
            return

        logging.info(f"{log_prefix} Processing batch: User='{username}', Dir='{batch_directory_path}', Files={original_filenames_in_batch}")
        upload_data['status'] = 'processing_telegram'

        access_id: Optional[str] = upload_data.get('access_id') 
        if not access_id: # This will be true for new uploads
            access_id = uuid.uuid4().hex[:10] 
            upload_data['access_id'] = access_id # Store it back in upload_data
            logging.info(f"{log_prefix} Generated new access_id for batch: {access_id}") # Changed log level for visibility
        else:
            logging.info(f"{log_prefix} Using existing access_id for batch: {access_id}")


        executor: Optional[ThreadPoolExecutor] = None
        if len(TELEGRAM_CHAT_IDS) > 1:
            executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'Upload_{upload_id[:4]}')
            logging.info(f"{log_prefix} Initialized Upload Executor (max={MAX_UPLOAD_WORKERS})")

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

        yield _yield_sse_event('start', {'filename': batch_display_name, 'totalSize': total_original_bytes_in_batch})
        yield _yield_sse_event('status', {'message': f'Uploading {len(files_to_process_details)} files...'})

        overall_start_time = time.time()
        bytes_sent_so_far = 0
        all_files_metadata_for_db = [] 
        all_succeeded = True 

        for file_detail in files_to_process_details:
            # ... (inner loop for processing each file - assumed to be largely correct for this issue) ...
            # ... This loop populates all_files_metadata_for_db ...
            current_file_path = file_detail["path"]
            current_filename = file_detail["name"]
            current_file_size = file_detail["size"]
            log_file_prefix = f"{log_prefix} File '{current_filename}'"

            logging.info(f"{log_file_prefix} Starting send process.")

            if current_file_size == 0:
                logging.warning(f"{log_file_prefix} is empty, skipping send but including in metadata.")
                file_meta_entry = {
                "original_filename": current_filename, "original_size": 0, "skipped": True, "failed": False, 
                "reason": "File is empty", "send_locations": []
                }
                all_files_metadata_for_db.append(file_meta_entry)
                bytes_sent_so_far += 0 
                progress_data = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
                yield _yield_sse_event('progress', progress_data)
                continue 

            file_specific_futures: Dict[Future, str] = {}
            file_specific_results: Dict[str, ApiResult] = {}
            primary_send_success_for_this_file = False
            primary_send_message = "Primary send not attempted or failed."

            try:
                if executor:
                    for chat_id_str in TELEGRAM_CHAT_IDS:
                        cid = str(chat_id_str)
                        if current_file_size > (2 * 1024 * 1024 * 1024): # 2GB limit
                            logging.error(f"{log_file_prefix} is larger than 2GB, chunking not implemented. Skipping file.")
                            file_specific_results[cid] = (False, "File exceeds 2GB size limit", None)
                            all_succeeded = False
                            # Create a failure metadata entry
                            file_meta_entry = {"original_filename": current_filename, "original_size": current_file_size, "skipped": False, "failed": True, "reason": "File exceeds 2GB size limit.", "send_locations": []}
                            all_files_metadata_for_db.append(file_meta_entry)
                            primary_send_success_for_this_file = False # Mark as failed for this file
                            primary_send_message = "File exceeds 2GB size limit."
                            break # Break from chat_id loop for this file
                        fut = executor.submit(_send_single_file_task, current_file_path, current_filename, cid, upload_id)
                        file_specific_futures[fut] = cid
                    if not primary_send_success_for_this_file and "File exceeds" in primary_send_message: # If already marked as failed due to size
                         pass # Skip to result processing for this file
                else: # No executor (single chat ID)
                    cid = str(TELEGRAM_CHAT_IDS[0])
                    if current_file_size > (2 * 1024 * 1024 * 1024): # 2GB limit
                        logging.error(f"{log_file_prefix} is larger than 2GB, chunking not implemented. Skipping file.")
                        res_tuple_no_exec = (False, "File exceeds 2GB size limit", None)
                        all_succeeded = False
                        file_meta_entry = {"original_filename": current_filename, "original_size": current_file_size, "skipped": False, "failed": True, "reason": "File exceeds 2GB size limit.", "send_locations": []}
                        all_files_metadata_for_db.append(file_meta_entry)
                    else:
                        _, res_tuple_no_exec = _send_single_file_task(current_file_path, current_filename, cid, upload_id)
                    file_specific_results[cid] = res_tuple_no_exec
                    primary_send_success_for_this_file = res_tuple_no_exec[0]
                    primary_send_message = res_tuple_no_exec[1]
                    if not primary_send_success_for_this_file:
                        all_succeeded = False
                
                if file_specific_futures: # If executor was used and futures were submitted
                    primary_fut: Optional[Future] = None
                    primary_cid_str = str(PRIMARY_TELEGRAM_CHAT_ID)
                    for fut_key, chat_id_val in file_specific_futures.items():
                        if chat_id_val == primary_cid_str: primary_fut = fut_key; break
                    
                    if primary_fut:
                        logging.debug(f"{log_file_prefix} Waiting for primary send...")
                        cid_res, res = primary_fut.result()
                        file_specific_results[cid_res] = res
                        primary_send_success_for_this_file = res[0]
                        primary_send_message = res[1]
                        if not primary_send_success_for_this_file:
                            all_succeeded = False
                            logging.error(f"{log_file_prefix} Primary send failed: {primary_send_message}")
                    else: # Should not happen if primary chat ID is in TELEGRAM_CHAT_IDS
                        logging.warning(f"{log_file_prefix} Primary future not found (CID: {primary_cid_str}). Assuming failure for safety.")
                        primary_send_success_for_this_file = False
                        primary_send_message = "Primary Telegram chat not configured or send task failed to initialize."
                        all_succeeded = False

                    logging.debug(f"{log_file_prefix} Waiting for backup sends...")
                    for fut_completed in as_completed(file_specific_futures):
                        cid_res, res = fut_completed.result()
                        if cid_res not in file_specific_results: file_specific_results[cid_res] = res
                
                # --- Process results for *this specific file* ---
                current_file_send_report = [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k, r in file_specific_results.items()]
                parsed_locations_for_this_file = _parse_send_results(f"{log_prefix}-{current_filename}", current_file_send_report)

                if primary_send_success_for_this_file:
                    bytes_sent_so_far += current_file_size 
                    primary_parsed_loc = next((loc for loc in parsed_locations_for_this_file if loc.get('chat_id') == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                    if not primary_parsed_loc or not primary_parsed_loc.get('success'):
                        logging.error(f"{log_file_prefix} Primary send reported OK by API but parsing failed or IDs missing. Marking as failed.")
                        all_succeeded = False 
                        file_meta_entry = {"original_filename": current_filename, "original_size": current_file_size, "skipped": False, "failed": True, "reason": "Primary send metadata parsing failed.", "send_locations": parsed_locations_for_this_file}
                        all_files_metadata_for_db.append(file_meta_entry)
                    else:
                        file_meta_entry = {"original_filename": current_filename, "original_size": current_file_size, "skipped": False, "failed": False, "reason": None, "send_locations": parsed_locations_for_this_file}
                        all_files_metadata_for_db.append(file_meta_entry)
                        logging.info(f"{log_file_prefix} Successfully processed and recorded.")
                elif not any(fm for fm in all_files_metadata_for_db if fm['original_filename'] == current_filename and fm['failed']): # Only add if not already added as failed (e.g. size limit)
                    all_succeeded = False
                    logging.error(f"{log_file_prefix} Failed primary send. Reason: {primary_send_message}")
                    file_meta_entry = {"original_filename": current_filename, "original_size": current_file_size, "skipped": False, "failed": True, "reason": f"Primary send failed: {primary_send_message}", "send_locations": parsed_locations_for_this_file}
                    all_files_metadata_for_db.append(file_meta_entry)

            except Exception as file_loop_error: # Catch errors within the file processing loop
                logging.error(f"{log_file_prefix} Unexpected error during send: {file_loop_error}", exc_info=True)
                all_succeeded = False
                if not any(fm for fm in all_files_metadata_for_db if fm['original_filename'] == current_filename and fm['failed']):
                    file_meta_entry = {"original_filename": current_filename, "original_size": file_detail.get("size", 0), "skipped": False, "failed": True, "reason": f"Unexpected error: {str(file_loop_error)}", "send_locations": [] }
                    all_files_metadata_for_db.append(file_meta_entry)

            progress_data = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
            yield _yield_sse_event('progress', progress_data)
            yield _yield_sse_event('status', {'message': f'Processed {len(all_files_metadata_for_db)} of {len(files_to_process_details)} files...'})
        # --- End of for file_detail loop ---

        total_batch_duration = time.time() - overall_start_time
        logging.info(f"{log_prefix} Finished processing all files in batch. Duration: {total_batch_duration:.2f}s. Overall Primary Success: {all_succeeded}")

        if not all_files_metadata_for_db: # Should be populated even if files failed/skipped
            logging.error(f"{log_prefix} Critical: No metadata was generated for any file after processing loop.")
            raise RuntimeError("Processing finished but no metadata was generated for any file.")

        db_batch_timestamp = datetime.now(timezone.utc).isoformat()
        db_batch_record = {
            "access_id": access_id, # This is the batch access_id
            "username": upload_data['username'],
            "is_anonymous": upload_data.get('is_anonymous', False),
            "anonymous_id": upload_data.get('anonymous_id'),
            "upload_timestamp": db_batch_timestamp,
            "is_batch": True,
            "batch_display_name": batch_display_name,
            "files_in_batch": all_files_metadata_for_db,
            "total_original_size": total_original_bytes_in_batch,
            "total_upload_duration_seconds": round(total_batch_duration, 2),
        }
        if db_batch_record["anonymous_id"] is None:
            del db_batch_record["anonymous_id"]
        
        logging.debug(f"Attempting to save batch metadata: {json.dumps(db_batch_record, indent=2, default=str)}")

        save_success, save_msg = save_file_metadata(db_batch_record)
        if not save_success:
            logging.error(f"{log_prefix} DB CRITICAL: Failed to save batch metadata: {save_msg}")
            raise IOError(f"Failed to save batch metadata: {save_msg}")
        else:
            logging.info(f"{log_prefix} DB: Successfully saved batch metadata.")
        
        base_url = request.host_url.rstrip('/')
        browser_url_path = f"/browse/{access_id}"
        browser_url = f"{base_url}{browser_url_path}"
        
        # --- ADDED LOGGING and check for access_id ---
        if not access_id or not isinstance(access_id, str) or len(access_id.strip()) == 0:
            logging.error(f"{log_prefix} CRITICAL: access_id is invalid ('{access_id}') before yielding 'complete' event. This should not happen.")
            # Fallback or error yield if access_id is bad
            yield _yield_sse_event('error', {'message': 'Internal server error: Failed to generate a valid batch identifier.'})
            upload_data['status'] = 'error'
            upload_data['error'] = 'Invalid batch identifier generated'
            return # Prevent yielding 'complete' with bad access_id

        complete_payload = {
            'message': f'Batch upload ({len(files_to_process_details)} files) complete!',
            'download_url': browser_url,
            'filename': batch_display_name,
            'batch_access_id': access_id # This should be the valid batch access_id
        }
        logging.info(f"{log_prefix} Preparing to yield 'complete' event. Access ID: '{access_id}', Payload: {json.dumps(complete_payload)}")
        yield _yield_sse_event('complete', complete_payload)
        # --- END OF ADDED LOGGING ---
        
        upload_data['status'] = 'completed' if all_succeeded else 'completed_with_errors'

    except Exception as e:
        logging.error(f"{log_prefix} An exception occurred during batch upload processing:", exc_info=True)
        error_msg_final = f"Batch upload processing failed: {str(e) or type(e).__name__}"
        logging.error(f"{log_prefix} Sending SSE error to client: {error_msg_final}")
        yield _yield_sse_event('error', {'message': error_msg_final})
        if upload_id in upload_progress_data:
            upload_data['status'] = 'error'
            upload_data['error'] = error_msg_final
    finally:
        logging.info(f"{log_prefix} Batch upload generator final cleanup.")
        if executor:
            executor.shutdown(wait=False)
            logging.info(f"{log_prefix} Upload executor shutdown.")
        
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
                if file_info.get('is_batch', False):
                    logging.error(
                        f"{log_prefix} Attempt to download a batch record (access_id: {access_id}) "
                        f"as if it were a single file. This typically means the download "
                        f"button on the batch browse page is misconfigured to call "
                        f"/stream-download/ instead of /download-single/ for an item "
                        f"or /initiate-download-all/ for the entire batch."
                    )
                    raise ValueError(
                        "This link refers to a batch of files. "
                        "To download all files, use the 'Download All' option. "
                        "To download a specific file, click its individual download button from the list."
                    )
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
# routes.py
# ...

@app.route('/initiate-download-all/<access_id>') # Changed route name slightly for clarity
def initiate_download_all(access_id: str):
    # ... (existing code to lookup batch_info and files_to_zip_meta) ...
    prep_id_for_zip = str(uuid.uuid4()) 
    log_prefix = f"[DLAll-Init-{prep_id_for_zip}]"
    logging.info(f"{log_prefix} Request to initiate 'Download All' for batch_access_id: {access_id}")

    batch_info, error_msg = find_metadata_by_access_id(access_id)

    if error_msg or not batch_info or not batch_info.get('is_batch'):
        error_message_for_user = error_msg if error_msg else f"Batch '{access_id}' not found or invalid."
        logging.warning(f"{log_prefix} Batch metadata lookup failed: {error_message_for_user}")
        status_code = 404 if "not found" in error_message_for_user.lower() else 400
        return jsonify({"error": error_message_for_user, "prep_id": None}), status_code
    
    files_to_zip_meta = []
    total_expected_zip_content_size = 0
    for file_item in batch_info.get('files_in_batch', []):
        if not file_item.get('skipped') and not file_item.get('failed'):
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

    batch_display_name_for_zip = batch_info.get('batch_display_name', f"download_all_{access_id}.zip")
    # Ensure the .zip extension if it's a generic name
    if not batch_display_name_for_zip.lower().endswith(".zip"):
        batch_display_name_for_zip += ".zip"


    download_prep_data[prep_id_for_zip] = {
        "prep_id": prep_id_for_zip,
        "status": "initiated_zip_all", 
        "access_id_original_batch": access_id, 
        "username": batch_info.get('username'),
        "batch_display_name": batch_display_name_for_zip, # Used by _generate_zip_and_stream_progress
        "original_filename": batch_display_name_for_zip, # ADDED/MODIFIED: For serve_temp_file consistency
        "files_to_zip_meta": files_to_zip_meta, 
        "total_expected_content_size": total_expected_zip_content_size, 
        "error": None,
        "final_temp_file_path": None, # Will be set by the zipping generator using this standardized key
        "final_file_size": 0,         # Will be set by the zipping generator using this standardized key
        "start_time": time.time()
    }
    logging.info(f"{log_prefix} Stored prep data for 'Download All'. {len(files_to_zip_meta)} files to zip. Expected content size: {total_expected_zip_content_size} bytes. Zip name: {batch_display_name_for_zip}")

    return jsonify({
        "message": "Download All initiated. Connect to SSE stream for progress.",
        "prep_id_for_zip": prep_id_for_zip, 
        "sse_stream_url": url_for('stream_download_all', prep_id_for_zip=prep_id_for_zip, _external=False) 
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
    prep_data['original_filename'] = batch_display_name_for_zip
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
        
        downloaded_file_contents: Dict[str, bytes] = {} 
        files_processed_count = 0
        
        if files_to_process_meta: # Ensure there are files
            download_all_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DLAllZip_{prep_id_for_zip[:4]}')
            
            # Submit all download tasks
            future_to_filename: Dict[Future, str] = {}
            for file_meta in files_to_process_meta:
                filename = file_meta["original_filename"]
                tg_file_id = file_meta["telegram_file_id"]
                fut = download_all_executor.submit(_download_chunk_task, tg_file_id, 0, prep_id_for_zip) 
                future_to_filename[fut] = filename
            
            logging.info(f"{log_prefix} Submitted {len(future_to_filename)} individual file download tasks.")
            for future in as_completed(future_to_filename):
                original_filename = future_to_filename[future]
                try:
                    _, content, error_msg = future.result() 
                    if error_msg:
                        logging.error(f"{log_prefix} Failed to download '{original_filename}': {error_msg}")
                        raise ValueError(f"Failed to download '{original_filename}': {error_msg}")
                    if not content:
                        raise ValueError(f"Downloaded empty content for '{original_filename}'.")
                    
                    downloaded_file_contents[original_filename] = content
                    bytes_downloaded_and_zipped += len(content) 
                    files_processed_count += 1
                    
                    progress = _calculate_progress(overall_zip_gen_start_time, bytes_downloaded_and_zipped, total_expected_content_size)
                    yield _yield_sse_event('progress', progress)
                    yield _yield_sse_event('status', {'message': f'Fetched {files_processed_count}/{len(files_to_process_meta)} files... ({original_filename})'})
                    logging.debug(f"{log_prefix} Fetched '{original_filename}' ({len(content)} bytes).")

                except Exception as exc:
                    logging.error(f"{log_prefix} Error processing download for '{original_filename}': {exc}", exc_info=True)
                    raise 

        # All files (hopefully) downloaded, now write them to the zip
        logging.info(f"{log_prefix} All necessary files fetched. Writing to master zip: {temp_zip_file_path}")
        yield _yield_sse_event('status', {'message': 'Creating archive...'})
        
        with zipfile.ZipFile(temp_zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file_meta in files_to_process_meta: 
                filename_to_add = file_meta["original_filename"]
                if filename_to_add in downloaded_file_contents:
                    zf.writestr(filename_to_add, downloaded_file_contents[filename_to_add])
                    logging.debug(f"{log_prefix} Added '{filename_to_add}' to zip archive.")
                else:
                    logging.warning(f"{log_prefix} Content for '{filename_to_add}' not found in downloaded map, skipping zip entry.")
        
        final_zip_actual_size = os.path.getsize(temp_zip_file_path)
        logging.info(f"{log_prefix} Finished writing master zip. Path: {temp_zip_file_path}, Size: {final_zip_actual_size}")

        prep_data['status'] = 'ready'
        # MODIFIED LINES: Standardize keys for serve_temp_file
        prep_data['final_temp_file_path'] = temp_zip_file_path 
        prep_data['final_file_size'] = final_zip_actual_size

        # Final progress update to 100%
        yield _yield_sse_event('progress', {
            'percentage': 100, 
            'bytesProcessed': total_expected_content_size, 
            'totalBytes': total_expected_content_size, 
            'etaFormatted': '00:00'
        })
        yield _yield_sse_event('status', {'message': 'Archive ready for download!'})
        time.sleep(0.1)
        
        yield _yield_sse_event('ready', {
            'temp_file_id': prep_id_for_zip, 
            'final_filename': batch_display_name_for_zip 
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
        
        if prep_data.get('status') != 'ready' and temp_zip_file_path and os.path.exists(temp_zip_file_path):
            _safe_remove_file(temp_zip_file_path, log_prefix, "partially created download-all zip")
        
        logging.info(f"{log_prefix} 'Download All' zipping generator task ended. Status: {prep_data.get('status')}")
        
        # The temp_zip_file_path created by this generator will be cleaned up
        # by the serve_temp_file route or its scheduled cleanup,
        # so we don't delete it here IF it was successfully prepared.
        # If an error occurred *before* prep_data['final_temp_zip_path'] was set, clean it.
        

@app.route('/stream-download-all/<prep_id_for_zip>')
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



# @app.route('/browse/<access_id>')
# # def serve_angular():
# #     return send_file('path_to_angular_build/index.html')
# def browse_batch_page(access_id: str):
#     """
#     Displays a page listing all files within a batch upload,
#     identified by the batch's access_id.
#     """
#     log_prefix = f"[Browse-{access_id}]"
#     logging.info(f"{log_prefix} Request to browse batch.")

#     # 1. Find the batch metadata record using the access_id
#     batch_info, error_msg = find_metadata_by_access_id(access_id)

#     if error_msg or not batch_info:
#         error_message_for_user = error_msg if error_msg else f"Batch '{access_id}' not found or link expired."
#         logging.warning(f"{log_prefix} Metadata lookup failed: {error_message_for_user}")
#         status_code = 404 if "not found" in error_message_for_user.lower() else 500
#         return make_response(render_template('404_error.html', message=error_message_for_user), status_code)

#     # 2. Validate if it's actually a batch record
#     if not batch_info.get('is_batch'):
#         logging.error(f"{log_prefix} Access ID exists but does not point to a batch record. Info: {batch_info}")
#         return make_response(render_template('404_error.html', message="Invalid link: Does not point to a batch upload."), 400)

#     # 3. Extract the list of files within the batch and process for template
#     files_in_batch_original = batch_info.get('files_in_batch', [])
#     processed_files_for_template = []
#     for f_meta in files_in_batch_original: # Process the list *after* getting it
#         new_meta = f_meta.copy()
#         if 'original_filename' not in new_meta: # Ensure filename for display
#             new_meta['original_filename'] = "Unknown File"
#         if 'original_size' in new_meta:
#             new_meta['filesize'] = new_meta['original_size'] # Add the 'filesize' key for items in the list
#         else:
#             new_meta['filesize'] = 0 # Default if original_size is missing
#         processed_files_for_template.append(new_meta)

#     # Extract other info
#     batch_display_name = batch_info.get('batch_display_name', f"Batch {access_id}")
#     upload_timestamp_iso = batch_info.get('upload_timestamp')
#     total_original_size = batch_info.get('total_original_size') # Sum of original sizes

#     # Format timestamp (similar to get_file_by_access_id)
#     date_str = "Unknown date"
#     if upload_timestamp_iso:
#         try:
#             dt = dateutil_parser.isoparse(upload_timestamp_iso)
#             if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
#             else: dt = dt.astimezone(timezone.utc)
#             date_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
#         except Exception as e:
#             logging.warning(f"{log_prefix} Could not parse timestamp '{upload_timestamp_iso}': {e}")

#     logging.info(f"{log_prefix} Found {len(processed_files_for_template)} files in batch. Rendering browser page.")

#     # 4. Render the new template, passing the PROCESSED data
#     return render_template(
#         'batch-file-browser.component.html',
#         access_id=access_id,
#         batch_name=batch_display_name,
#         files=processed_files_for_template, 
#         upload_date=date_str,
#         total_size=total_original_size,
#         username=batch_info.get('username', 'Unknown User'),
#         filename=None,  # <--- ADD THIS LINE
#         filesize=None   # <--- ADD THIS LINE
#     )
    
# In routes.py, modify the browse_batch function:

# In routes.py

# @app.route('/browse/<access_id>')
# def browse_batch_page(access_id: str): # Renamed for clarity
#     """
#     Serves the main Angular application page.
#     Angular's router will then take over and display the
#     BatchFileBrowserComponent for the '/browse/:accessId' route.
#     """
#     log_prefix = f"[BrowseShell-{access_id}]"
#     logging.info(f"{log_prefix} Serving Angular application shell for batch browsing.")
#     try:
#         # This 'index.html' should be the main entry point of your compiled Angular application,
#         # containing <app-root></app-root> and script tags for Angular bundles.
#         # Make sure it's in your Flask 'templates' folder or served as a static file correctly.
#         return render_template('index.html')
#     except Exception as e:
#         logging.error(f"{log_prefix} Error rendering Angular shell (index.html): {e}", exc_info=True)
#         return make_response("Error loading application page.", 500)



# REMOVE the filename=None, filesize=None that might have been added previously
# to the render_template call in the original browse_batch function.
# The old browse_batch function should be replaced by the one above.


@app.route('/download-single/<access_id>/<path:filename>')
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

# @app.route('/get/<access_id>')
# def get_file_by_access_id(access_id: str) -> Union[str, Response]:
#     logging.info(f"Request dl page via access_id: {access_id}")
#     logging.info(f"Looking up access_id '{access_id}' in DB...")
#     file_info, error_msg = find_metadata_by_access_id(access_id)
#     if error_msg or not file_info:
#         error_message_for_user = error_msg if error_msg else f"Link '{access_id}' not found or expired."
#         logging.warning(f"Failed lookup for access_id '{access_id}': {error_message_for_user}")
#     # Distinguish "not found" from server errors for status code
#         status_code = 404 if "not found" in error_message_for_user.lower() else 500
#         return make_response(render_template('404_error.html', message=error_message_for_user), status_code)
    
#     username = file_info.get('username')
#     if not username:    
#      # This shouldn't happen if records are saved correctly, but handle defensively
#         logging.error(f"Record found for access_id '{access_id}' but missing username field.")
#         message = "File record found but is incomplete (missing user info)."
#         return make_response(render_template('404_error.html', message=message), 500)

#     orig_name = file_info.get('original_filename', 'Unknown'); size = file_info.get('original_size')
#     ts_iso = file_info.get('upload_timestamp'); date_str = "Unknown date"
#     if ts_iso:
#         try:
#             dt = dateutil_parser.isoparse(ts_iso);
#             if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
#             else: dt = dt.astimezone(timezone.utc)
#             date_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
#         except Exception as e: logging.warning(f"Could not parse ts '{ts_iso}': {e}")
#     logging.info(f"Rendering dl page for '{orig_name}' (id: {access_id}).")
#     return render_template('batch-file-browser.component.html', filename=orig_name, filesize=size if size is not None else 0, upload_date=date_str, username=username, access_id=access_id)

# In routes.py, modify get_file_by_access_id:

@app.route('/api/file-details/<access_id>', methods=['GET'])
def api_get_single_file_details(access_id: str):
    log_prefix = f"[API-SingleFileDetails-{access_id}]"
    logging.info(f"{log_prefix} API request for single file details.")

    file_info, error_msg = find_metadata_by_access_id(access_id)

    if error_msg:
        logging.warning(f"{log_prefix} DB error finding file: {error_msg}")
        return jsonify({"error": "Server error retrieving file information."}), 500

    if not file_info:
        logging.warning(f"{log_prefix} File not found.")
        return jsonify({"error": f"File with ID '{access_id}' not found."}), 404

    # If it's a batch record, this API shouldn't be called for it,
    # but as a safeguard, you could return an error or different structure.
    if file_info.get('is_batch'):
        logging.warning(f"{log_prefix} Attempted to get batch info via single file API.")
        return jsonify({"error": "This ID refers to a batch, not a single file."}), 400

    # Prepare the JSON response for the single file
    # Customize this based on what your SingleFileViewComponent needs to display
    response_data = {
        "access_id": access_id,
        "original_filename": file_info.get('original_filename', 'Unknown'),
        "original_size": file_info.get('original_size', 0),
        "upload_timestamp": file_info.get('upload_timestamp'),
        "username": file_info.get('username', 'N/A'),
        "is_compressed": file_info.get('is_compressed', False), # Example additional field
        "is_split": file_info.get('is_split', False),         # Example additional field
        # Add any other relevant metadata for a single file
    }
    logging.info(f"{log_prefix} Successfully retrieved single file details.")
    return jsonify(response_data)

# @app.route('/delete-file/<username>/<path:filename>', methods=['DELETE'])
# def delete_file_record(username: str, filename: str) -> Response:
#     logging.info(f"DELETE request user='{username}', file='{filename}'")
#     logging.info(f"Attempting delete from DB: User='{username}', File='{filename}'")
#     # Using original_filename for deletion as per previous logic.
#     # Consider changing frontend/backend to use access_id for guaranteed uniqueness if needed.
#     deleted_count, error_msg = delete_metadata_by_filename(username, filename) # Call DB function

#     if error_msg:
#         logging.error(f"DB Error deleting file record for '{username}/{filename}': {error_msg}")
#         # Provide a generic server error message to the user
#         return jsonify({"error": "Server error during deletion. Please try again later."}), 500

#     if deleted_count == 0:
#         logging.warning(f"No file record found to delete for '{username}/{filename}'.")
#         return jsonify({"error": f"File '{filename}' not found for user '{username}'."}), 404
#     else:
#         # It's possible multiple records were deleted if names weren't unique
#         logging.info(f"Successfully deleted {deleted_count} record(s) for '{username}/{filename}' from DB.")
#         return jsonify({"message": f"Record for '{filename}' deleted successfully."}), 200
# logging.info("Flask routes defined using configurable workers and linter fixes.")

# routes.py

# ... (other imports and functions) ...

# MODIFIED delete_file_record function
@app.route('/delete-file/<username>/<path:access_id_from_path>', methods=['DELETE'])
@jwt_required() # Ensure only authenticated users can attempt deletion
def delete_file_record(username: str, access_id_from_path: str) -> Response:
    # username from path is mostly for URL structure, actual auth comes from JWT
    log_prefix = f"[DeleteFile-{access_id_from_path}]"
    logging.info(f"{log_prefix} DELETE request for username='{username}', access_id_from_path='{access_id_from_path}'")

    # 1. --- Get Requesting User's Identity from JWT ---
    current_user_jwt_identity = get_jwt_identity()
    if not current_user_jwt_identity: # Should be caught by @jwt_required but defensive check
        logging.warning(f"{log_prefix} JWT identity missing.")
        return jsonify({"error": "Authentication token is invalid or missing."}), 401

    user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))
    if error or not user_doc:
        logging.error(f"{log_prefix} Could not find user for JWT identity '{current_user_jwt_identity}'. Error: {error}")
        return jsonify({"error": "User not found or token invalid."}), 401

    requesting_username = user_doc.get('username')
    if not requesting_username:
        logging.error(f"{log_prefix} User document for JWT identity '{current_user_jwt_identity}' missing username.")
        return jsonify({"error": "User identity error."}), 500

    logging.info(f"{log_prefix} Request initiated by user: '{requesting_username}'.")

    # 2. --- Find the Record by access_id_from_path ---
    record_to_delete, find_err = find_metadata_by_access_id(access_id_from_path)
    if find_err:
        logging.error(f"{log_prefix} DB Error finding record by access_id: {find_err}")
        return jsonify({"error": "Server error checking record details."}), 500
    if not record_to_delete:
        logging.warning(f"{log_prefix} Record with access_id '{access_id_from_path}' not found.")
        # This message is more accurate than what the old logic would produce
        return jsonify({"error": f"Record with ID '{access_id_from_path}' not found."}), 404

    # 3. --- Verify Ownership ---
    record_owner_username = record_to_delete.get('username')
    is_anonymous_record = record_to_delete.get('is_anonymous', False)
    record_anonymous_id = record_to_delete.get('anonymous_id') # This would be for future anonymous deletion logic

    # Ownership logic:
    # - If the record has a username, it must match the requesting user's username.
    # - (Future: If it's an anonymous record, different logic might apply, e.g., matching an anonymous session ID.
    #   For now, we'll assume only named users can delete, or anonymous records aren't deletable this way).
    
    can_delete = False
    if record_owner_username == requesting_username:
        can_delete = True
    # Add anonymous deletion logic here if required in the future:
    # elif is_anonymous_record and record_anonymous_id and record_anonymous_id == some_identifier_from_requesting_anonymous_user:
    #    can_delete = True


    if not can_delete:
        logging.warning(f"{log_prefix} Permission denied: User '{requesting_username}' (from JWT) attempted to delete record "
                        f"with access_id '{access_id_from_path}' owned by '{record_owner_username}'. "
                        f"Path username was '{username}'.")
        return jsonify({"error": "Permission denied to delete this record."}), 403 # Forbidden

    # 4. --- Ownership Confirmed - Proceed with Deletion by access_id ---
    logging.info(f"{log_prefix} Ownership confirmed for user '{requesting_username}'. Attempting deletion by access_id '{access_id_from_path}'.")
    
    # Call the database function that deletes by access_id
    deleted_count, db_error_msg = database.delete_metadata_by_access_id(access_id_from_path)

    if db_error_msg:
        logging.error(f"{log_prefix} DB Error during deletion by access_id: {db_error_msg}")
        return jsonify({"error": "Server error during deletion."}), 500

    if deleted_count == 0:
        # This case should be rare if we found the record just before, but handle defensively.
        logging.warning(f"{log_prefix} Record with access_id '{access_id_from_path}' found but deletion resulted in 0 records removed.")
        return jsonify({"error": f"Failed to delete record with ID '{access_id_from_path}'. It might have been deleted by another process."}), 404 # Or 500
    else:
        logging.info(f"{log_prefix} Successfully deleted record with access_id '{access_id_from_path}'. Count: {deleted_count}")
        return jsonify({"message": f"Record for '{record_to_delete.get('batch_display_name', record_to_delete.get('original_filename', access_id_from_path))}' deleted successfully."}), 200


@app.route('/delete-batch/<access_id>', methods=['DELETE'])
@jwt_required() # Make sure only logged-in users can delete their stuff
def delete_batch_record(access_id: str) -> Response:
    log_prefix = f"[Delete-{access_id}]"
    logging.info(f"{log_prefix} DELETE request for access_id='{access_id}'")

    # --- Verify Ownership ---
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))
    if error or not user_doc:
        logging.error(f"{log_prefix} Could not find user for JWT identity '{current_user_jwt_identity}'. Error: {error}")
        return jsonify({"error": "User not found or token invalid."}), 401

    try:
         # Check if user_doc is None or does not contain 'username'
         if user_doc is None or 'username' not in user_doc:
             logging.error(f"{log_prefix} User document is invalid or missing username for identity '{current_user_jwt_identity}'.")
             return jsonify({"error": "User identity error."}), 500
         current_username = user_doc.get('username')
    except Exception as e:
     # Catch any unexpected error during user data access
         logging.error(f"{log_prefix} Error accessing user data for identity '{current_user_jwt_identity}': {e}", exc_info=True)
         return jsonify({"error": "Server error processing user data."}), 500
         
    # Find the record first to check ownership
    record_to_delete, find_err = find_metadata_by_access_id(access_id)
    if find_err:
        logging.error(f"{log_prefix} DB Error finding record: {find_err}")
        return jsonify({"error": "Server error checking record ownership."}), 500
    if not record_to_delete:
        logging.warning(f"{log_prefix} Record not found.")
        return jsonify({"error": f"Record '{access_id}' not found."}), 404

    # Check if the username from the token matches the username in the record
    record_owner = record_to_delete.get('username')
    if record_owner != current_username:
        # Add check for anonymous uploads potentially linked to the anonymous_id if you implement that later
        logging.warning(f"{log_prefix} Permission denied: User '{current_username}' attempted to delete record owned by '{record_owner}'.")
        return jsonify({"error": "Permission denied to delete this record."}), 403 # Forbidden

    # --- Ownership Confirmed - Proceed with Deletion ---
    logging.info(f"{log_prefix} Attempting delete from DB: AccessID='{access_id}', Owner='{current_username}'")
    deleted_count, error_msg = database.delete_metadata_by_access_id(access_id) # Use the new function

    if error_msg:
        logging.error(f"{log_prefix} DB Error deleting record: {error_msg}")
        return jsonify({"error": "Server error during deletion."}), 500

    if deleted_count == 0:
        # Should not happen if we found it above, but handle defensively
        logging.warning(f"{log_prefix} Record found but deletion failed (deleted_count=0).")
        return jsonify({"error": f"Failed to delete record '{access_id}'."}), 500
    else:
        logging.info(f"{log_prefix} Successfully deleted record.")
        return jsonify({"message": f"Record '{access_id}' deleted successfully."}), 200

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


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_angular_app(path: str):
    """
    Serves the Angular application's index.html for any route
    not handled by other Flask routes (like API routes).
    This allows Angular's client-side router to take over.
    """
    angular_index_path = os.path.join(app.static_folder, 'index.html')
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        # If the path exists as a static file (e.g., main.js, an image), serve it.
        return send_from_directory(app.static_folder, path)
    elif os.path.exists(angular_index_path):
        # Otherwise, for any other path, serve the Angular index.html.
        # Angular router will handle the specific frontend route.
        logging.info(f"Serving Angular index.html for path: /{path}")
        return send_from_directory(app.static_folder, 'index.html')
    else:
        logging.error(f"Angular index.html not found at: {angular_index_path}")
        return "Angular application not found.", 404



# Add this to your routes.py file

# In routes.py (add this new route)

@app.route('/api/batch-details/<access_id>', methods=['GET'])
def api_get_batch_details(access_id: str):
    log_prefix = f"[API-BatchDetails-{access_id}]"
    logging.info(f"{log_prefix} API request for batch details.")

    batch_info, error_msg = find_metadata_by_access_id(access_id)

    if error_msg:
        logging.warning(f"{log_prefix} DB error finding batch: {error_msg}")
        return jsonify({"error": "Failed to retrieve batch information due to a server error."}), 500

    if not batch_info:
        logging.warning(f"{log_prefix} Batch not found.")
        return jsonify({"error": f"Batch with ID '{access_id}' not found."}), 404

    # Ensure the structure matches what BatchDetails interface in Angular expects
    response_data = {
        "batch_name": batch_info.get('batch_display_name', f"Batch {access_id}"),
        "username": batch_info.get('username', 'N/A'),
        "upload_date": batch_info.get('upload_timestamp'), # Send as ISO string; Angular DatePipe will format it
        "total_size": batch_info.get('total_original_size', 0),
        "files": batch_info.get('files_in_batch', []), # This list should contain dicts compatible with FileInBatchInfo
        "access_id": access_id
    }

    # Validate that 'files_in_batch' items have 'original_filename' and 'original_size'
    processed_files = []
    for f_item in response_data["files"]:
        processed_f_item = f_item.copy()
        if 'original_filename' not in processed_f_item:
            processed_f_item['original_filename'] = "Unknown File"
        if 'original_size' not in processed_f_item:
            processed_f_item['original_size'] = 0
        # Ensure other expected fields like 'skipped', 'failed', 'reason' are present or defaulted if necessary
        processed_f_item.setdefault('skipped', False)
        processed_f_item.setdefault('failed', False)
        processed_f_item.setdefault('reason', None)
        processed_files.append(processed_f_item)
    response_data["files"] = processed_files

    logging.info(f"{log_prefix} Successfully retrieved and processed batch details for API.")
    return jsonify(response_data)

@app.route('/logout')
@login_required # Make sure only logged-in users can log out
def logout():
    """Logs the current user out."""
    user_email = current_user.email # Get email before logout for logging
    logout_user() # Flask-Login function to clear the session
    logging.info(f"User {user_email} logged out.")
    flash('You have been successfully logged out.', 'success')
    return redirect(url_for('login'))
