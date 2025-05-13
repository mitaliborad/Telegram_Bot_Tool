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
import io
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
from config import app 
from app_setup import login_manager, upload_progress_data, download_prep_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID, CHUNK_SIZE,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, MAX_DOWNLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES
    ,format_bytes
)
from telegram_api import send_file_to_telegram, download_telegram_file_content
from flask_jwt_extended import create_access_token

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
    logging.debug(f"Attempting to load user with ID: {user_id}")
    if not user_id:
        return None
    try:
        user_obj_id = ObjectId(user_id) # Convert here
    except Exception:
        logging.error(f"Invalid ObjectId format for user_id: {user_id}")
        return None
    user_doc, error = database.find_user_by_id(user_obj_id)
    if error:
         logging.error(f"Error loading user by ID {user_id}: {error}")
         return None
    if user_doc:
         try:
            return User(user_doc)
         except ValueError as ve:
            logging.error(f"Failed to instantiate User for ID {user_id}: {ve}")
            return None
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
def register_user(): 
    """Handles user registration submission with username."""
    logging.info("Received POST request for /register")

    if request.method == 'OPTIONS':
        return make_response(), 204
    
    # 1. --- Get Data ---
    try:
        data = request.get_json()
        if not data:
            logging.warning("Registration failed: No JSON data received.")
            return make_response(jsonify({"error": "Invalid request format. Expected JSON."}), 400)

        logging.info(f"Received registration data: {data}") 

        username = data.get('username', '').strip() 
        email = data.get('email', '').strip().lower()
        password = data.get('password', '')
        confirmPassword = data.get('confirmPassword', '')
        agreeTerms = data.get('agreeTerms', False)
        understand_privacy = data.get('understandPrivacy', False)

    except Exception as e:
        logging.error(f"Error parsing registration JSON data: {e}", exc_info=True)
        return make_response(jsonify({"error": "Invalid request data received."}), 400)

    # 2. --- Validation ---
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
    try:
        existing_user_by_username, db_error_uname = database.find_user_by_username(username)
        if db_error_uname:
            raise Exception(db_error_uname) 
        if existing_user_by_username:
            logging.warning(f"Registration failed: Username '{username}' already exists.")
            return make_response(jsonify({"error": "Username is already taken."}), 409) 
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
        save_success, save_msg = database.save_user(new_user_data)

        if not save_success:
            logging.error(f"Failed to save new user '{username}' / '{email}': {save_msg}")
            if "duplicate key error" in save_msg.lower():
                 if "username_1" in save_msg: 
                     return make_response(jsonify({"error": "Username is already taken."}), 409)
                 elif "email_1" in save_msg: 
                     return make_response(jsonify({"error": "An account with this email address already exists."}), 409)
                 else: 
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
        "user": {"username": username, "email": email}
    }), 201) 

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
        logging.info(f"{log_prefix} Authenticated upload attempt. JWT Identity: {current_user_jwt_identity}")
        is_anonymous = False
        try:
            user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))
            if error or not user_doc:
                logging.error(f"{log_prefix} Could not find user for JWT identity '{current_user_jwt_identity}'. Error: {error}")
                return jsonify({"error": "Invalid user token or user not found"}), 401 

            user_object_from_jwt = User(user_doc)
            display_username = user_object_from_jwt.username
            user_email = user_object_from_jwt.email
            logging.info(f"{log_prefix} User identified via JWT: Username='{display_username}'")

        except ValueError as ve: 
            logging.error(f"{log_prefix} Failed to instantiate User object for JWT identity '{current_user_jwt_identity}':{ve}")
            return jsonify({"error": "User data inconsistency"}), 500
        except Exception as e: 
            logging.error(f"{log_prefix} Error processing JWT identity '{current_user_jwt_identity}': {e}", exc_info=True)
            return jsonify({"error": "Server error processing authentication"}), 500
    else:
        logging.info(f"{log_prefix} Anonymous upload attempt.")
        is_anonymous = True
        anonymous_id = request.form.get('anonymous_upload_id')
        if not anonymous_id:
            logging.warning(f"{log_prefix} Anonymous upload failed: Missing 'anonymous_upload_id' in form data.")
            return jsonify({"error": "Missing required anonymous identifier for anonymous upload."}), 400
    
    
        display_username = f"AnonymousUser-{anonymous_id[:6]}" 
        user_email = None 
        logging.info(f"{log_prefix} Anonymous upload identified by temp ID: {anonymous_id}")
    
    if display_username is None:
        logging.error(f"{log_prefix} Internal state error: display_username is None after auth check.")
        return jsonify({"error": "Internal server error processing user identity."}), 500
    
    uploaded_files = request.files.getlist('files[]')
    if not uploaded_files or all(not f.filename for f in uploaded_files):
        logging.warning(f"{log_prefix} Initiate upload failed: No files provided or files have no names.")
        return jsonify({"error": "No files selected or files are invalid"}), 400


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
            "batch_display_name": batch_display_name, 
            "username": display_username, 
            "user_email": user_email,     
            "is_anonymous": is_anonymous, 
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

# --- Helper Functions ---
def _yield_sse_event(event_type: str, data: Dict[str, Any]) -> SseEvent:
    json_data = json.dumps(data); return f"event: {event_type}\ndata: {json_data}\n\n"

# routes.py


def _send_single_file_task(file_path: str, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
    log_prefix = f"[{upload_id}] Task for '{filename}' to {chat_id}"
    try:
        with open(file_path, 'rb') as f_handle:
            file_size = os.path.getsize(file_path) 
            logging.info(f"{log_prefix} Sending single file ({format_bytes(file_size)}) from path: {file_path}")
            result = send_file_to_telegram(f_handle, filename, chat_id)
        logging.info(f"{log_prefix} Single file send result: Success={result[0]}")
        return str(chat_id), result
    except FileNotFoundError:
         logging.error(f"{log_prefix} Single file not found at path: {file_path}")
         return str(chat_id), (False, f"File not found: {filename}", None)
    except Exception as e:
        logging.error(f"{log_prefix} Unexpected error opening/sending single file: {e}", exc_info=True)
        return str(chat_id), (False, f"Thread error processing single file: {e}", None)

def _safe_remove_directory(dir_path: str, log_prefix: str, description: str):
    """Safely removes a directory and its contents."""
    if not dir_path or not isinstance(dir_path, str):
        logging.warning(f"[{log_prefix}] Attempted to remove invalid directory path for {description}: {dir_path}")
        return
    if os.path.isdir(dir_path): 
        try:
            shutil.rmtree(dir_path) 
            logging.info(f"[{log_prefix}] Cleaned up {description}: {dir_path}")
        except OSError as e:
            logging.error(f"[{log_prefix}] Error deleting {description} directory '{dir_path}': {e}", exc_info=True)
    elif os.path.exists(dir_path): 
        logging.warning(f"[{log_prefix}] Cleanup skipped for {description}, path exists but is not a directory: {dir_path}")
    else:
        logging.debug(f"[{log_prefix}] Cleanup skipped, {description} directory not found: {dir_path}")

def _schedule_cleanup(temp_id: str, path: Optional[str]):
    """Safely cleans up temporary download file and state data."""
    log_prefix = f"Cleanup-{temp_id}"
    logging.info(f"[{log_prefix}] Scheduled cleanup executing for path: {path}")
    if path:
        _safe_remove_file(path, log_prefix, "final dl") 

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
    log_prefix = f"[{upload_id}] Chunk {chunk_num} ('{filename}') to {chat_id}"
    try:
        buffer = io.BytesIO(chunk_data)
        logging.info(f"{log_prefix} Sending chunk ({format_bytes(len(chunk_data))})")
        result = send_file_to_telegram(buffer, filename, chat_id)
        buffer.close()
        logging.info(f"{log_prefix} Send chunk result: Success={result[0]}")
        return str(chat_id), result
    except Exception as e: 
        logging.error(f"{log_prefix} Unexpected error sending chunk: {e}", exc_info=True)
        return str(chat_id), (False, f"Thread error processing chunk: {e}", None)

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
    logging.warning(f"[{prefix}] Expected '{expected}' not in zip. Using first entry '{names[0]}' instead.")
    return names[0]
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
# def process_upload_and_generate_updates(upload_id: str) -> Generator[SseEvent, None, None]:
#     try:
#         log_prefix = f"[{upload_id}]"
#         logging.info(f"{log_prefix} Starting processing generator (Batch Individual File Send)...")
#         upload_data = upload_progress_data.get(upload_id)

#         if not upload_data:
#             logging.error(f"{log_prefix} Critical: Upload data missing for ID.")
#             yield _yield_sse_event('error', {'message': 'Internal error: Upload data not found.'})
#             return

#         is_batch_upload = upload_data.get("is_batch", False)
#         username = upload_data['username']
#         batch_directory_path = upload_data.get("batch_directory_path")
#         original_filenames_in_batch = upload_data.get("original_filenames_in_batch", [])
        
#         batch_display_name = f"Upload ({len(original_filenames_in_batch)} files)"
#         if original_filenames_in_batch:
#             batch_display_name = f"{original_filenames_in_batch[0]} (+{len(original_filenames_in_batch)-1} others)" if len(original_filenames_in_batch) > 1 else original_filenames_in_batch[0]

#         if not is_batch_upload or not batch_directory_path or not os.path.isdir(batch_directory_path) or not original_filenames_in_batch:
#             logging.error(f"{log_prefix} Invalid batch data. is_batch={is_batch_upload}, dir={batch_directory_path}, files={original_filenames_in_batch}")
#             yield _yield_sse_event('error', {'message': 'Internal error: Invalid batch data.'})
#             if batch_directory_path and os.path.isdir(batch_directory_path):
#                 _safe_remove_directory(batch_directory_path, log_prefix, "invalid batch dir")
#             return

#         logging.info(f"{log_prefix} Processing batch: User='{username}', Dir='{batch_directory_path}', Files={original_filenames_in_batch}")
#         upload_data['status'] = 'processing_telegram'

#         access_id: Optional[str] = upload_data.get('access_id') 
#         if not access_id: 
#             access_id = uuid.uuid4().hex[:10] 
#             upload_data['access_id'] = access_id
#             logging.info(f"{log_prefix} Generated new access_id for batch: {access_id}") 
#         else:
#             logging.info(f"{log_prefix} Using existing access_id for batch: {access_id}")


#         executor: Optional[ThreadPoolExecutor] = None
#         if len(TELEGRAM_CHAT_IDS) > 1:
#             executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'Upload_{upload_id[:4]}')
#             logging.info(f"{log_prefix} Initialized Upload Executor (max={MAX_UPLOAD_WORKERS})")

#         total_original_bytes_in_batch = 0
#         files_to_process_details = []
#         for filename_in_list in original_filenames_in_batch: # Renamed to avoid conflict
#             file_path_in_list = os.path.join(batch_directory_path, filename_in_list) # Renamed
#             if os.path.exists(file_path_in_list):
#                 try:
#                     size = os.path.getsize(file_path_in_list)
#                     total_original_bytes_in_batch += size
#                     files_to_process_details.append({"path": file_path_in_list, "name": filename_in_list, "size": size})
#                 except OSError as e:
#                     logging.warning(f"{log_prefix} Could not get size for {filename_in_list}, skipping. Error: {e}")
#             else:
#                 logging.warning(f"{log_prefix} File {filename_in_list} not found in batch dir, skipping.")

#         if not files_to_process_details:
#             yield _yield_sse_event('error', {'message': 'No valid files found to upload in the batch.'})
#             _safe_remove_directory(batch_directory_path, log_prefix, "empty batch dir")
#             return

#         yield _yield_sse_event('start', {'filename': batch_display_name, 'totalSize': total_original_bytes_in_batch})
#         yield _yield_sse_event('status', {'message': f'Uploading {len(files_to_process_details)} files...'})

#         overall_start_time = time.time()
#         bytes_sent_so_far = 0
#         all_files_metadata_for_db = [] 
#         all_succeeded = True 

#         for file_detail in files_to_process_details:
#             current_file_path = file_detail["path"]
#             current_filename = file_detail["name"]
#             current_file_size = file_detail["size"]
#             log_file_prefix = f"{log_prefix} File '{current_filename}'"

#             logging.info(f"{log_file_prefix} Starting send process.")

#             file_meta_entry: Dict[str, Any] = {
#                 "original_filename": current_filename,
#                 "original_size": current_file_size,
#                 "is_split": False,
#                 "is_compressed": current_filename.lower().endswith('.zip'),
#                 "skipped": False,
#                 "failed": False,
#                 "reason": None,
#                 "send_locations": [], 
#                 "chunks": []          
#             }
            
#             if current_file_size == 0:
#                 logging.warning(f"{log_file_prefix} is empty, skipping send but including in metadata.")
#                 file_meta_entry["skipped"] = True
#                 file_meta_entry["reason"] = "File is empty"
#                 all_files_metadata_for_db.append(file_meta_entry) # Append for empty file
#                 progress_data = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
#                 yield _yield_sse_event('progress', progress_data)
#                 continue 
            
#             try:
#                 if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
#                     logging.info(f"{log_file_prefix} is large ({format_bytes(current_file_size)}), starting chunked upload.")
#                     file_meta_entry["is_split"] = True
                    
#                     part_number = 1
#                     bytes_processed_for_this_file_chunking = 0
#                     all_chunks_sent_successfully_for_this_file = True

#                 try:
#                         with open(current_file_path, 'rb') as f_in:
#                             while True:
#                                 chunk_data = f_in.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
#                                 if not chunk_data:
#                                     break 

#                                 chunk_filename = f"{current_filename}.part{part_number}"
#                                 logging.info(f"{log_file_prefix} Preparing chunk {part_number} ({format_bytes(len(chunk_data))}) as '{chunk_filename}'")

#                                 chunk_specific_futures: Dict[Future, str] = {}
#                                 chunk_specific_results: Dict[str, ApiResult] = {}
#                                 primary_send_success_for_this_chunk = False
#                                 primary_send_message_for_this_chunk = "Primary chunk send not attempted or failed."

#                                 if executor:
#                                     for chat_id_str_loop in TELEGRAM_CHAT_IDS:
#                                         cid_loop = str(chat_id_str_loop)
#                                         fut = executor.submit(_send_chunk_task, chunk_data, chunk_filename, cid_loop, upload_id, part_number)
#                                         chunk_specific_futures[fut] = cid_loop
#                                 else: 
#                                     cid_single = str(TELEGRAM_CHAT_IDS[0])
#                                     _, res_tuple_no_exec_chunk = _send_chunk_task(chunk_data, chunk_filename, cid_single, upload_id, part_number)
#                                     chunk_specific_results[cid_single] = res_tuple_no_exec_chunk
#                                     primary_send_success_for_this_chunk = res_tuple_no_exec_chunk[0]
#                                     primary_send_message_for_this_chunk = res_tuple_no_exec_chunk[1]

#                                 if chunk_specific_futures: 
#                                     primary_chunk_fut: Optional[Future] = None
#                                     primary_cid_str = str(PRIMARY_TELEGRAM_CHAT_ID)
#                                     for fut_key, chat_id_val in chunk_specific_futures.items():
#                                         if chat_id_val == primary_cid_str:
#                                             primary_chunk_fut = fut_key
#                                             break
                                    
#                                     if primary_chunk_fut:
#                                         cid_res_chunk, res_chunk = primary_chunk_fut.result()
#                                         chunk_specific_results[cid_res_chunk] = res_chunk
#                                         primary_send_success_for_this_chunk = res_chunk[0]
#                                         primary_send_message_for_this_chunk = res_chunk[1]
#                                     else: 
#                                         primary_send_success_for_this_chunk = False
#                                         primary_send_message_for_this_chunk = "Primary chat not configured for chunk."
#                                         logging.error(f"{log_file_prefix} Chunk {part_number}: Primary future not found.")
                                    
#                                     for fut_completed_chunk in as_completed(chunk_specific_futures):
#                                         cid_res_c, res_c = fut_completed_chunk.result()
#                                         if cid_res_c not in chunk_specific_results:
#                                             chunk_specific_results[cid_res_c] = res_c
                                
#                                 current_chunk_send_report_tg_format = [
#                                     {"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]}
#                                     for k, r in chunk_specific_results.items()
#                                 ]
#                                 parsed_locations_for_this_chunk = _parse_send_results(f"{log_prefix}-{current_filename}-P{part_number}", current_chunk_send_report_tg_format)

#                                 if primary_send_success_for_this_chunk:
#                                     chunk_meta_for_db = {
#                                         "part_number": part_number,
#                                         "size": len(chunk_data),
#                                         "send_locations": parsed_locations_for_this_chunk
#                                     }
#                                     file_meta_entry["chunks"].append(chunk_meta_for_db)
#                                     bytes_sent_so_far += len(chunk_data)
#                                     bytes_processed_for_this_file_chunking += len(chunk_data)

#                                     current_progress_data = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
#                                     yield _yield_sse_event('progress', current_progress_data)
#                                     yield _yield_sse_event('status', {'message': f'Uploaded chunk {part_number} for {current_filename}'})
#                                 else:
#                                     logging.error(f"{log_file_prefix} Failed to send chunk {part_number}. Reason: {primary_send_message_for_this_chunk}. Aborting for this file.")
#                                     all_succeeded = False 
#                                     all_chunks_sent_successfully_for_this_file = False
#                                     file_meta_entry["failed"] = True
#                                     file_meta_entry["reason"] = f"Failed to upload chunk {part_number}: {primary_send_message_for_this_chunk}"
#                                     break 
                                
#                                 part_number += 1
#                 except IOError as ioe:
#                     logging.error(f"{log_file_prefix} IOError during chunking: {ioe}", exc_info=True)
#                     all_succeeded = False
#                     all_chunks_sent_successfully_for_this_file = False
#                     file_meta_entry["failed"] = True
#                     file_meta_entry["reason"] = f"IOError during file chunking: {ioe}"
                
#                 if all_chunks_sent_successfully_for_this_file:
#                     file_meta_entry["compressed_total_size"] = bytes_processed_for_this_file_chunking # Sum of actual chunk bytes sent
#                     logging.info(f"{log_file_prefix} All {part_number-1} chunks processed. Success: {all_chunks_sent_successfully_for_this_file}")
#                 # `file_meta_entry` (with chunks or failure status) will be added to `all_files_metadata_for_db` later

#                 else:
            
#                     file_specific_futures: Dict[Future, str] = {}
#                     file_specific_results: Dict[str, ApiResult] = {}
#                     primary_send_success_for_this_file = False
#                     primary_send_message = "Primary send not attempted or failed."

            
#                 if executor:
#                     for chat_id_str in TELEGRAM_CHAT_IDS:
#                         cid = str(chat_id_str)
#                         # if current_file_size > (2 * 1024 * 1024 * 1024): 
#                         #     logging.error(f"{log_file_prefix} is larger than 2GB, chunking not implemented. Skipping file.")
#                         #     file_specific_results[cid] = (False, "File exceeds 2GB size limit", None)
#                         #     all_succeeded = False
#                         #     file_meta_entry = {"original_filename": current_filename, "original_size": current_file_size, "skipped": False, "failed": True, "reason": "File exceeds 2GB size limit.", "send_locations": []}
#                         #     all_files_metadata_for_db.append(file_meta_entry)
#                         #     primary_send_success_for_this_file = False
#                         #     primary_send_message = "File exceeds 2GB size limit."
#                         #     break 
#                         fut = executor.submit(_send_single_file_task, current_file_path, current_filename, cid, upload_id)
#                         file_specific_futures[fut] = cid
#                     if not primary_send_success_for_this_file and "File exceeds" in primary_send_message: 
#                          pass 
#                 else:
#                     cid = str(TELEGRAM_CHAT_IDS[0])
#                     # if current_file_size > (2 * 1024 * 1024 * 1024): 
#                     #     logging.error(f"{log_file_prefix} is larger than 2GB, chunking not implemented. Skipping file.")
#                     #     res_tuple_no_exec = (False, "File exceeds 2GB size limit", None)
#                     #     all_succeeded = False
#                     #     file_meta_entry = {"original_filename": current_filename, "original_size": current_file_size, "skipped": False, "failed": True, "reason": "File exceeds 2GB size limit.", "send_locations": []}
#                     #     all_files_metadata_for_db.append(file_meta_entry)
#                     # else:
#                     #     _, res_tuple_no_exec = _send_single_file_task(current_file_path, current_filename, cid, upload_id)
#                     # file_specific_results[cid] = res_tuple_no_exec
#                     # primary_send_success_for_this_file = res_tuple_no_exec[0]
#                     # primary_send_message = res_tuple_no_exec[1]
#                     # if not primary_send_success_for_this_file:
#                     #     all_succeeded = False
#                     _, res_tuple_no_exec = _send_single_file_task(current_file_path, current_filename, cid, upload_id)
#                     file_specific_results[cid] = res_tuple_no_exec
#                     primary_send_success_for_this_file = res_tuple_no_exec[0]
#                     primary_send_message = res_tuple_no_exec[1]
                
#                 if file_specific_futures: 
#                     primary_fut: Optional[Future] = None
#                     primary_cid_str = str(PRIMARY_TELEGRAM_CHAT_ID)
#                     for fut_key, chat_id_val in file_specific_futures.items():
#                         if chat_id_val == primary_cid_str: primary_fut = fut_key; break
                    
#                     if primary_fut:
#                         logging.debug(f"{log_file_prefix} Waiting for primary send...")
#                         cid_res, res = primary_fut.result()
#                         file_specific_results[cid_res] = res
#                         primary_send_success_for_this_file = res[0]
#                         primary_send_message = res[1]
#                         if not primary_send_success_for_this_file:
#                             all_succeeded = False
#                             logging.error(f"{log_file_prefix} Primary send failed: {primary_send_message}")
#                     else: 
#                         logging.warning(f"{log_file_prefix} Primary future not found (CID: {primary_cid_str}). Assuming failure for safety.")
#                         primary_send_success_for_this_file = False
#                         primary_send_message = "Primary Telegram chat not configured or send task failed to initialize."
#                         all_succeeded = False
#                     if not primary_send_success_for_this_file: all_succeeded = False
                    
#                     logging.debug(f"{log_file_prefix} Waiting for backup sends...")
#                     for fut_completed in as_completed(file_specific_futures):
#                         cid_res, res = fut_completed.result()
#                         if cid_res not in file_specific_results: file_specific_results[cid_res] = res
                
#                 # --- Process results for *this specific file* ---
#                 current_file_send_report = [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k, r in file_specific_results.items()]
#                 parsed_locations_for_this_file = _parse_send_results(f"{log_prefix}-{current_filename}", current_file_send_report)

#                 if primary_send_success_for_this_file:
#                     bytes_sent_so_far += current_file_size 
#                     primary_parsed_loc = next((loc for loc in parsed_locations_for_this_file if loc.get('chat_id') == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
#                     if not primary_parsed_loc or not primary_parsed_loc.get('success'):
#                         logging.error(f"{log_file_prefix} Primary send reported OK by API but parsing failed or IDs missing. Marking as failed.")
#                         all_succeeded = False 
#                         file_meta_entry["failed"] = True
#                         file_meta_entry["reason"] = "Primary send metadata parsing failed."
#                         file_meta_entry["send_locations"] = parsed_locations_for_this_file
#                     else:
#                         file_meta_entry["send_locations"] = parsed_locations_for_this_file
#                         logging.info(f"{log_file_prefix} Successfully processed (single file) and recorded.")
#                 else: # Primary send failed for non-split file
#                     all_succeeded = False
#                     file_meta_entry["failed"] = True
#                     file_meta_entry["reason"] = f"Primary send failed: {primary_send_message}"
#                     file_meta_entry["send_locations"] = parsed_locations_for_this_file
#                     logging.error(f"{log_file_prefix} Failed primary send (single file). Reason: {primary_send_message}")
                    
#                 all_files_metadata_for_db.append(file_meta_entry)

#             except Exception as file_processing_exception:
#                 logging.error(f"{log_file_prefix} Unexpected error processing this file: {file_processing_exception}", exc_info=True)
#                 file_meta_entry["failed"] = True
#                 file_meta_entry["reason"] = f"Unexpected internal error during file processing: {str(file_processing_exception)}"
#                 all_succeeded = False
                
#             all_files_metadata_for_db.append(file_meta_entry)

#             progress_data_after_file = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
#             yield _yield_sse_event('progress', progress_data_after_file)
#             yield _yield_sse_event('status', {'message': f'Processed {len(all_files_metadata_for_db)} of {len(files_to_process_details)} files...'})
        
#         total_batch_duration = time.time() - overall_start_time
#         logging.info(f"{log_prefix} Finished processing all files in batch. Duration: {total_batch_duration:.2f}s. Overall Primary Success: {all_succeeded}")

#         if not all_files_metadata_for_db:
#             logging.error(f"{log_prefix} Critical: No metadata was generated for any file after processing loop.")
#             raise RuntimeError("Processing finished but no metadata was generated for any file.")

#         final_batch_had_errors = any(fme.get("failed", False) for fme in all_files_metadata_for_db)
        
#         db_batch_timestamp = datetime.now(timezone.utc).isoformat()
#         db_batch_record = {
#             "access_id": access_id, 
#             "username": upload_data['username'],
#             "is_anonymous": upload_data.get('is_anonymous', False),
#             "anonymous_id": upload_data.get('anonymous_id'),
#             "upload_timestamp": db_batch_timestamp,
#             "is_batch": True,
#             "batch_display_name": batch_display_name,
#             "files_in_batch": all_files_metadata_for_db,
#             "total_original_size": total_original_bytes_in_batch,
#             "total_upload_duration_seconds": round(total_batch_duration, 2),
#         }
#         if db_batch_record["anonymous_id"] is None:
#             del db_batch_record["anonymous_id"]
        
#         logging.debug(f"Attempting to save batch metadata: {json.dumps(db_batch_record, indent=2, default=str)}")

#         save_success, save_msg = save_file_metadata(db_batch_record)
#         if not save_success:
#             logging.error(f"{log_prefix} DB CRITICAL: Failed to save batch metadata: {save_msg}")
#             # Yield error, but also set upload_data status correctly
#             upload_data['status'] = 'completed_metadata_error' # New status
#             upload_data['error'] = f"Failed to save batch metadata: {save_msg}"
#             yield _yield_sse_event('error', {'message': f"Batch upload processed, but failed to save details: {save_msg}"})
#             # No `raise IOError` here, let finally block handle cleanup
#             return
#         else:
#             logging.info(f"{log_prefix} DB: Successfully saved batch metadata.")
        
#         base_url = request.host_url.rstrip('/')
#         browser_url_path = f"/browse/{access_id}"
#         browser_url = f"{base_url}{browser_url_path}"
        

#         if not access_id or not isinstance(access_id, str) or len(access_id.strip()) == 0:
#             logging.error(f"{log_prefix} CRITICAL: access_id is invalid ('{access_id}') before yielding 'complete' event. This should not happen.")
#             yield _yield_sse_event('error', {'message': 'Internal server error: Failed to generate a valid batch identifier.'})
#             upload_data['status'] = 'error'
#             upload_data['error'] = 'Invalid batch identifier generated'
#             return 
#         complete_payload = {
#             'message': f'Batch upload ({len(files_to_process_details)} files) complete!',
#             'download_url': browser_url,
#             'filename': batch_display_name,
#             'batch_access_id': access_id 
#         }
#         logging.info(f"{log_prefix} Preparing to yield 'complete' event. Access ID: '{access_id}', Payload: {json.dumps(complete_payload)}")
            
        
#         upload_data['status'] = 'completed' if all_succeeded else 'completed_with_errors'

#     except Exception as e:
#         logging.error(f"{log_prefix} An exception occurred during batch upload processing:", exc_info=True)
#         error_msg_final = f"Batch upload processing failed: {str(e) or type(e).__name__}"
#         logging.error(f"{log_prefix} Sending SSE error to client: {error_msg_final}")
#         yield _yield_sse_event('error', {'message': error_msg_final})
#         if upload_id in upload_progress_data:
#             upload_data['status'] = 'error'
#             upload_data['error'] = error_msg_final
#     finally:
#         logging.info(f"{log_prefix} Batch upload generator final cleanup.")
#         if executor:
#             executor.shutdown(wait=False)
#             logging.info(f"{log_prefix} Upload executor shutdown.")
        
#         if batch_directory_path and os.path.exists(batch_directory_path):
#              logging.warning(f"{log_prefix} Cleaning up batch directory that might have been left over: {batch_directory_path}")
#              _safe_remove_directory(batch_directory_path, log_prefix, "lingering batch dir")

#         final_status_report = upload_progress_data.get(upload_id, {}).get('status', 'unknown')
#         logging.info(f"{log_prefix} Batch processing generator finished. Final Status: {final_status_report}")

def process_upload_and_generate_updates(upload_id: str) -> Generator[SseEvent, None, None]:
    # Initialize variables that might be in finally block outside the try
    executor: Optional[ThreadPoolExecutor] = None
    batch_directory_path: Optional[str] = None # Initialize to ensure it's defined for finally

    try:
        log_prefix = f"[{upload_id}]"
        logging.info(f"{log_prefix} Starting processing generator...")
        upload_data = upload_progress_data.get(upload_id)

        if not upload_data:
            logging.error(f"{log_prefix} Critical: Upload data missing for ID.")
            yield _yield_sse_event('error', {'message': 'Internal error: Upload data not found.'})
            return

        username = upload_data['username']
        batch_directory_path = upload_data.get("batch_directory_path") # Assign here
        original_filenames_in_batch = upload_data.get("original_filenames_in_batch", [])
        
        batch_display_name = f"Upload ({len(original_filenames_in_batch)} files)"
        if original_filenames_in_batch:
            batch_display_name = f"{original_filenames_in_batch[0]} (+{len(original_filenames_in_batch)-1} others)" if len(original_filenames_in_batch) > 1 else original_filenames_in_batch[0]

        if not upload_data.get("is_batch") or not batch_directory_path or not os.path.isdir(batch_directory_path) or not original_filenames_in_batch:
            logging.error(f"{log_prefix} Invalid batch data provided.")
            yield _yield_sse_event('error', {'message': 'Internal error: Invalid batch data.'})
            if batch_directory_path and os.path.isdir(batch_directory_path): # Check if path is valid before removing
                _safe_remove_directory(batch_directory_path, log_prefix, "invalid batch dir")
            return

        logging.info(f"{log_prefix} Processing batch: User='{username}', Dir='{batch_directory_path}', Files={original_filenames_in_batch}")
        upload_data['status'] = 'processing_telegram'

        access_id: Optional[str] = upload_data.get('access_id') 
        if not access_id: 
            access_id = uuid.uuid4().hex[:10] 
            upload_data['access_id'] = access_id
            logging.info(f"{log_prefix} Generated new access_id for batch: {access_id}") 
        else:
            logging.info(f"{log_prefix} Using existing access_id for batch: {access_id}")

        if len(TELEGRAM_CHAT_IDS) > 0: # Ensure there's at least one chat ID
            executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'Upload_{upload_id[:4]}')
            logging.info(f"{log_prefix} Initialized Upload Executor (max={MAX_UPLOAD_WORKERS})")
        else:
            logging.error(f"{log_prefix} No Telegram chat IDs configured. Cannot upload.")
            yield _yield_sse_event('error', {'message': 'Server configuration error: No destination chats.'})
            return


        total_original_bytes_in_batch = 0
        files_to_process_details = []
        for filename_in_list in original_filenames_in_batch:
            file_path_in_list = os.path.join(batch_directory_path, filename_in_list)
            if os.path.exists(file_path_in_list):
                try:
                    size = os.path.getsize(file_path_in_list)
                    total_original_bytes_in_batch += size
                    files_to_process_details.append({"path": file_path_in_list, "name": filename_in_list, "size": size})
                except OSError as e:
                    logging.warning(f"{log_prefix} Could not get size for {filename_in_list}, skipping. Error: {e}")
            else:
                logging.warning(f"{log_prefix} File {filename_in_list} not found in batch dir {batch_directory_path}, skipping.")

        if not files_to_process_details:
            yield _yield_sse_event('error', {'message': 'No valid files found to upload in the batch.'})
            _safe_remove_directory(batch_directory_path, log_prefix, "empty batch dir after file check")
            return

        yield _yield_sse_event('start', {'filename': batch_display_name, 'totalSize': total_original_bytes_in_batch})
        yield _yield_sse_event('status', {'message': f'Uploading {len(files_to_process_details)} files...'})

        overall_start_time = time.time()
        bytes_sent_so_far = 0
        all_files_metadata_for_db = [] 
        batch_overall_success = True # Tracks if all files in the batch were processed without primary failure

        for file_detail in files_to_process_details:
            current_file_path = file_detail["path"]
            current_filename = file_detail["name"]
            current_file_size = file_detail["size"]
            log_file_prefix_indiv = f"{log_prefix} File '{current_filename}'" # Unique prefix for this file

            logging.info(f"{log_file_prefix_indiv} Starting send process.")

            file_meta_entry: Dict[str, Any] = {
                "original_filename": current_filename,
                "original_size": current_file_size,
                "is_split": False, "is_compressed": current_filename.lower().endswith('.zip'),
                "skipped": False, "failed": False, "reason": None,
                "send_locations": [], "chunks": []          
            }
            
            if current_file_size == 0:
                logging.warning(f"{log_file_prefix_indiv} is empty, skipping.")
                file_meta_entry["skipped"] = True
                file_meta_entry["reason"] = "File is empty"
                all_files_metadata_for_db.append(file_meta_entry)
                # Progress update for the batch (0 bytes for this file)
                current_batch_progress = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
                yield _yield_sse_event('progress', current_batch_progress)
                continue 
            
            # --- Main processing logic for a single file (chunked or not) ---
            try:
                if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
                    # --- CHUNKING LOGIC ---
                    logging.info(f"{log_file_prefix_indiv} is large ({format_bytes(current_file_size)}), starting chunked upload.")
                    file_meta_entry["is_split"] = True
                    part_number = 1
                    bytes_processed_for_this_file_chunking = 0
                    all_chunks_sent_successfully_for_this_file = True

                    with open(current_file_path, 'rb') as f_in: # File handle for reading chunks
                        while True:
                            chunk_data = f_in.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
                            if not chunk_data: break 

                            chunk_filename = f"{current_filename}.part{part_number}"
                            log_chunk_prefix = f"{log_file_prefix_indiv} Chunk {part_number}"
                            logging.info(f"{log_chunk_prefix} Preparing ({format_bytes(len(chunk_data))}) as '{chunk_filename}'")

                            chunk_specific_futures: Dict[Future, str] = {}
                            chunk_specific_results: Dict[str, ApiResult] = {}
                            primary_send_success_for_this_chunk = False
                            primary_send_message_for_this_chunk = "Primary chunk send not attempted or failed."

                            if executor:
                                for chat_id_str_loop in TELEGRAM_CHAT_IDS:
                                    cid_loop = str(chat_id_str_loop)
                                    fut = executor.submit(_send_chunk_task, chunk_data, chunk_filename, cid_loop, upload_id, part_number)
                                    chunk_specific_futures[fut] = cid_loop
                            else: 
                                cid_single = str(TELEGRAM_CHAT_IDS[0])
                                _, res_tuple_no_exec_chunk = _send_chunk_task(chunk_data, chunk_filename, cid_single, upload_id, part_number)
                                chunk_specific_results[cid_single] = res_tuple_no_exec_chunk
                                primary_send_success_for_this_chunk = res_tuple_no_exec_chunk[0]
                                primary_send_message_for_this_chunk = res_tuple_no_exec_chunk[1]
                            
                            if chunk_specific_futures: 
                                primary_chunk_fut: Optional[Future] = None
                                primary_cid_str_chunk = str(PRIMARY_TELEGRAM_CHAT_ID)
                                for fut_key, chat_id_val in chunk_specific_futures.items():
                                    if chat_id_val == primary_cid_str_chunk: primary_chunk_fut = fut_key; break
                                if primary_chunk_fut:
                                    cid_res_chunk, res_chunk = primary_chunk_fut.result()
                                    chunk_specific_results[cid_res_chunk] = res_chunk
                                    primary_send_success_for_this_chunk = res_chunk[0]
                                    primary_send_message_for_this_chunk = res_chunk[1]
                                else: 
                                    primary_send_success_for_this_chunk = False; primary_send_message_for_this_chunk = "Primary chat not configured for chunk."
                                for fut_completed_chunk in as_completed(chunk_specific_futures):
                                    cid_res_c, res_c = fut_completed_chunk.result()
                                    if cid_res_c not in chunk_specific_results: chunk_specific_results[cid_res_c] = res_c
                            
                            parsed_locations_for_this_chunk = _parse_send_results(f"{log_chunk_prefix}", 
                                [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k, r in chunk_specific_results.items()])

                            if primary_send_success_for_this_chunk:
                                file_meta_entry["chunks"].append({"part_number": part_number, "size": len(chunk_data), "send_locations": parsed_locations_for_this_chunk})
                                bytes_sent_so_far += len(chunk_data)
                                bytes_processed_for_this_file_chunking += len(chunk_data)
                                current_batch_progress_chunk = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
                                yield _yield_sse_event('progress', current_batch_progress_chunk)
                                yield _yield_sse_event('status', {'message': f'Uploaded chunk {part_number} for {current_filename}'})
                            else:
                                logging.error(f"{log_chunk_prefix} Failed. Reason: {primary_send_message_for_this_chunk}. Aborting for this file.")
                                batch_overall_success = False; all_chunks_sent_successfully_for_this_file = False
                                file_meta_entry["failed"] = True; file_meta_entry["reason"] = f"Failed chunk {part_number}: {primary_send_message_for_this_chunk}"
                                break # Break from while True (chunk loop for this file)
                            part_number += 1
                    # End of `with open` for chunking
                    if all_chunks_sent_successfully_for_this_file: # If loop completed without break
                        file_meta_entry["compressed_total_size"] = bytes_processed_for_this_file_chunking
                        logging.info(f"{log_file_prefix_indiv} All {part_number-1} chunks processed successfully.")
                    # If loop broke due to chunk failure, file_meta_entry["failed"] is already True.

                else: # --- SINGLE FILE (NON-CHUNKED) LOGIC ---
                    logging.info(f"{log_file_prefix_indiv} Sending as single file ({format_bytes(current_file_size)}).")
                    # Distinct variable names for single file futures/results to avoid UnboundLocalError
                    single_file_futures: Dict[Future, str] = {}
                    single_file_results: Dict[str, ApiResult] = {}
                    primary_send_success_for_single_file = False
                    primary_send_message_single_file = "Primary send (single) not attempted or failed."

                    if executor:
                        for chat_id_str_single in TELEGRAM_CHAT_IDS:
                            cid_single_loop = str(chat_id_str_single)
                            fut_single = executor.submit(_send_single_file_task, current_file_path, current_filename, cid_single_loop, upload_id)
                            single_file_futures[fut_single] = cid_single_loop
                    else:
                        cid_single_no_exec = str(TELEGRAM_CHAT_IDS[0])
                        _, res_tuple_single_no_exec = _send_single_file_task(current_file_path, current_filename, cid_single_no_exec, upload_id)
                        single_file_results[cid_single_no_exec] = res_tuple_single_no_exec
                        primary_send_success_for_single_file = res_tuple_single_no_exec[0]
                        primary_send_message_single_file = res_tuple_single_no_exec[1]

                    if single_file_futures:
                        primary_fut_single: Optional[Future] = None
                        primary_cid_str_single = str(PRIMARY_TELEGRAM_CHAT_ID)
                        for fut_key_s, chat_id_val_s in single_file_futures.items():
                            if chat_id_val_s == primary_cid_str_single: primary_fut_single = fut_key_s; break
                        if primary_fut_single:
                            cid_res_s, res_s = primary_fut_single.result()
                            single_file_results[cid_res_s] = res_s
                            primary_send_success_for_single_file = res_s[0]; primary_send_message_single_file = res_s[1]
                        else:
                            primary_send_success_for_single_file = False; primary_send_message_single_file = "Primary chat not configured (single file)."
                        for fut_completed_s in as_completed(single_file_futures):
                            cid_res_s_comp, res_s_comp = fut_completed_s.result()
                            if cid_res_s_comp not in single_file_results: single_file_results[cid_res_s_comp] = res_s_comp
                    
                    parsed_locations_single_file = _parse_send_results(f"{log_file_prefix_indiv}", 
                        [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k,r in single_file_results.items()])

                    if primary_send_success_for_single_file:
                        bytes_sent_so_far += current_file_size
                        file_meta_entry["send_locations"] = parsed_locations_single_file
                        logging.info(f"{log_file_prefix_indiv} Successfully processed (single file).")
                    else:
                        batch_overall_success = False
                        file_meta_entry["failed"] = True
                        file_meta_entry["reason"] = f"Primary send failed (single): {primary_send_message_single_file}"
                        file_meta_entry["send_locations"] = parsed_locations_single_file
                        logging.error(f"{log_file_prefix_indiv} Failed (single). Reason: {primary_send_message_single_file}")

            except Exception as file_processing_exception: # Catch errors from if/else block
                logging.error(f"{log_file_prefix_indiv} Unexpected error processing this file: {file_processing_exception}", exc_info=True)
                file_meta_entry["failed"] = True
                file_meta_entry["reason"] = f"Unexpected internal error: {str(file_processing_exception)}"
                batch_overall_success = False
            
            all_files_metadata_for_db.append(file_meta_entry)
            progress_data_after_file = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
            yield _yield_sse_event('progress', progress_data_after_file)
            yield _yield_sse_event('status', {'message': f'Processed {len(all_files_metadata_for_db)} of {len(files_to_process_details)} files...'})
        
        # End of loop for file_detail
        total_batch_duration = time.time() - overall_start_time
        logging.info(f"{log_prefix} Finished all files processing. Duration: {total_batch_duration:.2f}s. Overall Batch Success State: {batch_overall_success}")

        if not all_files_metadata_for_db: # Should not happen if logic is correct
            logging.error(f"{log_prefix} CRITICAL: No metadata generated after processing loop.")
            # This indicates a fundamental flaw if reached. Send error and exit.
            yield _yield_sse_event('error', {'message': 'Internal server error: Failed to record upload details.'})
            upload_data['status'] = 'error'; upload_data['error'] = "No metadata generated"
            return

        final_batch_had_errors = any(fme.get("failed", False) for fme in all_files_metadata_for_db)
        if final_batch_had_errors: batch_overall_success = False # Ensure this flag is accurate

        db_batch_record = {
            "access_id": access_id, "username": upload_data['username'],
            "is_anonymous": upload_data.get('is_anonymous', False), "anonymous_id": upload_data.get('anonymous_id'),
            "upload_timestamp": datetime.now(timezone.utc).isoformat(), "is_batch": True,
            "batch_display_name": batch_display_name, "files_in_batch": all_files_metadata_for_db,
            "total_original_size": total_original_bytes_in_batch,
            "total_upload_duration_seconds": round(total_batch_duration, 2),
        }
        if db_batch_record["anonymous_id"] is None: del db_batch_record["anonymous_id"]
        
        save_success, save_msg = save_file_metadata(db_batch_record)
        if not save_success:
            logging.error(f"{log_prefix} DB CRITICAL: Failed to save batch metadata: {save_msg}")
            upload_data['status'] = 'completed_metadata_error'; upload_data['error'] = f"DB save fail: {save_msg}"
            yield _yield_sse_event('error', {'message': f"Upload processed, but failed to save details: {save_msg}"})
            return
        logging.info(f"{log_prefix} DB: Successfully saved batch metadata.")
        
        browser_url = f"{request.host_url.rstrip('/')}/browse/{access_id}"
        complete_message = f'Batch upload ({len(files_to_process_details)} files) ' + \
                           ('completed with errors.' if final_batch_had_errors else 'complete!')
        complete_payload = {
            'message': complete_message, 'download_url': browser_url,
            'filename': batch_display_name, 'batch_access_id': access_id 
        }
        upload_data['status'] = 'completed_with_errors' if final_batch_had_errors else 'completed'
        logging.info(f"{log_prefix} Yielding '{upload_data['status']}' event. Payload: {json.dumps(complete_payload)}")
        yield _yield_sse_event('complete', complete_payload)

    except Exception as e: # Outermost catch-all for the generator
        logging.error(f"{log_prefix} UNHANDLED EXCEPTION in upload generator: {e}", exc_info=True)
        error_msg_final = f"Critical upload processing error: {str(e) or type(e).__name__}"
        yield _yield_sse_event('error', {'message': error_msg_final})
        if upload_id in upload_progress_data: # Should exist
            upload_progress_data[upload_id]['status'] = 'error'
            upload_progress_data[upload_id]['error'] = error_msg_final
    finally:
        logging.info(f"{log_prefix} Upload generator final cleanup.")
        if executor:
            executor.shutdown(wait=True) # Changed to wait=True to ensure tasks (like file closing) finish
            logging.info(f"{log_prefix} Upload executor shutdown (waited).")
        
        if batch_directory_path and os.path.exists(batch_directory_path):
             logging.info(f"{log_prefix} Cleaning up batch temp directory: {batch_directory_path}")
             _safe_remove_directory(batch_directory_path, log_prefix, "batch temp dir in finally")

        final_status_report = upload_progress_data.get(upload_id, {}).get('status', 'unknown')
        logging.info(f"{log_prefix} Upload processing generator finished. Final Status: {final_status_report}")

# Download Preparation Route ---
@app.route('/prepare-download/<username>/<path:filename>') 
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
    prep_id = str(uuid.uuid4()) 
    logging.info(f"[{prep_id}] SSE connection request for dl prep via access_id: {access_id}")
    file_info, error_msg = find_metadata_by_access_id(access_id)
    username = file_info.get('username')
    if not file_info or not username:
         logging.warning(f"[{prep_id}] Invalid access_id '{access_id}' for SSE stream.")
         def error_stream(): yield _yield_sse_event('error', {'message':'Invalid or expired download link.'})
         return Response(stream_with_context(error_stream()), mimetype='text/event-stream')

    download_prep_data[prep_id] = {
         "prep_id": prep_id,
         "status": "initiated",
         "username": username, 
         "requested_filename": file_info.get('original_filename', 'unknown'), 
         "original_filename": file_info.get('original_filename', 'unknown'), 
         "access_id": access_id,
         "error": None,
         "final_temp_file_path": None,
         "final_file_size": 0,
         "start_time": time.time()
    }
    logging.debug(f"[{prep_id}] Stored initial prep data for access_id lookup. Calling generator.")
    return Response(stream_with_context(
         _prepare_download_and_generate_updates(prep_id)
    ), mimetype='text/event-stream')

# def _prepare_download_and_generate_updates(prep_id: str) -> Generator[SseEvent, None, None]:
#     """
#     Generator handling download preparation and yielding SSE updates.
#     Handles:
#     1. Lookup by access_id/username+filename (for ZIPs or single-file records).
#     2. Direct download of a single file using a pre-supplied telegram_file_id.
#     """
#     log_prefix = f"[DLPrep-{prep_id}]"
#     logging.info(f"{log_prefix} Download prep generator started.")
#     prep_data = download_prep_data.get(prep_id)

#     if not prep_data:
#         logging.error(f"{log_prefix} Critical: Prep data missing at generator start.")
#         yield _yield_sse_event('error', {'message': 'Internal Server Error: Prep data lost.'})
#         return

#     if prep_data.get('status') != 'initiated':
#         logging.warning(f"{log_prefix} Prep already running/finished (Status: {prep_data.get('status')}). Aborting.")
#         return

#     prep_data['status'] = 'preparing'

#     # --- Initialize variables ---
#     temp_reassembled_zip_path: Optional[str] = None
#     temp_decompressed_path: Optional[str] = None
#     temp_final_file_path: Optional[str] = None 

#     file_info: Optional[Dict[str, Any]] = None 
#     username = prep_data.get('username')
#     access_id = prep_data.get('access_id')
#     requested_filename = prep_data.get('requested_filename')
#     direct_telegram_file_id = prep_data.get('telegram_file_id')
#     is_direct_single_file_download = bool(direct_telegram_file_id)
#     original_filename_from_meta = prep_data.get('original_filename', requested_filename or "download")
#     final_expected_size = prep_data.get('final_expected_size', 0) 
#     is_split = prep_data.get('is_split', False)
#     chunks_meta_from_prep = prep_data.get('chunks_meta') 
#     is_compressed = prep_data.get('is_compressed', False)

#     total_bytes_to_fetch = 0 
#     download_executor: Optional[ThreadPoolExecutor] = None

#     try:
#         # --- Phase 1: Get File Info (Lookup or Direct) ---
#         if not is_direct_single_file_download and not is_split:
#             yield _yield_sse_event('status', {'message': 'Looking up file info...'}); time.sleep(0.1)
#         if not prep_data.get('telegram_file_id') and not prep_data.get('chunks_meta'): # Neither direct single file ID nor chunk info is present in prep_data
#             # This means we need to look up the main metadata record
#             yield _yield_sse_event('status', {'message': 'Looking up file info...'}); time.sleep(0.1)
#             lookup_error_msg = ""
#             if access_id:
#                 logging.debug(f"{log_prefix} Finding metadata by access_id: {access_id}")
#                 file_info, lookup_error_msg = find_metadata_by_access_id(access_id)
#                 if not file_info and not lookup_error_msg: lookup_error_msg = f"Access ID '{access_id}' not found."
#                 elif file_info and not file_info.get('username'): 
#                      lookup_error_msg = "Record found but missing username field."
#                      file_info = None 
#             elif username and requested_filename: 
#                 logging.debug(f"{log_prefix} Finding metadata by username/filename: User='{username}', File='{requested_filename}'")
#                 all_user_files, lookup_error_msg = find_metadata_by_username(username)
#                 if not lookup_error_msg and all_user_files is not None:
#                     file_info = next((f for f in all_user_files if f.get('original_filename') == requested_filename), None)
#                     if not file_info: lookup_error_msg = f"File '{requested_filename}' not found for user '{username}'."
#                 elif not lookup_error_msg: lookup_error_msg = "Internal error: Failed to retrieve user file list."
#             else:
#                 lookup_error_msg = "Insufficient information for metadata lookup."

#             if lookup_error_msg or not file_info:
#                 final_error_message = lookup_error_msg or "File metadata not found."
#                 logging.error(f"{log_prefix} Metadata lookup failed: {final_error_message}")
#                 raise FileNotFoundError(final_error_message)
#             original_filename_from_meta = file_info.get('original_filename', requested_filename or 'unknown');
#             final_expected_size = file_info.get('original_size', 0)
#             is_split = file_info.get('is_split', False)
#             if is_split:
#                 chunks_meta_from_prep = file_info.get('chunks')
#             else: # Not split, expect send_locations for single TG file
#                 locations = file_info.get('send_locations', [])
#                 tg_file_id_lookup, _ = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID)
#                 if not tg_file_id_lookup: raise ValueError("No Telegram file ID in metadata for non-split file.")
#                 prep_data['telegram_file_id'] = tg_file_id_lookup
#             is_compressed = file_info.get('is_compressed', True) 
#             prep_data['original_filename'] = original_filename_from_meta
#             prep_data['final_expected_size'] = final_expected_size
#             prep_data['is_split'] = is_split
#             prep_data['chunks_meta'] = chunks_meta_from_prep
#             prep_data['is_compressed'] = is_compressed
#             prep_data['compressed_total_size'] = file_info.get('compressed_total_size', 0)
#             logging.info(f"{log_prefix} Meta Found: '{original_filename_from_meta}', Size:{final_expected_size}, Split:{is_split}, Comp:{is_compressed}")
#         else:
#             # --- Direct Single File Download Path ---
#             logging.info(f"{log_prefix} Using data from prep_data: '{original_filename_from_meta}', Split:{is_split}, Comp:{is_compressed}")
#             original_filename_from_meta = requested_filename
#             is_split = False
#             is_compressed = original_filename_from_meta.lower().endswith('.zip') 
#             logging.info(f"{log_prefix} Direct Single File: '{original_filename_from_meta}', Size:{final_expected_size}, Split:{is_split}, Comp:{is_compressed}")
#         yield _yield_sse_event('filename', {'filename': original_filename_from_meta})
#         yield _yield_sse_event('totalSizeUpdate', {'totalSize': final_expected_size}) 
#         yield _yield_sse_event('progress', {'percentage': 0})
#         yield _yield_sse_event('status', {'message': 'Preparing file...'}); time.sleep(0.2)

#         # --- Phase 2: File Preparation ---
#         if is_split: # <<< THIS IS THE NEW MAJOR BRANCH
#             if not chunks_meta_from_prep: # Should have been populated if is_split is true
#                  raise RuntimeError(f"{log_prefix} File marked as split but no chunk metadata found.")
#             logging.info(f"{log_prefix} Preparing SPLIT file download for '{original_filename_from_meta}'")
#             yield _yield_sse_event('status', {'message': 'Downloading & Reassembling chunks...'})
#             yield _yield_sse_event('progress', {'percentage': 5})

#             try:
#                 chunks_meta_from_prep.sort(key=lambda c: int(c.get('part_number', 0)))
#             except (TypeError, ValueError):
#                 raise ValueError("Invalid 'part_number' in chunks metadata for sorting.");
            
#             num_chunks = len(chunks_meta_from_prep)
#             if num_chunks == 0: raise ValueError("Chunks list is empty in metadata.")

#             total_bytes_to_fetch = prep_data.get('compressed_total_size', 0) # Sum of TG chunk sizes
#             if total_bytes_to_fetch == 0: # Estimate if not present
#                 total_bytes_to_fetch = sum(c.get('size',0) for c in chunks_meta_from_prep)

#             logging.info(f"{log_prefix} Expecting {num_chunks} chunks. Total compressed size: ~{format_bytes(total_bytes_to_fetch)}.")
#             start_fetch_time = time.time()
#             fetched_bytes_count = 0
#             fetch_percentage_target = 80.0 
#             downloaded_chunk_count = 0
#             download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DlPrep_{prep_id[:4]}')
#             logging.info(f"{log_prefix} Initialized Download Executor (max={MAX_DOWNLOAD_WORKERS})")
            
#             submitted_futures: List[Future] = []
#             downloaded_content_map: Dict[int, bytes] = {} # part_number -> content
#             first_download_error: Optional[str] = None

            
#         #     tg_file_id_to_download = None
#         #     if is_direct_single_file_download:
#         #         tg_file_id_to_download = direct_telegram_file_id
#         #         logging.info(f"{log_prefix} Using direct telegram_file_id: {tg_file_id_to_download}")
#         #     elif file_info:
#         #         if file_info.get('is_batch', False):
#         #             logging.error(
#         #                 f"{log_prefix} Attempt to download a batch record (access_id: {access_id}) "
#         #                 f"as if it were a single file. This typically means the download "
#         #                 f"button on the batch browse page is misconfigured to call "
#         #                 f"/stream-download/ instead of /download-single/ for an item "
#         #                 f"or /initiate-download-all/ for the entire batch."
#         #             )
#         #             raise ValueError(
#         #                 "This link refers to a batch of files. "
#         #                 "To download all files, use the 'Download All' option. "
#         #                 "To download a specific file, click its individual download button from the list."
#         #             )
#         #         locations = file_info.get('send_locations', [])
#         #         tg_file_id_lookup, chat_id_lookup = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID)
#         #         tg_file_id_to_download = tg_file_id_lookup
#         #         if tg_file_id_to_download:
#         #             logging.info(f"{log_prefix} Using looked-up telegram_file_id: {tg_file_id_to_download} from chat {chat_id_lookup}")

#         #     if not tg_file_id_to_download:
#         #         raise ValueError("Could not determine Telegram file ID to download.")

#         #     start_dl = time.time()
#         #     content_bytes, err_msg = download_telegram_file_content(tg_file_id_to_download)
#         #     dl_duration = time.time() - start_dl
#         #     dl_bytes_count = len(content_bytes) if content_bytes else 0
#         #     dl_speed_mbps = (dl_bytes_count / (1024*1024) / dl_duration) if dl_duration > 0 else 0
#         #     logging.info(f"{log_prefix} TG download ({dl_bytes_count} bytes) in {dl_duration:.2f}s. Speed: {dl_speed_mbps:.2f} MB/s")

#         #     if err_msg: raise ValueError(f"TG download failed: {err_msg}")
#         #     if not content_bytes: raise ValueError("TG download returned empty content.")
#         #     progress_total = final_expected_size if final_expected_size > 0 else dl_bytes_count
#         #     yield _yield_sse_event('progress', {'percentage': 50, 'bytesProcessed': int(progress_total*0.5), 'totalBytes': progress_total, 'speedMBps': dl_speed_mbps, 'etaFormatted': '00:00'})
#         #     if is_compressed:
#         #         yield _yield_sse_event('status', {'message': 'Decompressing...'})
#         #         logging.info(f"{log_prefix} Decompressing downloaded file...")
#         #         with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
#         #             temp_final_file_path = tf.name
#         #             zf = None
#         #             try:
#         #                 zip_buffer = io.BytesIO(content_bytes)
#         #                 zf = zipfile.ZipFile(zip_buffer, 'r')
#         #                 inner_filename = _find_filename_in_zip(zf, original_filename_from_meta, prep_id)
#         #                 logging.info(f"{log_prefix} Extracting '{inner_filename}' from zip.")
#         #                 with zf.open(inner_filename, 'r') as inner_file_stream:
#         #                     yield _yield_sse_event('progress', {'percentage': 75, 'bytesProcessed': int(progress_total*0.75), 'totalBytes': progress_total})
#         #                     shutil.copyfileobj(inner_file_stream, tf) 
#         #             finally:
#         #                 if zf: zf.close()
#         #         yield _yield_sse_event('progress', {'percentage': 95, 'bytesProcessed': int(progress_total*0.95), 'totalBytes': progress_total})
#         #     else: 
#         #         yield _yield_sse_event('status', {'message': 'Saving temporary file...'})
#         #         with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
#         #             temp_final_file_path = tf.name
#         #             tf.write(content_bytes)
#         #         yield _yield_sse_event('progress', {'percentage': 95, 'bytesProcessed': int(dl_bytes_count*0.95), 'totalBytes': dl_bytes_count})

#         # else:
#         #     if is_direct_single_file_download:
#         #          raise RuntimeError("Invalid state: Cannot process split file via direct single file download request.")
#         #     logging.info(f"{log_prefix} Preparing SPLIT file download for '{original_filename_from_meta}'")
#         #     yield _yield_sse_event('status', {'message': 'Downloading & Reassembling chunks...'})

#         #     chunks_meta = file_info.get('chunks');
#         #     if not chunks_meta or not isinstance(chunks_meta, list): raise ValueError("Invalid 'chunks' metadata.")
#         #     try: chunks_meta.sort(key=lambda c: int(c.get('part_number', 0)))
#         #     except (TypeError, ValueError): raise ValueError("Invalid 'part_number' in chunks metadata.");
#         #     num_chunks = len(chunks_meta)
#         #     if num_chunks == 0: raise ValueError("Chunks list is empty in metadata.")

#         #     total_bytes_to_fetch = file_info.get('compressed_total_size', 0) 
#         #     logging.info(f"{log_prefix} Expecting {num_chunks} chunks. Total compressed size: ~{format_bytes(total_bytes_to_fetch)}.")

#         #     start_fetch_time = time.time()
#         #     fetched_bytes_count = 0
#         #     fetch_percentage_target = 80.0 
#         #     downloaded_chunk_count = 0
#         #     download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DlPrep_{prep_id[:4]}')
#         #     logging.info(f"{log_prefix} Initialized Download Executor (max={MAX_DOWNLOAD_WORKERS})")
#         #     submitted_futures: List[Future] = []
#         #     downloaded_content_map: Dict[int, bytes] = {}
#         #     first_download_error: Optional[str] = None

#             logging.info(f"{log_prefix} Submitting {num_chunks} chunk download tasks...")
#             for i, chunk_info in enumerate(chunks_meta_from_prep):
#                 part_num = chunk_info.get('part_number')
#                 if part_num is None: raise ValueError(f"Chunk metadata missing 'part_number' at index {i}")
#                 chunk_send_locations = chunk_info.get('send_locations', [])
#                 if not chunk_send_locations: raise ValueError(f"Chunk {part_num} missing 'send_locations'.")
#                 chunk_telegram_file_id, chunk_chat_id = _find_best_telegram_file_id(chunk_send_locations, PRIMARY_TELEGRAM_CHAT_ID)
#                 if not chunk_telegram_file_id: raise ValueError(f"No usable source file_id found for chunk {part_num}.")
                
#                 submitted_futures.append(download_executor.submit(_download_chunk_task, chunk_telegram_file_id, part_num, prep_id))
                
#                 # chunk_file_id, chunk_chat_id = _find_best_telegram_file_id(chunk_locations, PRIMARY_TELEGRAM_CHAT_ID)
#                 # if not chunk_file_id: raise ValueError(f"No usable source file_id found for chunk {part_num}.")
#                 # submitted_futures.append(download_executor.submit(_download_chunk_task, chunk_file_id, part_num, prep_id))

#             logging.info(f"{log_prefix} All {len(submitted_futures)} chunk download tasks submitted.")
#             yield _yield_sse_event('status', {'message': f'Downloading {num_chunks} file parts...'})

#             # Process completed futures
#             progress_stats = {}
#             for future in as_completed(submitted_futures):
#                 try:
#                     pnum_result, content_result, err_result = future.result()
#                     if err_result:
#                         logging.error(f"{log_prefix} Failed download chunk {pnum_result}: {err_result}")
#                         if not first_download_error: first_download_error = f"Chunk {pnum_result}: {err_result}"
#                     elif content_result:
#                         downloaded_chunk_count += 1
#                         chunk_length = len(content_result)
#                         fetched_bytes_count += chunk_length
#                         downloaded_content_map[pnum_result] = content_result
#                         logging.debug(f"{log_prefix} Downloaded chunk {pnum_result}. Count:{downloaded_chunk_count}/{num_chunks} ({chunk_length}b)")
#                         overall_perc = (downloaded_chunk_count / num_chunks) * fetch_percentage_target
#                         progress_stats = _calculate_download_fetch_progress(
#                             start_fetch_time, fetched_bytes_count, total_bytes_to_fetch,
#                             downloaded_chunk_count, num_chunks, overall_perc, final_expected_size
#                         )
#                         yield _yield_sse_event('progress', progress_stats)
#                     else: 
#                         logging.error(f"{log_prefix} Task for chunk {pnum_result} returned invalid state (no content, no error).")
#                         if not first_download_error: first_download_error = f"Chunk {pnum_result}: Internal task error."
#                 except Exception as e:
#                     logging.error(f"{log_prefix} Error processing download future result: {e}", exc_info=True)
#                     if not first_download_error: first_download_error = f"Error processing result: {str(e)}"

#             if first_download_error: raise ValueError(f"Download failed: {first_download_error}")
#             if downloaded_chunk_count != num_chunks: raise SystemError(f"Chunk download count mismatch. Expected:{num_chunks}, Got:{downloaded_chunk_count}.")

#             logging.info(f"{log_prefix} All {num_chunks} chunks downloaded OK. Total fetched: {fetched_bytes_count} bytes.")
#             yield _yield_sse_event('status', {'message': 'Reassembling file...'})
#             with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf_reassemble:
#                 temp_reassembled_zip_path = tf_reassemble.name
#                 logging.debug(f"{log_prefix} Reassembling into: {temp_reassembled_zip_path}")
#                 for pnum_write in range(1, num_chunks + 1):
#                     chunk_content_to_write = downloaded_content_map.get(pnum_write)
#                     if not chunk_content_to_write: raise SystemError(f"Missing content for chunk {pnum_write} during reassembly.")
#                     tf_reassemble.write(chunk_content_to_write)
#             downloaded_content_map.clear() 
#             logging.info(f"{log_prefix} Finished reassembly.")
#             yield _yield_sse_event('progress', {'percentage': fetch_percentage_target, 'bytesProcessed': fetched_bytes_count, 'totalBytes': total_bytes_to_fetch if total_bytes_to_fetch > 0 else fetched_bytes_count, 'speedMBps': progress_stats.get('speedMBps',0), 'etaFormatted':'00:00'})
#             if is_compressed: 
#                 yield _yield_sse_event('status', {'message': 'Decompressing...'})
#                 logging.info(f"{log_prefix} Decompressing reassembled file: {temp_reassembled_zip_path}")
#                 decomp_start_time = time.time()
#                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf_final:
#                    temp_final_file_path = tf_final.name # Path for the actual final file
#                    zf_reassembled = None
#                    try:
#                        zf_reassembled = zipfile.ZipFile(temp_reassembled_file_path, 'r')
#                        inner_filename_in_zip = _find_filename_in_zip(zf_reassembled, original_filename_from_meta, prep_id)
#                        logging.info(f"{log_prefix} Extracting '{inner_filename_in_zip}' from reassembled zip.")
#                        with zf_reassembled.open(inner_filename_in_zip, 'r') as inner_stream:
#                            yield _yield_sse_event('progress', {'percentage': 90}) 
#                            shutil.copyfileobj(inner_stream, tf_final)
#                    finally:
#                        if zf_reassembled: zf_reassembled.close()
#                 _safe_remove_file(temp_reassembled_file_path, prep_id, "intermediate reassembled zip")
#                 temp_reassembled_file_path = None # Nullify to avoid double cleanup
#                 logging.info(f"{log_prefix} Decompression finished. Final file at {temp_final_file_path}")
#                 # decomp_duration = time.time() - decomp_start_time
#                 # logging.info(f"{log_prefix} Decompression finished in {decomp_duration:.2f}s.")
#                 yield _yield_sse_event('progress', {'percentage': 98})
#             else: 
#                 logging.info(f"{log_prefix} Using reassembled file directly (not compressed).")
#                 temp_final_file_path = temp_reassembled_file_path
#                 temp_reassembled_zip_path = None 
#                 yield _yield_sse_event('progress', {'percentage': 98})
#         else: # File is NOT split, use existing logic for single TG file download
#             logging.info(f"{log_prefix} Preparing non-split file '{original_filename_from_meta}'.")
#             # ... (existing non-split logic using prep_data.get('telegram_file_id')) ...
#             # This part downloads content_bytes, then optionally decompresses if is_compressed
#             # Ensure tg_file_id_to_download is correctly sourced from prep_data['telegram_file_id']
#             tg_file_id_to_download = prep_data.get('telegram_file_id')
#             if not tg_file_id_to_download:
#                 # This case could happen if lookup failed to populate it for a non-split file
#                 raise ValueError("Could not determine Telegram file ID for non-split download.")

#         # --- Phase 3: Complete ---
#         if not temp_final_file_path or not os.path.exists(temp_final_file_path):
#             raise RuntimeError(f"{log_prefix} Failed to produce final temp file.")

#         final_actual_size = os.path.getsize(temp_final_file_path)
#         logging.info(f"{log_prefix} Final file ready: '{temp_final_file_path}', Size: {final_actual_size}.")
#         if final_expected_size > 0 and final_actual_size != final_expected_size:
#             logging.warning(f"{log_prefix} Final size mismatch! Expected original size:{final_expected_size}, Actual final size:{final_actual_size}")
#         prep_data['final_temp_file_path'] = temp_final_file_path
#         prep_data['final_file_size'] = final_actual_size
#         prep_data['status'] = 'ready'

#         # Final progress update
#         yield _yield_sse_event('progress', {'percentage': 100, 'bytesProcessed': final_actual_size, 'totalBytes': final_expected_size if final_expected_size else final_actual_size});
#         yield _yield_sse_event('status', {'message': 'File ready!'}); time.sleep(0.1)

#         # Send ready event
#         yield _yield_sse_event('ready', {'temp_file_id': prep_id, 'final_filename': original_filename_from_meta})
#         logging.info(f"{log_prefix} Prep complete. Sent 'ready' event.")

#     except Exception as e:
#         error_message = f"Download preparation failed: {str(e) or type(e).__name__}"
#         logging.error(f"{log_prefix} {error_message}", exc_info=True)
#         yield _yield_sse_event('error', {'message': error_message})
#         if prep_id in download_prep_data:
#             download_prep_data[prep_id]['status'] = 'error'
#             download_prep_data[prep_id]['error'] = error_message
#     finally:
#         # --- Cleanup ---
#         logging.info(f"{log_prefix} Download prep generator cleanup.")
#         if download_executor:
#             download_executor.shutdown(wait=False)
#             logging.info(f"{log_prefix} Download executor shutdown.")
#         if temp_reassembled_zip_path and temp_reassembled_zip_path != temp_final_file_path and os.path.exists(temp_reassembled_zip_path):
#             _safe_remove_file(temp_reassembled_zip_path, prep_id, "intermediate reassembled")
#         logging.info(f"{log_prefix} Download prep generator task ended.")

# def _prepare_download_and_generate_updates(prep_id: str) -> Generator[SseEvent, None, None]:
#     log_prefix = f"[DLPrep-{prep_id}]"
#     logging.info(f"{log_prefix} Download prep generator started.")
#     prep_data = download_prep_data.get(prep_id)

#     if not prep_data: # Should already be caught by stream_download route if prep_id is invalid
#         logging.error(f"{log_prefix} Critical: Prep data missing at generator start.")
#         yield _yield_sse_event('error', {'message': 'Internal Server Error: Prep data lost.'})
#         return

#     logging.debug(f"{log_prefix} Initial prep_data received by generator: {json.dumps(prep_data, indent=2, default=str)}")


#     if prep_data.get('status') != 'initiated':
#         logging.warning(f"{log_prefix} Prep already running/finished (Status: {prep_data.get('status')}). Aborting.")
#         # Optionally, yield an error or status if this happens.
#         return

#     prep_data['status'] = 'preparing'

#     # --- Initialize definitive variables for file processing ---
#     # These will be populated either from prep_data directly or from a DB lookup
#     is_split_final: bool = prep_data.get('is_split', False)
#     chunks_meta_final: Optional[List[Dict[str, Any]]] = prep_data.get('chunks_meta')
#     telegram_file_id_final: Optional[str] = prep_data.get('telegram_file_id')
#     original_filename_final: str = prep_data.get('original_filename', prep_data.get('requested_filename', "download"))
#     is_compressed_final: bool = prep_data.get('is_compressed', False)
#     final_expected_size_final: int = prep_data.get('final_expected_size', 0)
#     compressed_total_size_final: int = prep_data.get('compressed_total_size', 0)

#     # TEMP file paths
#     temp_reassembled_file_path: Optional[str] = None # Changed from temp_reassembled_zip_path for clarity
#     temp_final_file_path: Optional[str] = None
#     download_executor: Optional[ThreadPoolExecutor] = None


#     try:
#         # --- Phase 1: Determine if DB lookup is needed and populate final variables ---
#         is_item_from_batch = prep_data.get("is_item_from_batch", False)
        
#         # If it's an item from a batch, all necessary info should already be in prep_data.
#         # If not an item from batch (e.g. /stream-download/<single_file_access_id>),
#         # or if essential info is missing, then lookup.
#         needs_db_lookup = not is_item_from_batch

#         if is_item_from_batch:
#             logging.info(f"{log_prefix} Processing as item from batch. is_split: {is_split_final}, has_chunks_meta: {bool(chunks_meta_final)}, has_tg_id: {bool(telegram_file_id_final)}")
#             # Basic validation for item_from_batch
#             if is_split_final and not chunks_meta_final:
#                 raise RuntimeError(f"{log_prefix} Batch item marked split but no chunk data in prep_data.")
#             if not is_split_final and not telegram_file_id_final:
#                  raise RuntimeError(f"{log_prefix} Batch item marked non-split but no telegram_file_id in prep_data.")
        
#         elif direct_telegram_file_id_from_prep_data := prep_data.get('telegram_file_id'): # Python 3.8+ walrus operator
#             # This handles calls like /prepare-download/<user>/<filename> for a *non-split, non-batched* file
#             # where the initial prep_data might only have username/filename and then telegram_file_id gets populated.
#             # OR if is_item_from_batch was false, but telegram_file_id was somehow set in prep_data.
#             logging.info(f"{log_prefix} Using direct telegram_file_id from prep_data: {direct_telegram_file_id_from_prep_data}")
#             is_split_final = False
#             chunks_meta_final = None
#             telegram_file_id_final = direct_telegram_file_id_from_prep_data
#             needs_db_lookup = False # We have the file_id, no further lookup for *this specific info*
        
#         # If still needs_db_lookup (e.g., /stream-download/<access_id_of_main_record>)
#         if needs_db_lookup:
#             logging.info(f"{log_prefix} Insufficient info in prep_data or not a pre-defined batch item. Looking up metadata from DB.")
#             yield _yield_sse_event('status', {'message': 'Looking up file info...'}); time.sleep(0.1)
            
#             db_access_id = prep_data.get('access_id') # This would be the main record's access_id
#             db_username = prep_data.get('username')
#             db_requested_filename = prep_data.get('requested_filename')
            
#             fetched_file_info: Optional[Dict[str, Any]] = None
#             lookup_error_msg = ""

#             if db_access_id:
#                 fetched_file_info, lookup_error_msg = find_metadata_by_access_id(db_access_id)
#             elif db_username and db_requested_filename:
#                 all_user_files, lookup_error_msg = find_metadata_by_username(db_username)
#                 if not lookup_error_msg and all_user_files is not None:
#                     fetched_file_info = next((f for f in all_user_files if f.get('original_filename') == db_requested_filename), None)
#                     if not fetched_file_info: lookup_error_msg = f"File '{db_requested_filename}' not found for user '{db_username}'."
#             else:
#                 lookup_error_msg = "Insufficient information for DB metadata lookup."

#             if lookup_error_msg or not fetched_file_info:
#                 raise FileNotFoundError(lookup_error_msg or "File metadata not found in DB.")

#             # Populate/Overwrite _final variables from fetched_file_info
#             original_filename_final = fetched_file_info.get('original_filename', db_requested_filename or 'unknown')
#             final_expected_size_final = fetched_file_info.get('original_size', 0)
#             is_split_final = fetched_file_info.get('is_split', False)
#             is_compressed_final = fetched_file_info.get('is_compressed', False)
#             compressed_total_size_final = fetched_file_info.get('compressed_total_size', 0)

#             if is_split_final:
#                 chunks_meta_final = fetched_file_info.get('chunks')
#                 if not chunks_meta_final:
#                      raise RuntimeError(f"{log_prefix} DB record (access_id: {db_access_id}) says file is split but has no chunk data.")
#                 telegram_file_id_final = None 
#             else: # Not split according to DB record
#                 locations = fetched_file_info.get('send_locations', [])
#                 tg_id, _ = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID)
#                 if not tg_id:
#                     raise ValueError(f"No Telegram file ID in DB metadata for non-split file (access_id: {db_access_id}).")
#                 telegram_file_id_final = tg_id
#                 chunks_meta_final = None
#             logging.info(f"{log_prefix} Metadata loaded from DB: Name='{original_filename_final}', Split={is_split_final}, Comp={is_compressed_final}")
        
#         # --- UI Updates after info is definitive ---
#         yield _yield_sse_event('filename', {'filename': original_filename_final})
#         yield _yield_sse_event('totalSizeUpdate', {'totalSize': final_expected_size_final}) 
#         yield _yield_sse_event('progress', {'percentage': 0})
#         yield _yield_sse_event('status', {'message': 'Preparing file...'}); time.sleep(0.2)

#         # --- Phase 2: File Preparation using _final variables ---
#         if is_split_final:
#             if not chunks_meta_final: 
#                  raise RuntimeError(f"{log_prefix} CRITICAL: File is split but no chunk metadata available for processing (Phase 2).")
            
#             logging.info(f"{log_prefix} Preparing SPLIT file download for '{original_filename_final}' using {len(chunks_meta_final)} chunks.")
#             yield _yield_sse_event('status', {'message': 'Downloading & Reassembling chunks...'})
            
#             # Sort chunks_meta_final just in case
#             try: chunks_meta_final.sort(key=lambda c: int(c.get('part_number', 0)))
#             except (TypeError, ValueError): raise ValueError("Invalid 'part_number' in final chunks metadata for sorting.")
            
#             num_chunks = len(chunks_meta_final)
#             # total_bytes_to_fetch already uses compressed_total_size_final or estimates from chunk sizes
#             total_bytes_to_fetch = compressed_total_size_final or sum(c.get('size',0) for c in chunks_meta_final)

#             # ... (rest of the split file download logic using chunks_meta_final)
#             # Ensure all references are to chunks_meta_final
#             start_fetch_time = time.time()
#             fetched_bytes_count = 0
#             fetch_percentage_target = 80.0 
#             downloaded_chunk_count = 0
#             download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DlPrep_{prep_id[:4]}')
            
#             submitted_futures: List[Future] = []
#             downloaded_content_map: Dict[int, bytes] = {}
#             first_download_error: Optional[str] = None

#             logging.info(f"{log_prefix} Submitting {num_chunks} chunk download tasks...")
#             for i, chunk_info in enumerate(chunks_meta_final): # Use chunks_meta_final
#                 part_num = chunk_info.get('part_number')
#                 # ... (rest of existing split logic) ...
#                 chunk_send_locations = chunk_info.get('send_locations', [])
#                 if not chunk_send_locations: raise ValueError(f"Chunk {part_num} missing 'send_locations'.")
#                 chunk_telegram_file_id, chunk_chat_id = _find_best_telegram_file_id(chunk_send_locations, PRIMARY_TELEGRAM_CHAT_ID)
#                 if not chunk_telegram_file_id: raise ValueError(f"No usable source file_id found for chunk {part_num}.")
#                 submitted_futures.append(download_executor.submit(_download_chunk_task, chunk_telegram_file_id, part_num, prep_id))
#             # ... (as_completed loop and reassembly logic remains same, ensure it uses temp_reassembled_file_path) ...
#             # After reassembly:
#             # if is_compressed_final:
#             #    ... decompress from temp_reassembled_file_path to temp_final_file_path ...
#             # else:
#             #    temp_final_file_path = temp_reassembled_file_path
#             #    temp_reassembled_file_path = None
#             # This part seems correct from before.

#             logging.info(f"{log_prefix} All {num_chunks} chunk download tasks submitted.")
#             yield _yield_sse_event('status', {'message': f'Downloading {num_chunks} file parts...'})

#             # Process completed futures
#             progress_stats = {}
#             for future in as_completed(submitted_futures):
#                 try:
#                     pnum_result, content_result, err_result = future.result()
#                     if err_result:
#                         logging.error(f"{log_prefix} Failed download chunk {pnum_result}: {err_result}")
#                         if not first_download_error: first_download_error = f"Chunk {pnum_result}: {err_result}"
#                     elif content_result:
#                         downloaded_chunk_count += 1
#                         chunk_length = len(content_result)
#                         fetched_bytes_count += chunk_length
#                         downloaded_content_map[pnum_result] = content_result
#                         logging.debug(f"{log_prefix} Downloaded chunk {pnum_result}. Count:{downloaded_chunk_count}/{num_chunks} ({chunk_length}b)")
#                         overall_perc = (downloaded_chunk_count / num_chunks) * fetch_percentage_target
#                         progress_stats = _calculate_download_fetch_progress(
#                             start_fetch_time, fetched_bytes_count, total_bytes_to_fetch,
#                             downloaded_chunk_count, num_chunks, overall_perc, final_expected_size_final
#                         )
#                         yield _yield_sse_event('progress', progress_stats)
#                     else: 
#                         logging.error(f"{log_prefix} Task for chunk {pnum_result} returned invalid state (no content, no error).")
#                         if not first_download_error: first_download_error = f"Chunk {pnum_result}: Internal task error."
#                 except Exception as e:
#                     logging.error(f"{log_prefix} Error processing download future result: {e}", exc_info=True)
#                     if not first_download_error: first_download_error = f"Error processing result: {str(e)}"

#             if first_download_error: raise ValueError(f"Download failed: {first_download_error}")
#             if downloaded_chunk_count != num_chunks: raise SystemError(f"Chunk download count mismatch. Expected:{num_chunks}, Got:{downloaded_chunk_count}.")

#             logging.info(f"{log_prefix} All {num_chunks} chunks downloaded OK. Total fetched: {fetched_bytes_count} bytes.")
#             yield _yield_sse_event('status', {'message': 'Reassembling file...'})
#             with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf_reassemble:
#                 temp_reassembled_file_path = tf_reassemble.name # Corrected var name
#                 logging.debug(f"{log_prefix} Reassembling into: {temp_reassembled_file_path}")
#                 for pnum_write in range(1, num_chunks + 1):
#                     chunk_content_to_write = downloaded_content_map.get(pnum_write)
#                     if not chunk_content_to_write: raise SystemError(f"Missing content for chunk {pnum_write} during reassembly.")
#                     tf_reassemble.write(chunk_content_to_write)
#             downloaded_content_map.clear() 
#             logging.info(f"{log_prefix} Finished reassembly to {temp_reassembled_file_path}.")
#             yield _yield_sse_event('progress', {'percentage': fetch_percentage_target, 'bytesProcessed': fetched_bytes_count, 'totalBytes': total_bytes_to_fetch if total_bytes_to_fetch > 0 else fetched_bytes_count, 'speedMBps': progress_stats.get('speedMBps',0), 'etaFormatted':'00:00'})
            
#             if is_compressed_final: 
#                 yield _yield_sse_event('status', {'message': 'Decompressing...'})
#                 logging.info(f"{log_prefix} Decompressing reassembled file: {temp_reassembled_file_path}")
#                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf_final:
#                    temp_final_file_path = tf_final.name 
#                    zf_reassembled = None
#                    try:
#                        zf_reassembled = zipfile.ZipFile(temp_reassembled_file_path, 'r')
#                        inner_filename_in_zip = _find_filename_in_zip(zf_reassembled, original_filename_final, prep_id)
#                        logging.info(f"{log_prefix} Extracting '{inner_filename_in_zip}' from reassembled zip.")
#                        with zf_reassembled.open(inner_filename_in_zip, 'r') as inner_stream:
#                            yield _yield_sse_event('progress', {'percentage': 90}) 
#                            shutil.copyfileobj(inner_stream, tf_final)
#                    finally:
#                        if zf_reassembled: zf_reassembled.close()
#                 _safe_remove_file(temp_reassembled_file_path, prep_id, "intermediate reassembled file (was zip)") # Corrected description
#                 temp_reassembled_file_path = None 
#                 logging.info(f"{log_prefix} Decompression finished. Final file at {temp_final_file_path}")
#                 yield _yield_sse_event('progress', {'percentage': 98})
#             else: 
#                 logging.info(f"{log_prefix} Using reassembled file directly (not compressed).")
#                 temp_final_file_path = temp_reassembled_file_path
#                 temp_reassembled_file_path = None 
#                 yield _yield_sse_event('progress', {'percentage': 98})

#         else: # File is NOT split (is_split_final is False)
#             logging.info(f"{log_prefix} Preparing non-split file '{original_filename_final}'.")
#             if not telegram_file_id_final:
#                 raise ValueError("Could not determine Telegram file ID for non-split download (Phase 2).")
            
#             yield _yield_sse_event('status', {'message': 'Downloading...'})
#             start_dl = time.time()
#             content_bytes, err_msg = download_telegram_file_content(telegram_file_id_final)
#             # ... (rest of existing non-split logic: calculate speed, check errors, handle is_compressed_final)
#             dl_duration = time.time() - start_dl
#             dl_bytes_count = len(content_bytes) if content_bytes else 0
#             dl_speed_mbps = (dl_bytes_count / (1024*1024) / dl_duration) if dl_duration > 0 else 0
#             logging.info(f"{log_prefix} TG download ({dl_bytes_count} bytes) in {dl_duration:.2f}s. Speed: {dl_speed_mbps:.2f} MB/s")

#             if err_msg: raise ValueError(f"TG download failed: {err_msg}")
#             if not content_bytes: raise ValueError("TG download returned empty content.")
            
#             progress_total_non_split = final_expected_size_final if final_expected_size_final > 0 else dl_bytes_count
#             yield _yield_sse_event('progress', {'percentage': 50, 'bytesProcessed': int(progress_total_non_split*0.5), 'totalBytes': progress_total_non_split, 'speedMBps': dl_speed_mbps, 'etaFormatted': '00:00'})

#             if is_compressed_final:
#                 yield _yield_sse_event('status', {'message': 'Decompressing...'})
#                 logging.info(f"{log_prefix} Decompressing downloaded file...")
#                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
#                     temp_final_file_path = tf.name
#                     zf = None
#                     try:
#                         zip_buffer = io.BytesIO(content_bytes)
#                         zf = zipfile.ZipFile(zip_buffer, 'r')
#                         inner_filename = _find_filename_in_zip(zf, original_filename_final, prep_id)
#                         logging.info(f"{log_prefix} Extracting '{inner_filename}' from zip.")
#                         with zf.open(inner_filename, 'r') as inner_file_stream:
#                             yield _yield_sse_event('progress', {'percentage': 75})
#                             shutil.copyfileobj(inner_file_stream, tf) 
#                     finally:
#                         if zf: zf.close()
#                 yield _yield_sse_event('progress', {'percentage': 95})
#             else: 
#                 yield _yield_sse_event('status', {'message': 'Saving temporary file...'})
#                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
#                     temp_final_file_path = tf.name
#                     tf.write(content_bytes)
#                 yield _yield_sse_event('progress', {'percentage': 95})


#         # --- Phase 3: Complete ---
#         if not temp_final_file_path or not os.path.exists(temp_final_file_path):
#             raise RuntimeError(f"{log_prefix} Failed to produce final temp file path.")

#         final_actual_size = os.path.getsize(temp_final_file_path)
#         logging.info(f"{log_prefix} Final file ready: '{temp_final_file_path}', Size: {final_actual_size}.")
#         if final_expected_size_final > 0 and final_actual_size != final_expected_size_final:
#             logging.warning(f"{log_prefix} Final size mismatch! Expected:{final_expected_size_final}, Actual:{final_actual_size}")
        
#         prep_data['final_temp_file_path'] = temp_final_file_path
#         prep_data['final_file_size'] = final_actual_size
#         prep_data['status'] = 'ready'

#         yield _yield_sse_event('progress', {'percentage': 100, 'bytesProcessed': final_actual_size, 'totalBytes': final_expected_size_final if final_expected_size_final else final_actual_size});
#         yield _yield_sse_event('status', {'message': 'File ready!'}); time.sleep(0.1)
#         yield _yield_sse_event('ready', {'temp_file_id': prep_id, 'final_filename': original_filename_final})
#         logging.info(f"{log_prefix} Prep complete. Sent 'ready' event.")

#     except Exception as e:
#         error_message = f"Download preparation failed: {str(e) or type(e).__name__}"
#         logging.error(f"{log_prefix} {error_message}", exc_info=True)
#         yield _yield_sse_event('error', {'message': error_message})
#         if prep_id in download_prep_data:
#             download_prep_data[prep_id]['status'] = 'error'
#             download_prep_data[prep_id]['error'] = error_message
#     finally:
#         logging.info(f"{log_prefix} Download prep generator cleanup.")
#         if download_executor:
#             download_executor.shutdown(wait=False) # wait=False is fine here
#             logging.info(f"{log_prefix} Download executor shutdown.")
#         # temp_reassembled_zip_path might be temp_reassembled_file_path if not compressed
#         if temp_reassembled_file_path and temp_reassembled_file_path != temp_final_file_path and os.path.exists(temp_reassembled_file_path):
#             _safe_remove_file(temp_reassembled_file_path, prep_id, "intermediate reassembled file")
#         # temp_final_file_path is cleaned up by serve_temp_file or _schedule_cleanup
#         logging.info(f"{log_prefix} Download prep generator task ended. Status: {prep_data.get('status', 'unknown')}")

def _prepare_download_and_generate_updates(prep_id: str) -> Generator[SseEvent, None, None]:
    log_prefix = f"[DLPrep-{prep_id}]"
    logging.info(f"{log_prefix} Download prep generator started.")
    prep_data = download_prep_data.get(prep_id)

    if not prep_data:
        logging.error(f"{log_prefix} Critical: Prep data missing at generator start.")
        yield _yield_sse_event('error', {'message': 'Internal Server Error: Prep data lost.'})
        return

    logging.debug(f"{log_prefix} Initial prep_data received by generator: {json.dumps(prep_data, indent=2, default=str)}")

    if prep_data.get('status') != 'initiated':
        logging.warning(f"{log_prefix} Prep already running/finished (Status: {prep_data.get('status')}). Aborting.")
        return

    prep_data['status'] = 'preparing'

    is_split_final: bool = prep_data.get('is_split', False)
    chunks_meta_final: Optional[List[Dict[str, Any]]] = prep_data.get('chunks_meta')
    telegram_file_id_final: Optional[str] = prep_data.get('telegram_file_id')
    original_filename_final: str = prep_data.get('original_filename', prep_data.get('requested_filename', "download"))
    is_compressed_final: bool = original_filename_final.lower().endswith('.zip')
    final_expected_size_final: int = prep_data.get('final_expected_size', 0)
    compressed_total_size_final: int = prep_data.get('compressed_total_size', 0)

    temp_reassembled_file_path: Optional[str] = None
    temp_final_file_path: Optional[str] = None
    download_executor: Optional[ThreadPoolExecutor] = None

    try:
        is_item_from_batch = prep_data.get("is_item_from_batch", False)
        needs_db_lookup = not is_item_from_batch

        if is_item_from_batch:
            logging.info(f"{log_prefix} Processing as item from batch. is_split: {is_split_final}, has_chunks_meta: {bool(chunks_meta_final)}, has_tg_id: {bool(telegram_file_id_final)}")
            if is_split_final and not chunks_meta_final:
                raise RuntimeError(f"{log_prefix} Batch item marked split but no chunk data in prep_data.")
            if not is_split_final and not telegram_file_id_final:
                 raise RuntimeError(f"{log_prefix} Batch item marked non-split but no telegram_file_id in prep_data.")
        
        elif direct_telegram_file_id_from_prep_data := prep_data.get('telegram_file_id'):
            logging.info(f"{log_prefix} Using direct telegram_file_id from prep_data: {direct_telegram_file_id_from_prep_data}")
            is_split_final = False; chunks_meta_final = None; telegram_file_id_final = direct_telegram_file_id_from_prep_data
            is_compressed_final = original_filename_final.lower().endswith('.zip')
            needs_db_lookup = False
        
        if needs_db_lookup:

            logging.info(f"{log_prefix} Insufficient info in prep_data or not a pre-defined batch item. Looking up metadata from DB.")
            yield _yield_sse_event('status', {'message': 'Looking up file info...'}); time.sleep(0.1)
            
            db_access_id = prep_data.get('access_id') 
            db_username = prep_data.get('username')
            db_requested_filename = prep_data.get('requested_filename')
            
            fetched_file_info: Optional[Dict[str, Any]] = None
            lookup_error_msg = ""

            if db_access_id:
                fetched_file_info, lookup_error_msg = find_metadata_by_access_id(db_access_id)
            elif db_username and db_requested_filename:
                all_user_files, lookup_error_msg = find_metadata_by_username(db_username)
                if not lookup_error_msg and all_user_files is not None:
                    fetched_file_info = next((f for f in all_user_files if f.get('original_filename') == db_requested_filename), None)
                    if not fetched_file_info: lookup_error_msg = f"File '{db_requested_filename}' not found for user '{db_username}'."
            else:
                lookup_error_msg = "Insufficient information for DB metadata lookup."

            if lookup_error_msg or not fetched_file_info:
                raise FileNotFoundError(lookup_error_msg or "File metadata not found in DB.")

            original_filename_final = fetched_file_info.get('original_filename', db_requested_filename or 'unknown')
            final_expected_size_final = fetched_file_info.get('original_size', 0)
            is_split_final = fetched_file_info.get('is_split', False)
            is_compressed_final = fetched_file_info.get('is_compressed', False)
            compressed_total_size_final = fetched_file_info.get('compressed_total_size', 0)

            if is_split_final:
                chunks_meta_final = fetched_file_info.get('chunks')
                if not chunks_meta_final:
                     raise RuntimeError(f"{log_prefix} DB record (access_id: {db_access_id}) says file is split but has no chunk data.")
                telegram_file_id_final = None 
            else: 
                locations = fetched_file_info.get('send_locations', [])
                tg_id, _ = _find_best_telegram_file_id(locations, PRIMARY_TELEGRAM_CHAT_ID)
                if not tg_id:
                    raise ValueError(f"No Telegram file ID in DB metadata for non-split file (access_id: {db_access_id}).")
                telegram_file_id_final = tg_id
                chunks_meta_final = None
            logging.info(f"{log_prefix} Metadata loaded from DB: Name='{original_filename_final}', Split={is_split_final}, Comp={is_compressed_final}")

        yield _yield_sse_event('filename', {'filename': original_filename_final})
        yield _yield_sse_event('totalSizeUpdate', {'totalSize': final_expected_size_final}) 
        yield _yield_sse_event('progress', {'percentage': 0})
        yield _yield_sse_event('status', {'message': 'Preparing file...'}); time.sleep(0.2)

        if is_split_final:
            if not chunks_meta_final: 
                 raise RuntimeError(f"{log_prefix} CRITICAL: File is split but no chunk metadata (chunks_meta_final).")
            
            logging.info(f"{log_prefix} Preparing SPLIT file download for '{original_filename_final}' using {len(chunks_meta_final)} chunks.")
            yield _yield_sse_event('status', {'message': 'Downloading & Reassembling chunks...'})
            
            try: chunks_meta_final.sort(key=lambda c: int(c.get('part_number', 0)))
            except (TypeError, ValueError): raise ValueError("Invalid 'part_number' in final chunks metadata for sorting.")
            
            num_chunks = len(chunks_meta_final)
            total_bytes_to_fetch = compressed_total_size_final or sum(c.get('size',0) for c in chunks_meta_final)
            
            start_fetch_time = time.time(); fetched_bytes_count = 0
            fetch_percentage_target = 80.0; downloaded_chunk_count = 0
            download_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DlPrep_{prep_id[:4]}')
            
            submitted_futures: List[Future] = []
            downloaded_content_map: Dict[int, bytes] = {}
            first_download_error: Optional[str] = None
            file_too_big_errors_count = 0 # Counter for "file is too big" errors

            logging.info(f"{log_prefix} Submitting {num_chunks} chunk download tasks...")
            # for i, chunk_info in enumerate(chunks_meta_final):
            #     part_num = chunk_info.get('part_number')
            #     chunk_send_locations = chunk_info.get('send_locations', [])
            #     if not chunk_send_locations: raise ValueError(f"Chunk {part_num} missing 'send_locations'.")
            #     chunk_telegram_file_id, _ = _find_best_telegram_file_id(chunk_send_locations, PRIMARY_TELEGRAM_CHAT_ID)
            #     if not chunk_telegram_file_id: raise ValueError(f"No usable source file_id for chunk {part_num}.")
            #     submitted_futures.append(download_executor.submit(_download_chunk_task, chunk_telegram_file_id, part_num, prep_id))
            for i, chunk_info in enumerate(chunks_meta_final):
                part_num = chunk_info.get("part_number")
                chunk_send_locations = chunk_info.get("send_locations", [])

                # ── NEW: don’t abort the entire download if a single chunk is bad ──
                if not chunk_send_locations:
                    logging.warning(f"{log_prefix} Chunk {part_num} has no send_locations – skipping.")
                    continue

                chunk_telegram_file_id, _ = _find_best_telegram_file_id(
                chunk_send_locations, PRIMARY_TELEGRAM_CHAT_ID
                )
                if not chunk_telegram_file_id:
                    logging.warning(f"{log_prefix} Chunk {part_num} has no Telegram file_id – skipping.")
                    continue

                submitted_futures.append(
                download_executor.submit(
                _download_chunk_task, chunk_telegram_file_id, part_num, prep_id
                )
                )
            
            yield _yield_sse_event('status', {'message': f'Downloading {num_chunks} file parts...'})
            progress_stats = {}
            first_download_error: Optional[str] = None
            file_too_big_errors_count = 0
            for future in as_completed(submitted_futures):
                try:
                    pnum_result, content_result, err_result = future.result()
                    logging.debug(f"{log_prefix} Future completed for chunk {pnum_result}. Error: {err_result is not None}")
                    if err_result:
                        logging.error(f"{log_prefix} Failed download chunk {pnum_result}: {err_result}")
                        if "file is too big" in err_result.lower(): file_too_big_errors_count += 1
                        if not first_download_error:
                            first_download_error = f"Chunk {pnum_result}: {err_result}"
                            logging.info(f"{log_prefix} Stored first download error: {first_download_error}")
                    elif content_result:
                        downloaded_chunk_count += 1; chunk_length = len(content_result)
                        fetched_bytes_count += chunk_length; downloaded_content_map[pnum_result] = content_result
                        logging.debug(f"{log_prefix} Downloaded chunk {pnum_result}. Count:{downloaded_chunk_count}/{num_chunks} ({format_bytes(chunk_length)})")
                        overall_perc = (downloaded_chunk_count / num_chunks) * fetch_percentage_target
                        progress_stats = _calculate_download_fetch_progress(
                            start_fetch_time, fetched_bytes_count, total_bytes_to_fetch,
                            downloaded_chunk_count, num_chunks, overall_perc, final_expected_size_final
                        )
                        yield _yield_sse_event('progress', progress_stats)
                    else: 
                        logging.error(f"{log_prefix} Task for chunk {pnum_result} returned invalid state.");
                        if not first_download_error: first_download_error = f"Chunk {pnum_result}: Internal task error."
                except Exception as e:
                    logging.error(f"{log_prefix} Error processing download future for a chunk: {e}", exc_info=True)
                    if not first_download_error: first_download_error = f"Processing future for chunk: {str(e)}"

            if first_download_error:
                # Prioritize the "file too big" message if it occurred
                if file_too_big_errors_count > 0:
                     error_to_raise = "Download failed: One or more file parts were too large for Telegram to serve."
                     logging.error(f"{log_prefix} Raising error due to 'file too big' count: {file_too_big_errors_count}. First specific error recorded was: {first_download_error}")
                     raise ValueError(error_to_raise)
                else:
                     # Raise the first specific error encountered otherwise
                     logging.error(f"{log_prefix} Raising first encountered download error: {first_download_error}")
                     raise ValueError(f"Download failed: {first_download_error}")

            if downloaded_chunk_count != num_chunks:
                 # This check might be redundant if error handling above is robust, but keep as safety net
                 logging.error(f"{log_prefix} Chunk download count mismatch! Expected:{num_chunks}, Got:{downloaded_chunk_count}. Raising SystemError.")
                 raise SystemError(f"Chunk download count mismatch. Expected:{num_chunks}, Got:{downloaded_chunk_count}.")
            
            # Create the temporary file for reassembly
            # temp_reassembled_file_path MUST be defined before the 'if is_compressed_final' block for split files
            with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_reass_{prep_id}_") as tf_reassemble:
                temp_reassembled_file_path = tf_reassemble.name
                logging.debug(f"{log_prefix} Reassembling into: {temp_reassembled_file_path}")
                for pnum_write in range(1, num_chunks + 1):
                    chunk_content_to_write = downloaded_content_map.get(pnum_write)
                    if not chunk_content_to_write: raise SystemError(f"Missing content for chunk {pnum_write} during reassembly.")
                    tf_reassemble.write(chunk_content_to_write)
            downloaded_content_map.clear()
            logging.info(f"{log_prefix} Finished reassembly to {temp_reassembled_file_path}.")
            yield _yield_sse_event('progress', {'percentage': fetch_percentage_target, 'bytesProcessed': fetched_bytes_count, 'totalBytes': total_bytes_to_fetch if total_bytes_to_fetch > 0 else fetched_bytes_count, 'speedMBps': progress_stats.get('speedMBps',0), 'etaFormatted':'00:00'})
            
            # For split files, the reassembled file IS the final file.
            # The 'is_compressed_final' flag (derived from original filename) tells us the NATURE of this final file.
            temp_final_file_path = temp_reassembled_file_path
            temp_reassembled_file_path = None # Mark as "moved" to final path conceptually

            if is_compressed_final: # i.e., original_filename_final ends with .zip
                 yield _yield_sse_event('status', {'message': 'File ready (was split, reassembled ZIP).'})
            else:
                 yield _yield_sse_event('status', {'message': 'File ready (was split, reassembled).'})
            yield _yield_sse_event('progress', {'percentage': 98})



        else: # File is NOT split (is_split_final is False)
            logging.info(f"{log_prefix} Preparing non-split file '{original_filename_final}'.")
            if not telegram_file_id_final:
                raise ValueError("Could not determine Telegram file ID for non-split download (Phase 2).")
            
            yield _yield_sse_event('status', {'message': 'Downloading...'})
            start_dl = time.time()
            content_bytes, err_msg = download_telegram_file_content(telegram_file_id_final)
            dl_duration = time.time() - start_dl
            dl_bytes_count = len(content_bytes) if content_bytes else 0
            dl_speed_mbps = (dl_bytes_count / (1024*1024) / dl_duration) if dl_duration > 0 else 0
            logging.info(f"{log_prefix} TG download ({format_bytes(dl_bytes_count)}) in {dl_duration:.2f}s. Speed: {dl_speed_mbps:.2f} MB/s")

            if err_msg: raise ValueError(f"TG download failed: {err_msg}")
            if not content_bytes: raise ValueError("TG download returned empty content.")
            
            progress_total_non_split = final_expected_size_final if final_expected_size_final > 0 else dl_bytes_count
            yield _yield_sse_event('progress', {'percentage': 50, 'bytesProcessed': int(progress_total_non_split*0.5), 'totalBytes': progress_total_non_split, 'speedMBps': dl_speed_mbps, 'etaFormatted': '00:00'})

            if is_compressed_final and not original_filename_final.lower().endswith('.zip'):
                yield _yield_sse_event('status', {'message': 'Decompressing...'})
                logging.info(f"{log_prefix} Decompressing downloaded single file: {original_filename_final}")
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                    temp_final_file_path = tf.name
                    zf_single_download = None
                    try:
                        zip_buffer = io.BytesIO(content_bytes)
                        zf_single_download = zipfile.ZipFile(zip_buffer, 'r')
                        inner_filename = _find_filename_in_zip(zf_single_download, original_filename_final, prep_id)
                        logging.info(f"{log_prefix} Extracting '{inner_filename}' from downloaded single zip.")
                        with zf_single_download.open(inner_filename, 'r') as inner_file_stream:
                            shutil.copyfileobj(inner_file_stream, tf)  
                    finally:
                        if zf_single_download: zf_single_download.close()
                yield _yield_sse_event('progress', {'percentage': 95})
            else: 
                yield _yield_sse_event('status', {'message': 'Saving temporary file...'})
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_final_{prep_id}_") as tf:
                    temp_final_file_path = tf.name
                    tf.write(content_bytes)
                yield _yield_sse_event('progress', {'percentage': 95})

        # --- Phase 3: Complete ---
        # ... (Phase 3 logic from previous answer - this part should be correct using temp_final_file_path and original_filename_final)
        if not temp_final_file_path or not os.path.exists(temp_final_file_path):
            logging.error(f"{log_prefix} Critical: temp_final_file_path is not set or file does not exist before 'ready' event. Path: {temp_final_file_path}")
            raise RuntimeError(f"{log_prefix} Failed to produce final temp file path.")

        final_actual_size = os.path.getsize(temp_final_file_path)
        logging.info(f"{log_prefix} Final file ready: '{temp_final_file_path}', Size: {final_actual_size}.")
        if final_expected_size_final > 0 and final_actual_size != final_expected_size_final:
            logging.warning(f"{log_prefix} Final size mismatch! Expected:{final_expected_size_final}, Actual:{final_actual_size}")
        
        prep_data['final_temp_file_path'] = temp_final_file_path
        prep_data['final_file_size'] = final_actual_size
        prep_data['status'] = 'ready'

        yield _yield_sse_event('progress', {'percentage': 100, 'bytesProcessed': final_actual_size, 'totalBytes': final_expected_size_final if final_expected_size_final else final_actual_size});
        yield _yield_sse_event('status', {'message': 'File ready!'}); time.sleep(0.1)
        yield _yield_sse_event('ready', {'temp_file_id': prep_id, 'final_filename': original_filename_final})
        logging.info(f"{log_prefix} Prep complete. Sent 'ready' event.")

    except Exception as e:
        error_message = f"Download preparation failed: {str(e) or type(e).__name__}"
        logging.error(f"{log_prefix} {error_message}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_message})
        if prep_id in download_prep_data:
            download_prep_data[prep_id]['status'] = 'error'
            download_prep_data[prep_id]['error'] = error_message
    finally:
        logging.info(f"{log_prefix} Download prep generator cleanup.")
        if download_executor:
            download_executor.shutdown(wait=False) 
            logging.info(f"{log_prefix} Download executor shutdown.")
        # if temp_reassembled_file_path and temp_reassembled_file_path != temp_final_file_path and os.path.exists(temp_reassembled_file_path):
        #     _safe_remove_file(temp_reassembled_file_path, prep_id, "intermediate reassembled file")
        # logging.info(f"{log_prefix} Download prep generator task ended. Status: {prep_data.get('status', 'unknown')}")
        if temp_reassembled_file_path and os.path.exists(temp_reassembled_file_path):
            _safe_remove_file(temp_reassembled_file_path, prep_id, "intermediate reassembled file (if not used as final)")
        # temp_final_file_path is cleaned up by /serve-temp-file or _schedule_cleanup after 'ready' or error.
        logging.info(f"{log_prefix} Download prep generator task ended. Status: {prep_data.get('status', 'unknown')}")

@app.route('/initiate-download-all/<access_id>')
def initiate_download_all(access_id: str):
    prep_id_for_zip = str(uuid.uuid4()) 
    log_prefix = f"[DLAll-Init-{prep_id_for_zip}]"
    logging.info(f"{log_prefix} Request to initiate 'Download All' for batch_access_id: {access_id}")

    batch_info, error_msg = find_metadata_by_access_id(access_id)

    if error_msg or not batch_info or not batch_info.get('is_batch'):
        error_message_for_user = error_msg if error_msg else f"Batch '{access_id}' not found or invalid."
        logging.warning(f"{log_prefix} Batch metadata lookup failed: {error_message_for_user}")
        status_code = 404 if "not found" in error_message_for_user.lower() else 400
        return jsonify({"error": error_message_for_user, "prep_id": None}), status_code
    
    # files_to_zip_meta = []
    # total_expected_zip_content_size = 0
    # for file_item in batch_info.get('files_in_batch', []):
    #     if not file_item.get('skipped') and not file_item.get('failed'):
    #         original_filename = file_item.get('original_filename')
    #         original_size = file_item.get('original_size', 0)
    #         send_locations = file_item.get('send_locations', [])
    #         tg_file_id, _ = _find_best_telegram_file_id(send_locations, PRIMARY_TELEGRAM_CHAT_ID)
    #         if original_filename and tg_file_id:
    #             files_to_zip_meta.append({
    #                 "original_filename": original_filename,
    #                 "telegram_file_id": tg_file_id,
    #                 "original_size": original_size
    #             })
    #             total_expected_zip_content_size += original_size
    #         else:
    #             logging.warning(f"{log_prefix} Skipping file '{original_filename or 'Unknown'}' due to missing name or TG file ID for zipping.")
    
    files_to_zip_meta: list[dict] = []
    total_expected_zip_content_size: int = 0

    for file_item in batch_info.get("files_in_batch", []):
# skip files the uploader marked as failed / skipped
        if file_item.get("skipped") or file_item.get("failed"):
            continue

        original_filename = file_item.get("original_filename")
        original_size = file_item.get("original_size", 0)

# ── NEW: handle normal upload vs. chunked (is_split) upload ───────────────
        tg_file_id = None

        if file_item.get("is_split"):
        # file was split into 18 MiB chunks; grab the TG file-id from the 1st chunk
            chunks = file_item.get("chunks", [])
            if chunks:
                first_chunk_locations = chunks[0].get("send_locations", [])
            if first_chunk_locations:
                tg_file_id = first_chunk_locations[0].get("file_id")
        else:
        # regular ≤ 18 MiB upload – existing logic
            send_locations = file_item.get("send_locations", [])
            tg_file_id, _ = _find_best_telegram_file_id(
            send_locations, PRIMARY_TELEGRAM_CHAT_ID
        )

        # ── add to the zip list if we found a TG file-id ──────────────────────────
        if original_filename and tg_file_id:
            meta_entry = {
            "original_filename": original_filename,
            "telegram_file_id" : tg_file_id,
            "original_size" : original_size,
            "is_split" : file_item.get("is_split", False),
            }
            # keep full chunk metadata so the zip-generator can re-assemble later
            if file_item.get("is_split"):
                meta_entry["chunks_meta"] = file_item.get("chunks", [])

                files_to_zip_meta.append(meta_entry)
                total_expected_zip_content_size += original_size
        else:
            logging.warning(
            f"{log_prefix} Skipping file '{original_filename or 'Unknown'}' "
            f"due to missing name or TG file ID for zipping."
            )
    
    if not files_to_zip_meta:
        logging.warning(f"{log_prefix} No valid files found in batch '{access_id}' to include in zip.")
        return jsonify({"error": "No files available to include in the 'Download All' zip.", "prep_id": None}), 404

    batch_display_name_for_zip = batch_info.get('batch_display_name', f"download_all_{access_id}.zip")
    if not batch_display_name_for_zip.lower().endswith(".zip"):
        batch_display_name_for_zip += ".zip"


    download_prep_data[prep_id_for_zip] = {
        "prep_id": prep_id_for_zip,
        "status": "initiated_zip_all", 
        "access_id_original_batch": access_id, 
        "username": batch_info.get('username'),
        "batch_display_name": batch_display_name_for_zip, 
        "original_filename": batch_display_name_for_zip, 
        "files_to_zip_meta": files_to_zip_meta, 
        "total_expected_content_size": total_expected_zip_content_size, 
        "error": None,
        "final_temp_file_path": None, 
        "final_file_size": 0,        
        "start_time": time.time()
    }
    logging.info(f"{log_prefix} Stored prep data for 'Download All'. {len(files_to_zip_meta)} files to zip. Expected content size: {total_expected_zip_content_size} bytes. Zip name: {batch_display_name_for_zip}")

    return jsonify({
        "message": "Download All initiated. Connect to SSE stream for progress.",
        "prep_id_for_zip": prep_id_for_zip, 
        "sse_stream_url": url_for('stream_download_all', prep_id_for_zip=prep_id_for_zip, _external=False) 
    }), 200

def _generate_zip_and_stream_progress(prep_id_for_zip: str) -> Generator[SseEvent, None, None]:
    log_prefix = f"[DLAll-ZipGen-{prep_id_for_zip}]"
    logging.info(f"{log_prefix} Starting 'Download All' zipping generator.")
    
    prep_data = download_prep_data.get(prep_id_for_zip)
    if not prep_data: 
        logging.error(f"{log_prefix} Critical error: Prep data missing.")
        yield _yield_sse_event('error', {'message': 'Internal server error: Preparation data lost.'})
        return

    prep_data['status'] = 'zipping_all_fetching' 

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
        yield _yield_sse_event('start', { 
            'filename': batch_display_name_for_zip,
            'totalSize': total_expected_content_size 
        })
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False, dir=UPLOADS_TEMP_DIR, prefix=f"dl_all_zip_{prep_id_for_zip}_") as tf_zip:
            temp_zip_file_path = tf_zip.name
        logging.info(f"{log_prefix} Created temporary zip file: {temp_zip_file_path}")

        bytes_downloaded_and_zipped = 0
        overall_zip_gen_start_time = time.time()
        
        downloaded_file_contents: Dict[str, bytes] = {} 
        files_processed_count = 0
        
        if files_to_process_meta: 
            download_all_executor = ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS, thread_name_prefix=f'DLAllZip_{prep_id_for_zip[:4]}')

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
        prep_data['final_temp_file_path'] = temp_zip_file_path 
        prep_data['final_file_size'] = final_zip_actual_size
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
        
@app.route('/stream-download-all/<prep_id_for_zip>')
def stream_download_all(prep_id_for_zip: str):
    """
    SSE endpoint that streams the progress of fetching multiple files from Telegram
    and zipping them for a "Download All" operation.
    """
    log_prefix = f"[DLAll-Stream-{prep_id_for_zip}]"
    logging.info(f"{log_prefix} SSE connection established for 'Download All'.")
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
        logging.warning(f"Serve fail: Prep data not found for ID '{temp_id}'.")
        return make_response(f"Error: Invalid or expired link (ID: {temp_id}).", 404)

    # Check status more carefully
    current_status = prep_info.get('status')
    if current_status != 'ready':
        err = prep_info.get('error', f'File not ready (Status: {current_status})')
        logging.error(f"Serve fail: '{temp_id}' status is '{current_status}'. Err:{err}")
        return make_response(f"Error: {err}", 400 if current_status == 'error' else 409) 

    temp_path = prep_info.get('final_temp_file_path')
    size = prep_info.get('final_file_size')
    dl_name = prep_info.get('original_filename', filename)

    if not temp_path or not os.path.exists(temp_path):
        logging.error(f"Serve fail: Prepared file missing ID '{temp_id}'. Expected Path:{temp_path}")
        prep_info['status'] = 'error'
        prep_info['error'] = 'File missing after preparation'
        cleanup_delay_seconds = 5 
        timer = threading.Timer(cleanup_delay_seconds, _schedule_cleanup, args=[temp_id, temp_path])
        timer.daemon = True 
        timer.start()
        logging.info(f"[{temp_id}] Scheduled immediate cleanup due to missing file.")
        return make_response("Error: Prepared file data missing or corrupted.", 500)

    # --- Generator modification ---
    def generate_stream(path: str, pid: str):
        cleanup_delay_seconds = 120 
        timer = threading.Timer(cleanup_delay_seconds, _schedule_cleanup, args=[pid, path])
        timer.daemon = True
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
    logging.info(f"[{temp_id}] Preparing streaming response for '{dl_name}'.")
    response = Response(stream_with_context(generate_stream(temp_path, temp_id)), mimetype='application/octet-stream')

    try:
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

    if request.method == 'OPTIONS':
        logging.debug("Handling OPTIONS request for /api/auth/login")
        response = make_response()
        return response, 204 
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
            return make_response(jsonify({"error": "Internal server error during login."}), 500)

        # 4. Validate User and Password
        user_obj = None
        if user_doc:
            try:
                user_obj = User(user_doc)
            except ValueError as e:
                logging.error(f"Failed to create User object for {email} from doc {user_doc}: {e}", exc_info=True)
                return make_response(jsonify({"error": "Login failed due to inconsistent user data."}), 500)

        # Check if user object was created AND password matches
        if user_obj and user_obj.check_password(password):
            try:
                identity = user_obj.get_id()
                if not identity:
                     logging.error(f"User object for {email} is missing an ID.")
                     return make_response(jsonify({"error": "Internal server error - user identity missing."}), 500)
                access_token = create_access_token(identity=identity) 

                logging.info(f"User '{user_obj.username}' ({email}) logged in successfully via API. Token created.")
                return make_response(jsonify({
                    "message": "Login successful!",
                    "user": {
                        "username": user_obj.username,
                        "email": user_obj.email,
                        "id": identity 
                    },
                    "token": access_token  
                }), 200) 

            except Exception as e:
                 logging.error(f"Error creating JWT for user {email}: {e}", exc_info=True)
                 return make_response(jsonify({"error": "Internal server error during token generation."}), 500)

        else:
            logging.warning(f"Failed API login attempt for email: {email} (Invalid credentials or user not found)")
            return make_response(jsonify({"error": "Invalid email or password."}), 401)
    logging.warning(f"Received unexpected method {request.method} for /api/auth/login")
    return make_response(jsonify({"error": "Method Not Allowed"}), 405)

@app.route('/files/<username>', methods=['GET'])
@jwt_required()
def list_user_files(username: str) -> Response:
    # --- Handle OPTIONS Preflight ---
    if request.method == 'OPTIONS':
        logging.debug(f"Handling OPTIONS request for /files/{username}")
        response = make_response()
        # Flask-CORS should automatically add the necessary headers based on app config
        return response, 204

    # --- Handle GET Request (Existing Logic) ---
    if request.method == 'GET':
        log_prefix = f"[ListFiles-{username}]" # Added for clarity
        logging.info(f"{log_prefix} List files request received.")

        # Verify JWT identity matches the requested username path
        current_user_jwt_identity = get_jwt_identity()
        user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))

        if error or not user_doc:
            logging.error(f"{log_prefix} Could not find user for JWT identity '{current_user_jwt_identity}'. Error: {error}")
            return jsonify({"error": "User not found or token invalid."}), 401

        jwt_username = user_doc.get('username')
        if not jwt_username:
             logging.error(f"{log_prefix} User document for JWT identity missing username.")
             return jsonify({"error": "User identity error."}), 500

        if jwt_username != username:
             logging.warning(f"{log_prefix} Authorization mismatch: JWT user '{jwt_username}' requested files for '{username}'.")
             return jsonify({"error": "Forbidden: You can only list your own files."}), 403

        logging.info(f"{log_prefix} Authorized. Fetching files for user '{username}' from DB...")
        user_files, error_msg = find_metadata_by_username(username) # Use the verified username

        if error_msg:
            logging.error(f"{log_prefix} DB Error listing files: {error_msg}")
            return jsonify({"error": "Server error retrieving file list."}), 500

        if user_files is None:
            user_files = []

        logging.info(f"{log_prefix} Found {len(user_files)} records for '{username}'.")
        serializable_files = []
        for file_record in user_files:
            # Ensure _id is stringified if it exists and is an ObjectId
            if '_id' in file_record and isinstance(file_record['_id'], ObjectId):
                file_record['_id'] = str(file_record['_id'])
            serializable_files.append(file_record)

        return jsonify(serializable_files)

    # Should not be reached if methods are restricted to GET, OPTIONS
    logging.error(f"Unexpected method {request.method} for /files/{username}")
    return make_response(jsonify({"error": "Method Not Allowed"}), 405)
# @app.route('/download-single/<access_id>/<path:filename>')
# def download_single_file(access_id: str, filename: str):
#     """
#     Initiates the download preparation stream for a single file
#     within a batch.
#     """
#     prep_id = str(uuid.uuid4())
#     log_prefix = f"[SingleDLPrep-{prep_id}]"
#     logging.info(f"{log_prefix} Request to prep single file download: BatchID='{access_id}', Filename='{filename}'")

#     batch_info, error_msg = find_metadata_by_access_id(access_id)
#     if error_msg or not batch_info or not batch_info.get('is_batch'):
#         error_message_for_user = error_msg if error_msg else f"Batch '{access_id}' not found or invalid."
#         logging.warning(f"{log_prefix} Batch metadata lookup failed: {error_message_for_user}")
#         def error_stream(): yield _yield_sse_event('error', {'message': error_message_for_user})
#         # Ensure the error response also gets CORS headers if possible, though fixing the root cause is better
#         response = Response(stream_with_context(error_stream()), mimetype='text/event-stream',
#                             status=404 if "not found" in error_message_for_user.lower() else 400)
#         return response

#     files_in_batch = batch_info.get('files_in_batch', [])
#     target_file_info = next((f for f in files_in_batch if f.get('original_filename') == filename and not f.get('skipped') and not f.get('failed')), None)

#     if not target_file_info:
#         logging.warning(f"{log_prefix} File '{filename}' not found or was skipped/failed in batch '{access_id}'.")
#         def error_stream(): yield _yield_sse_event('error', {'message': f"File '{filename}' not found or unavailable in this batch."})
#         return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=404)

#     # Initialize variables for prep_data
#     prep_telegram_file_id: Optional[str] = None
#     prep_is_split = target_file_info.get('is_split', False)
#     prep_chunks_meta: Optional[List[Dict[str, Any]]] = None
#     prep_is_compressed = target_file_info.get('is_compressed', False)
    
#     if prep_is_split:
#         prep_chunks_meta = target_file_info.get('chunks')
#         if not prep_chunks_meta:
#             logging.error(f"{log_prefix} Split file '{filename}' in batch '{access_id}' is missing chunk metadata.")
#             def error_stream(): yield _yield_sse_event('error', {'message': f"Internal error: Chunk data missing for '{filename}'."})
#             return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=500)
#         logging.info(f"{log_prefix} File '{filename}' is split, will use chunk metadata for download.")
#         # For split files, prep_telegram_file_id remains None
#     else: # File is not split
#         send_locations = target_file_info.get('send_locations', [])
#         tg_file_id_lookup, chat_id_lookup = _find_best_telegram_file_id(locations=send_locations, primary_chat_id=PRIMARY_TELEGRAM_CHAT_ID)
        
#         if not tg_file_id_lookup: # This check is now correctly inside the 'else' block
#             logging.error(f"{log_prefix} No usable Telegram file_id found for non-split file '{filename}' in batch '{access_id}'. Locations: {send_locations}")
#             def error_stream(): yield _yield_sse_event('error', {'message': f"Could not find a valid source for file '{filename}'. Upload might be incomplete."})
#             return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=500)
        
#         prep_telegram_file_id = tg_file_id_lookup # Assign if found
#         logging.info(f"{log_prefix} Found Telegram file_id '{prep_telegram_file_id}' for non-split file '{filename}' from chat {chat_id_lookup}.")

#     download_prep_data[prep_id] = {
#         "prep_id": prep_id,
#         "status": "initiated",
#         "access_id": access_id, # Batch access_id
#         "username": batch_info.get('username'),
#         "requested_filename": filename, # The specific file requested from batch
#         "original_filename": filename, 
#         "telegram_file_id": prep_telegram_file_id, # Correctly None if split
#         "is_split": prep_is_split, 
#         "chunks_meta": prep_chunks_meta, # Correctly None if not split
#         "is_compressed": prep_is_compressed,
#         "final_expected_size": target_file_info.get('original_size', 0),
#         "compressed_total_size": target_file_info.get('compressed_total_size', 0),
#         "error": None,
#         "final_temp_file_path": None,
#         "final_file_size": 0,
#         "start_time": time.time()
#     }
#     logging.debug(f"{log_prefix} Stored initial prep data for single file download: {download_prep_data[prep_id]}")
    
#     # This should now always be a valid Response object if no prior 'return' was hit
#     return Response(stream_with_context(
#         _prepare_download_and_generate_updates(prep_id)
#     ), mimetype='text/event-stream')

@app.route('/download-single/<access_id>/<path:filename>')
def download_single_file(access_id: str, filename: str):
    """
    Initiates the download preparation stream for a single file
    within a batch.
    """
    prep_id = str(uuid.uuid4())
    log_prefix = f"[SingleDLPrep-{prep_id}]"
    logging.info(f"{log_prefix} Request to prep single file download: BatchID='{access_id}', Filename='{filename}'")

    batch_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg or not batch_info or not batch_info.get('is_batch'):
        error_message_for_user = error_msg if error_msg else f"Batch '{access_id}' not found or invalid."
        logging.warning(f"{log_prefix} Batch metadata lookup failed: {error_message_for_user}")
        def error_stream(): yield _yield_sse_event('error', {'message': error_message_for_user})
        response = Response(stream_with_context(error_stream()), mimetype='text/event-stream',
                            status=404 if "not found" in error_message_for_user.lower() else 400)
        return response

    files_in_batch = batch_info.get('files_in_batch', [])
    target_file_info = next((f for f in files_in_batch if f.get('original_filename') == filename and not f.get('skipped') and not f.get('failed')), None)

    if not target_file_info:
        logging.warning(f"{log_prefix} File '{filename}' not found or was skipped/failed in batch '{access_id}'.")
        def error_stream(): yield _yield_sse_event('error', {'message': f"File '{filename}' not found or unavailable in this batch."})
        return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=404)

    prep_telegram_file_id: Optional[str] = None
    prep_is_split = target_file_info.get('is_split', False)
    prep_chunks_meta: Optional[List[Dict[str, Any]]] = None
    prep_is_compressed = target_file_info.get('is_compressed', False)
    
    if prep_is_split:
        prep_chunks_meta = target_file_info.get('chunks')
        if not prep_chunks_meta: # Checks for None or empty list
            logging.error(f"{log_prefix} Split file '{filename}' in batch '{access_id}' is missing chunk metadata or has an empty chunk list.")
            def error_stream(): yield _yield_sse_event('error', {'message': f"Internal error: Chunk data missing for '{filename}'."})
            return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=500)
        logging.info(f"{log_prefix} File '{filename}' is split, will use chunk metadata for download. Chunks found: {len(prep_chunks_meta)}")
    else: 
        send_locations = target_file_info.get('send_locations', [])
        tg_file_id_lookup, chat_id_lookup = _find_best_telegram_file_id(locations=send_locations, primary_chat_id=PRIMARY_TELEGRAM_CHAT_ID)
        
        if not tg_file_id_lookup: 
            logging.error(f"{log_prefix} No usable Telegram file_id found for non-split file '{filename}' in batch '{access_id}'. Locations: {send_locations}")
            def error_stream(): yield _yield_sse_event('error', {'message': f"Could not find a valid source for file '{filename}'. Upload might be incomplete."})
            return Response(stream_with_context(error_stream()), mimetype='text/event-stream', status=500)
        
        prep_telegram_file_id = tg_file_id_lookup 
        logging.info(f"{log_prefix} Found Telegram file_id '{prep_telegram_file_id}' for non-split file '{filename}' from chat {chat_id_lookup}.")

    download_prep_data[prep_id] = {
        "prep_id": prep_id,
        "status": "initiated",
        "access_id": access_id, 
        "username": batch_info.get('username'),
        "requested_filename": filename, 
        "original_filename": filename, 
        "telegram_file_id": prep_telegram_file_id, 
        "is_split": prep_is_split, 
        "chunks_meta": prep_chunks_meta, 
        "is_compressed": prep_is_compressed,
        "final_expected_size": target_file_info.get('original_size', 0),
        "compressed_total_size": target_file_info.get('compressed_total_size', 0),
        "is_item_from_batch": True, # Explicitly mark that this prep_data is for a file within a batch
        "error": None,
        "final_temp_file_path": None,
        "final_file_size": 0,
        "start_time": time.time()
    }
    logging.debug(f"{log_prefix} Final prep_data being used for SSE stream: {json.dumps(download_prep_data[prep_id], indent=2, default=str)}")
    
    return Response(stream_with_context(
        _prepare_download_and_generate_updates(prep_id)
    ), mimetype='text/event-stream')

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
    
    if file_info.get('is_batch'):
        logging.warning(f"{log_prefix} Attempted to get batch info via single file API.")
        return jsonify({"error": "This ID refers to a batch, not a single file."}), 400
    response_data = {
        "access_id": access_id,
        "original_filename": file_info.get('original_filename', 'Unknown'),
        "original_size": file_info.get('original_size', 0),
        "upload_timestamp": file_info.get('upload_timestamp'),
        "username": file_info.get('username', 'N/A'),
        "is_compressed": file_info.get('is_compressed', False), 
        "is_split": file_info.get('is_split', False),         
    }
    logging.info(f"{log_prefix} Successfully retrieved single file details.")
    return jsonify(response_data)

@app.route('/delete-file/<username>/<path:access_id_from_path>', methods=['DELETE'])
@jwt_required() 
def delete_file_record(username: str, access_id_from_path: str) -> Response:
    log_prefix = f"[DeleteFile-{access_id_from_path}]"
    logging.info(f"{log_prefix} DELETE request for username='{username}', access_id_from_path='{access_id_from_path}'")

    # 1. --- Get Requesting User's Identity from JWT ---
    current_user_jwt_identity = get_jwt_identity()
    if not current_user_jwt_identity: 
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
    record_anonymous_id = record_to_delete.get('anonymous_id') 
    can_delete = False
    if record_owner_username == requesting_username:
        can_delete = True

    if not can_delete:
        logging.warning(f"{log_prefix} Permission denied: User '{requesting_username}' (from JWT) attempted to delete record "
                        f"with access_id '{access_id_from_path}' owned by '{record_owner_username}'. "
                        f"Path username was '{username}'.")
        return jsonify({"error": "Permission denied to delete this record."}), 403 

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
@jwt_required() 
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
                user_obj = User(user_doc)
             except ValueError as e:
                 logging.error(f"Failed to create User object for {email}: {e}", exc_info=True)
                 flash('Login failed due to inconsistent user data. Please contact support.', 'danger')
                 return render_template('login.html')
        if user_obj and user_obj.check_password(password):
            login_user(user_obj, remember=False)
            logging.info(f"User '{user_obj.username}' ({email}) logged in successfully.")
            flash(f'Welcome back, {user_obj.username}!', 'success')
            next_page = request.args.get('next')
            if next_page and not next_page.startswith('/'):
                next_page = None 
            return redirect(next_page or url_for('index'))
        else:
            logging.warning(f"Failed login attempt for email: {email}")
            flash('Invalid email or password. Please try again.', 'danger')
            return render_template('login.html')
    return render_template('login.html')


# @app.route('/', defaults={'path': ''})
# @app.route('/<path:path>')
# def serve_angular_app(path: str):
#     """
#     Serves the Angular application's index.html for any route
#     not handled by other Flask routes (like API routes).
#     This allows Angular's client-side router to take over.
#     """
#     angular_index_path = os.path.join(app.static_folder, 'index.html')
#     if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
#         return send_from_directory(app.static_folder, path)
#     elif os.path.exists(angular_index_path):
#         logging.info(f"Serving Angular index.html for path: /{path}")
#         return send_from_directory(app.static_folder, 'index.html')
#     else:
#         logging.error(f"Angular index.html not found at: {angular_index_path}")
#         return "Angular application not found.", 404

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


    response_data = {
        "batch_name": batch_info.get('batch_display_name', f"Batch {access_id}"),
        "username": batch_info.get('username', 'N/A'),
        "upload_date": batch_info.get('upload_timestamp'),
        "total_size": batch_info.get('total_original_size', 0),
        "files": batch_info.get('files_in_batch', []),
        "access_id": access_id
    }

    processed_files = []
    for f_item in response_data["files"]:
        processed_f_item = f_item.copy()
        if 'original_filename' not in processed_f_item:
            processed_f_item['original_filename'] = "Unknown File"
        if 'original_size' not in processed_f_item:
            processed_f_item['original_size'] = 0
        processed_f_item.setdefault('skipped', False)
        processed_f_item.setdefault('failed', False)
        processed_f_item.setdefault('reason', None)
        processed_files.append(processed_f_item)
    response_data["files"] = processed_files

    logging.info(f"{log_prefix} Successfully retrieved and processed batch details for API.")
    return jsonify(response_data)

@app.route('/logout')
@login_required
def logout():
    """Logs the current user out."""
    user_email = current_user.email 
    logout_user() 
    logging.info(f"User {user_email} logged out.")
    flash('You have been successfully logged out.', 'success')
    return redirect(url_for('login'))
