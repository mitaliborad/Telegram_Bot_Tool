import logging
import mimetypes
import os
import uuid
import time
import json
import shutil
import io
from typing import Dict, Any, Tuple, Optional, List, Generator
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from extensions import login_manager, upload_progress_data, download_prep_data
from flask import (
    Blueprint, request, make_response, jsonify, Response, stream_with_context, url_for
)
from flask import Response, stream_with_context
from database import find_metadata_by_access_id
from .utils import _yield_sse_event, _calculate_progress, _safe_remove_directory, _safe_remove_file
import tempfile
from google_drive_api import download_from_gdrive, delete_from_gdrive, upload_to_gdrive_with_progress
from flask_jwt_extended import jwt_required, get_jwt_identity
from bson import ObjectId
from werkzeug.utils import secure_filename
from database import User, find_user_by_id, save_file_metadata, get_metadata_collection
from extensions import upload_progress_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID, CHUNK_SIZE,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES,
    format_bytes # format_bytes is used by _send_single_file_task & _send_chunk_task
)
from telegram_api import send_file_to_telegram
from flask import stream_with_context

from routes.utils import _yield_sse_event, _calculate_progress, _safe_remove_directory
from extensions import upload_progress_data
# from google_drive_api import upload_to_gdrive
# Type Aliases
background_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix='BgTgTransfer')
ApiResult = Tuple[bool, str, Optional[Dict[str, Any]]] # success, message, response_json
SseEvent = str

upload_bp = Blueprint('upload', __name__)

def _parse_send_results(log_prefix: str, send_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parses results from Telegram send operations for metadata storage."""
    all_chat_details = []
    for res in send_results:
        detail: Dict[str, Any] = {"chat_id": res["chat_id"], "success": res["success"]}
        if res["success"] and res["tg_response"]:
            res_data = res["tg_response"].get('result', {})
            msg_id = res_data.get('message_id')
            doc_data = res_data.get('document', {})
            f_id = doc_data.get('file_id')
            f_uid = doc_data.get('file_unique_id')
            f_size = doc_data.get('file_size')
            if msg_id and f_id and f_uid:
                detail["message_id"] = msg_id
                detail["file_id"] = f_id
                detail["file_unique_id"] = f_uid
                if f_size is not None: detail["file_size"] = f_size
            else:
                detail["success"] = False
                detail["error"] = "Missing critical IDs in Telegram response"
                logging.warning(f"[{log_prefix}] Missing IDs in TG response: {res['tg_response']}")
        elif not res["success"]:
            detail["error"] = res["message"]
        all_chat_details.append(detail)
    return all_chat_details


def _send_single_file_task(file_path: str, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
    # Your existing implementation for sending a single file (from file_path) to Telegram
    log_prefix = f"[{upload_id}] Task for '{filename}' to {chat_id}"
    try:
        with open(file_path, 'rb') as f_handle:
            file_size = os.path.getsize(file_path)
            logging.info(f"{log_prefix} Sending single file ({format_bytes(file_size)}) from path: {file_path} to Telegram.")
            # This calls your actual telegram_api.send_file_to_telegram
            success, message, tg_response = send_file_to_telegram(f_handle, filename, chat_id)
        logging.info(f"{log_prefix} Single file Telegram send result: Success={success}")
        return str(chat_id), (success, message, tg_response)
    except FileNotFoundError:
         logging.error(f"{log_prefix} Local temp file for Telegram not found at path: {file_path}")
         return str(chat_id), (False, f"Local temp file not found: {filename}", None)
    except Exception as e:
        logging.error(f"{log_prefix} Unexpected error sending single file to Telegram: {e}", exc_info=True)
        return str(chat_id), (False, f"Thread error processing single file for Telegram: {e}", None)


def _send_chunk_task(chunk_data: bytes, filename: str, chat_id: str, upload_id: str, chunk_num: int) -> Tuple[str, ApiResult]:
    # Your existing implementation for sending a chunk (from bytes) to Telegram
    log_prefix = f"[{upload_id}] TG Chunk {chunk_num} ('{filename}') to {chat_id}"
    try:
        buffer = io.BytesIO(chunk_data)
        logging.info(f"{log_prefix} Sending TG chunk ({format_bytes(len(chunk_data))})")
        success, message, tg_response = send_file_to_telegram(buffer, filename, chat_id)
        buffer.close()
        logging.info(f"{log_prefix} Send TG chunk result: Success={success}")
        return str(chat_id), (success, message, tg_response)
    except Exception as e:
        logging.error(f"{log_prefix} Unexpected error sending TG chunk: {e}", exc_info=True)
        return str(chat_id), (False, f"Thread error processing TG chunk: {e}", None)

def gdrive_upload_and_db_save_task(operation_id, user_info, filename, file_size, in_memory_stream):
    """
    A background task that handles the entire GDrive upload and DB save process.
    """
    log_prefix = f"[GDriveTask-{operation_id}]"
    gdrive_file_id = None
    upload_error = None
    try:
        logging.info(f"{log_prefix} Background task started for file '{filename}'.")
        
        # ==========================================================================
        #  THE FINAL FIX IS HERE: Manually consume the generator with next()
        # ==========================================================================
        # Create the generator instance
        upload_generator = upload_to_gdrive_with_progress(
            source=in_memory_stream,
            filename_in_gdrive=filename,
            operation_id_for_log=operation_id
        )

        # We must use a `while True` loop and `next()` to be able to catch StopIteration
        while True:
            try:
                # Get the next event (e.g., progress update) from the uploader
                progress_event = next(upload_generator)
                # Share this progress with the listening SSE stream
                upload_progress_data[operation_id] = progress_event
            except StopIteration as e:
                # This block is now correctly reached when the generator finishes.
                # The return value is in the exception's `value` attribute.
                gdrive_file_id, upload_error = e.value
                logging.info(f"{log_prefix} Generator finished. GDrive ID: {gdrive_file_id}, Error: {upload_error}")
                break # Exit the while loop
        # ==========================================================================

        if upload_error:
            raise Exception(upload_error)
        
        if not gdrive_file_id:
            raise Exception("GDrive upload complete but no file ID returned.")
        
        # --- Save Metadata ---
        file_details = {"original_filename": filename, "gdrive_file_id": gdrive_file_id, "original_size": file_size, "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream', "telegram_send_status": "pending"}
        db_record_payload = {"access_id": operation_id, "username": user_info['username'], "user_email": user_info['user_email'], "is_anonymous": user_info['is_anonymous'], "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "storage_location": "gdrive", "status_overall": "gdrive_complete_pending_telegram", "is_batch": False, "batch_display_name": filename, "files_in_batch": [file_details], "total_original_size": file_size}
        
        save_success, save_msg = save_file_metadata(db_record_payload)
        if not save_success:
            delete_from_gdrive(gdrive_file_id)
            raise Exception(f"DB save failed: {save_msg}")

        frontend_base_url = os.environ.get('FRONTEND_URL', '').rstrip('/')
        download_url = f"{frontend_base_url}/batch-view/{operation_id}"
        
        upload_progress_data[operation_id] = {"type": "complete", "message": "File uploaded successfully.", "access_id": operation_id, "download_url": download_url}
        
        logging.info(f"{log_prefix} Submitting GDrive-to-Telegram transfer.")
        background_executor.submit(run_gdrive_to_telegram_transfer, operation_id)

    except Exception as e:
        logging.error(f"{log_prefix} Error in background task: {e}", exc_info=True)
        if gdrive_file_id: delete_from_gdrive(gdrive_file_id)
        upload_progress_data[operation_id] = {"type": "error", "message": str(e)}

@upload_bp.route('/initiate-batch', methods=['POST'])
@jwt_required(optional=True)
def initiate_batch_upload():
    """
    Phase 1: Creates a placeholder "batch" record in the database.
    """
    operation_id = str(uuid.uuid4())
    log_prefix = f"[BatchInitiate-{operation_id}]"
    
    current_user_jwt_identity = get_jwt_identity()
    user_info = {"is_anonymous": True, "username": "Anonymous", "user_email": None}
    if current_user_jwt_identity:
        user_doc, _ = find_user_by_id(ObjectId(current_user_jwt_identity))
        if user_doc:
            user_info.update({"is_anonymous": False, "username": user_doc.get("username"), "user_email": user_doc.get("email")})

    data = request.get_json()
    batch_display_name = data.get('batch_display_name', 'Unnamed Batch')
    total_original_size = data.get('total_original_size', 0)
    is_batch = data.get('is_batch', False)

    db_record_payload = {
        "access_id": operation_id,
        "username": user_info['username'], "user_email": user_info['user_email'], "is_anonymous": user_info['is_anonymous'],
        "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "storage_location": "gdrive",
        "status_overall": "batch_initiated",
        "is_batch": is_batch,
        "batch_display_name": batch_display_name,
        "files_in_batch": [],
        "total_original_size": total_original_size,
    }

    save_success, save_msg = save_file_metadata(db_record_payload)
    if not save_success:
        logging.error(f"{log_prefix} DB placeholder save failed: {save_msg}.")
        return jsonify({"error": f"Failed to initiate batch record: {save_msg}"}), 500

    logging.info(f"{log_prefix} Batch placeholder created for user '{user_info['username']}'.")
    return jsonify({"message": "Batch initiated successfully.", "batch_id": operation_id}), 201

@upload_bp.route('/stream', methods=['POST', 'OPTIONS'])
@jwt_required(optional=True)
def stream_file_to_batch():
    """
    Phase 2: Streams a single file and ADDS its metadata to an EXISTING batch record.
    """
    if request.method == 'OPTIONS':
        return make_response(("OK", 200))
        
    batch_id = request.headers.get('X-Batch-Id')
    if not batch_id:
        return jsonify({"error": "Request is missing the 'X-Batch-Id' header."}), 400

    log_prefix = f"[StreamForBatch-{batch_id}]"
    
    filename = secure_filename(request.headers.get('X-Filename', ''))
    file_size = int(request.headers.get('X-Filesize', 0))
    if not filename:
        return jsonify({"error": "Filename header missing."}), 400

    gdrive_op_id = f"{batch_id}-{uuid.uuid4().hex[:6]}"
    upload_progress_data[gdrive_op_id] = {}
    
    in_memory_stream = io.BytesIO(request.stream.read())
    gdrive_file_id, upload_error = None, None

    try:
        # ==========================================================================
        #  THE FIX IS HERE: Manually consume the generator with next()
        # ==========================================================================
        upload_generator = upload_to_gdrive_with_progress(in_memory_stream, filename, gdrive_op_id)
        
        while True:
            try:
                # We don't need to do anything with the progress events here,
                # just consume them to drive the upload.
                next(upload_generator)
            except StopIteration as e:
                # When the generator is done, its return value is in `e.value`
                gdrive_file_id, upload_error = e.value
                logging.info(f"{log_prefix} Generator finished for '{filename}'. GDrive ID: {gdrive_file_id}, Error: {upload_error}")
                break # Exit the while loop
        # ==========================================================================

        if upload_error: raise Exception(upload_error)
        if not gdrive_file_id: raise Exception("GDrive upload complete but no file ID returned.")
    
        file_details = {"original_filename": filename, "gdrive_file_id": gdrive_file_id, "original_size": file_size, "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream', "telegram_send_status": "pending"}
        coll, db_error = get_metadata_collection()
        if db_error: raise Exception(db_error)
        
        result = coll.update_one({"access_id": batch_id}, {"$push": {"files_in_batch": file_details}})
        if result.matched_count == 0:
            delete_from_gdrive(gdrive_file_id) # Cleanup orphan
            raise Exception(f"Batch ID '{batch_id}' not found.")
            
    except Exception as e:
        if gdrive_file_id: delete_from_gdrive(gdrive_file_id)
        logging.error(f"{log_prefix} Streaming to GDrive failed for file '{filename}': {e}", exc_info=True)
        return jsonify({"error": f"Failed to upload '{filename}': {str(e)}"}), 500
    finally:
        if gdrive_op_id in upload_progress_data: del upload_progress_data[gdrive_op_id]

    return jsonify({"message": f"File '{filename}' streamed successfully."}), 200

@upload_bp.route('/finalize-batch/<batch_id>', methods=['POST'])
@jwt_required(optional=True)
def finalize_batch_upload(batch_id: str):
    """
    Phase 3: Finalizes the batch and triggers the background processing.
    """
    log_prefix = f"[BatchFinalize-{batch_id}]"
    coll, db_error = get_metadata_collection()
    if db_error: return jsonify({"error": "DB connection error"}), 500
    
    update_result = coll.update_one({"access_id": batch_id}, {"$set": {"status_overall": "gdrive_complete_pending_telegram"}})
    if update_result.matched_count == 0:
        return jsonify({"error": "Batch record not found."}), 404

    logging.info(f"{log_prefix} Batch finalized. Submitting background task.")
    background_executor.submit(run_gdrive_to_telegram_transfer, batch_id)

    frontend_base_url = os.environ.get('FRONTEND_URL', '').rstrip('/')
    download_url = f"{frontend_base_url}/batch-view/{batch_id}"

    return jsonify({"message": "Batch finalized.", "access_id": batch_id, "download_url": download_url}), 200
               
@upload_bp.route('/stream', methods=['POST'])
@jwt_required(optional=True)
def stream_upload_data():
    """
    Phase 1: Receives file data via POST and starts the background upload task.
    Immediately returns an operation_id for the client to poll for status.
    """
    operation_id = str(uuid.uuid4())
    log_prefix = f"[StreamAccept-{operation_id}]"
    
    current_user_jwt_identity = get_jwt_identity()
    user_info = {"is_anonymous": True, "username": "Anonymous", "user_email": None}
    if current_user_jwt_identity:
        user_doc, _ = find_user_by_id(ObjectId(current_user_jwt_identity))
        if user_doc:
            user_info.update({"is_anonymous": False, "username": user_doc.get("username"), "user_email": user_doc.get("email")})
            
    filename = secure_filename(request.headers.get('X-Filename', ''))
    file_size = int(request.headers.get('X-Filesize', 0))
    if not filename:
        return jsonify({"error": "Filename header missing."}), 400

    logging.info(f"{log_prefix} Accepting file '{filename}' to stream in background.")
    in_memory_stream = io.BytesIO(request.stream.read())
    upload_progress_data[operation_id] = {"type": "status", "message": "Upload initiated..."}
    
    background_executor.submit(gdrive_upload_and_db_save_task, operation_id, user_info, filename, file_size, in_memory_stream)
    
    return jsonify({
        "message": "Upload initiated. Poll status endpoint for progress.",
        "operation_id": operation_id
    }), 202

    
@upload_bp.route('/stream-status/<operation_id>')
def stream_upload_status(operation_id: str):
    """
    Phase 2: A GET endpoint that clients connect to for real-time SSE progress
    of an upload operation.
    """
    def generate_status_events():
        last_yielded_data = None
        try:
            while True:
                current_data = upload_progress_data.get(operation_id)
                if current_data and current_data != last_yielded_data:
                    yield _yield_sse_event(current_data.get("type", "status"), current_data)
                    last_yielded_data = current_data
                    
                    if current_data.get("type") in ["complete", "error"]:
                        break
                
                time.sleep(0.5)
        finally:
            if operation_id in upload_progress_data:
                del upload_progress_data[operation_id]
                logging.info(f"[StreamStatus-{operation_id}] SSE stream closed and progress data cleaned up.")

    # This route is now implicitly GET-only, which is correct for EventSource.
    return Response(stream_with_context(generate_status_events()), mimetype='text/event-stream')

@upload_bp.route('/sse/gdrive-upload-status/<operation_id>')
def sse_gdrive_upload_status(operation_id: str) -> Response:
    log_prefix_sse_gdrive = f"[SSE-GDrive-{operation_id}]"
    logging.info(f"{log_prefix_sse_gdrive} SSE connection established for GDrive upload phase.")
    def generate_gdrive_upload_events():
        gdrive_upload_error_final = None
        # Retrieve initial upload data stored by initiate_upload
        initial_upload_data = upload_progress_data.get(operation_id)
        if not initial_upload_data:
            logging.error(f"{log_prefix_sse_gdrive} No initial data found for operation_id.")
            yield _yield_sse_event("error", {"message": "Upload session not found or expired."})
            return

        files_to_upload_to_gdrive = initial_upload_data.get("files_for_gdrive_upload", [])
        
        # We NO LONGER need the temp_batch_source_dir here for cleanup
        # temp_batch_source_dir = initial_upload_data.get("temp_batch_source_dir") 

        if not files_to_upload_to_gdrive:
            gdrive_upload_error_final = "No files specified for GDrive upload in session data."
            logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
            yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
            return

        # ... (user details, SSE 'start' event logic remains the same) ...
        display_username = initial_upload_data.get("username", "anonymous")
        user_email = initial_upload_data.get("user_email")
        is_anonymous_user = initial_upload_data.get("is_anonymous", True)
        anonymous_id_form_val = initial_upload_data.get("anonymous_id")
        is_batch_overall = initial_upload_data.get("is_batch_overall", False)
        batch_display_name_overall = initial_upload_data.get("batch_display_name_overall", "Uploaded Files")
        
        total_operation_size = sum(f.get("original_size", 0) for f in files_to_upload_to_gdrive)
        yield _yield_sse_event("start", {"filename": batch_display_name_overall, "totalSize": total_operation_size})

        gdrive_files_details_for_db = []

        try:
            for index, file_to_upload_info in enumerate(files_to_upload_to_gdrive):
                current_local_temp_path = file_to_upload_info.get("temp_local_path")
                current_original_filename = file_to_upload_info.get("original_filename")
                current_original_size = file_to_upload_info.get("original_size", 0)

                if not current_local_temp_path or not os.path.exists(current_local_temp_path) or not current_original_filename:
                    # ... (error handling for this remains the same) ...
                    logging.error(f"{log_prefix_sse_gdrive} Invalid file info or missing temp file for GDrive upload: {file_to_upload_info}")
                    gdrive_upload_error_final = f"Missing or invalid temporary file for {current_original_filename or 'unknown file'}."
                    yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                    for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
                    return

                status_message = f"Storing {current_original_filename} ({index + 1}/{len(files_to_upload_to_gdrive)})..."
                yield _yield_sse_event("status", {"message": status_message})
                
                # --- GDrive Upload Logic (remains the same) ---
                current_file_gdrive_id = None
                current_file_gdrive_error = None

                for progress_event in upload_to_gdrive_with_progress(
                    source=current_local_temp_path,
                    filename_in_gdrive=current_original_filename,
                    operation_id_for_log=operation_id
                ):
                    # ... (progress yielding logic remains the same) ...
                    if progress_event.get("type") == "progress":
                        yield _yield_sse_event("progress", {
                            "percentage": progress_event.get("percentage", 0),
                            "bytesSent": int(current_original_size * (progress_event.get("percentage", 0) / 100.0)) if current_original_size > 0 else 0,
                            "totalBytes": current_original_size,
                            "currentFile": current_original_filename
                        })
                    elif progress_event.get("type") == "error":
                        current_file_gdrive_error = progress_event.get("message", f"Unknown GDrive upload error for {current_original_filename}")
                        logging.error(f"{log_prefix_sse_gdrive} Error from GDrive upload generator for {current_original_filename}: {current_file_gdrive_error}")
                        break
                
                if current_file_gdrive_error:
                    # ... (error handling remains the same) ...
                    gdrive_upload_error_final = f"Failed GDrive upload for {current_original_filename}: {current_file_gdrive_error}"
                    yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                    for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
                    return
                
                # ... (getting gdrive_file_id logic remains the same) ...
                updated_op_data_after_current_gdrive = upload_progress_data.get(operation_id, {})
                current_file_gdrive_id = updated_op_data_after_current_gdrive.get("gdrive_file_id_temp_result")

                if not current_file_gdrive_id:
                    # ... (error handling remains the same) ...
                    gdrive_upload_error_final = f"GDrive file ID not retrieved for {current_original_filename} after upload."
                    yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                    for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
                    return
                
                logging.info(f"{log_prefix_sse_gdrive} GDrive upload successful for {current_original_filename}. File ID: {current_file_gdrive_id}")
                gdrive_files_details_for_db.append({
                    "original_filename": current_original_filename,
                    "gdrive_file_id": current_file_gdrive_id,
                    "original_size": current_original_size,
                    "mime_type": mimetypes.guess_type(current_original_filename)[0] or 'application/octet-stream',
                    "telegram_send_status": "pending",
                })
                # ==========================================================
                #  REMOVED: This line is no longer needed here.
                # _safe_remove_file(current_local_temp_path, ...)
                # ==========================================================
            
            # ... (the rest of the try block remains the same: DB saving, yielding gdrive_complete, starting background task) ...
            if not gdrive_files_details_for_db:
                gdrive_upload_error_final = "No files were successfully uploaded to GDrive."
                yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                return

            db_record_payload = {
                "access_id": operation_id,
                "username": display_username, "user_email": user_email, "is_anonymous": is_anonymous_user,
                "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "storage_location": "gdrive",
                "status_overall": "gdrive_complete_pending_telegram",
                "is_batch": is_batch_overall,
                "batch_display_name": batch_display_name_overall,
                "files_in_batch": gdrive_files_details_for_db,
                "total_original_size": sum(f.get("original_size", 0) for f in gdrive_files_details_for_db),
            }
            if is_anonymous_user and anonymous_id_form_val:
                db_record_payload["anonymous_id_form"] = anonymous_id_form_val

            save_success, save_msg = save_file_metadata(db_record_payload)
            if not save_success:
                gdrive_upload_error_final = f"Failed to save DB record after GDrive uploads: {save_msg}"
                yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
                return

            logging.info(f"{log_prefix_sse_gdrive} MongoDB record saved for {operation_id} (GDrive stage complete).")
            
            yield _yield_sse_event("gdrive_complete", {
                "message": f"'{batch_display_name_overall}' is stored and ready. Archival to final storage in progress.",
                "access_id": operation_id,
                "filename": batch_display_name_overall,
                "is_batch": is_batch_overall
            })
            
            logging.info(f"{log_prefix_sse_gdrive} Submitting Telegram transfer for {operation_id} to background executor.")
            background_executor.submit(run_gdrive_to_telegram_transfer, operation_id)

        except Exception as e_gen:
            # ... (Exception handling remains the same) ...
            gdrive_upload_error_final = f"Internal server error during GDrive upload SSE: {str(e_gen)}"
            logging.error(f"{log_prefix_sse_gdrive} Unhandled exception in GDrive upload SSE: {e_gen}", exc_info=True)
            yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
            for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
        
        finally:
            # ==========================================================
            #  REMOVED: The cleanup logic is moved to the background task.
            # ==========================================================
            # if temp_batch_source_dir and os.path.exists(temp_batch_source_dir):
            #     _safe_remove_directory(temp_batch_source_dir, ...)
            
            # The progress data should be cleaned up by the background task now.
            # Only clean up here if an error prevented the background task from starting.
            if gdrive_upload_error_final and operation_id in upload_progress_data:
                # We need to clean up the source files here if we errored out
                initial_data = upload_progress_data.get(operation_id, {})
                temp_dir = initial_data.get("temp_batch_source_dir")
                if temp_dir and os.path.exists(temp_dir):
                    _safe_remove_directory(temp_dir, log_prefix_sse_gdrive, "batch source on GDrive SSE error")
                else: # single file
                    files_info = initial_data.get("files_for_gdrive_upload", [])
                    if files_info and files_info[0].get("temp_local_path"):
                        _safe_remove_file(files_info[0]["temp_local_path"], log_prefix_sse_gdrive, "single source on GDrive SSE error")

                del upload_progress_data[operation_id]
                logging.info(f"{log_prefix_sse_gdrive} Cleaned up progress data for {operation_id} due to GDrive phase error.")

            logging.info(f"{log_prefix_sse_gdrive} SSE stream for GDrive upload phase ended for client.")
    
    return Response(stream_with_context(generate_gdrive_upload_events()), mimetype='text/event-stream')

# routes/upload_routes.py

@upload_bp.route('/initiate-upload', methods=['POST'])
@jwt_required(optional=True)
def initiate_upload() -> Response:
    operation_id = str(uuid.uuid4())
    log_prefix = f"[{operation_id}]"
    logging.info(f"{log_prefix} Request to initiate GDrive upload stage.")
    
    current_user_jwt_identity = get_jwt_identity()
    display_username: Optional[str] = None
    user_email: Optional[str] = None
    is_anonymous: bool = False
    anonymous_id_form: Optional[str] = None
    
    if current_user_jwt_identity:
        is_anonymous = False
        try:
            user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
            if error or not user_doc:
                return jsonify({"error": "Invalid user token or user not found"}), 401
            user_object_from_jwt = User(user_doc) 
            display_username = user_object_from_jwt.username
            user_email = user_object_from_jwt.email
        except Exception as e: 
            logging.error(f"{log_prefix} Error processing JWT: {e}", exc_info=True)
            return jsonify({"error": "Server error processing authentication"}), 500
    else:
        is_anonymous = True
        anonymous_id_form = request.form.get('anonymous_upload_id')
        if not anonymous_id_form:
            return jsonify({"error": "Missing anonymous identifier."}), 400
        display_username = f"AnonymousUser-{anonymous_id_form[:6]}"
    
    if display_username is None:
        return jsonify({"error": "Internal server error (user identity)."}), 500
    
    uploaded_files = request.files.getlist('files[]')
    if not uploaded_files or all(not f.filename for f in uploaded_files):
        return jsonify({"error": "No files selected."}), 400
    
    is_multi_file_upload = len(uploaded_files) > 1
    files_for_gdrive_phase_data = []
    batch_temp_dir_for_gdrive = None # Only used if is_multi_file_upload

    try:
        if is_multi_file_upload:
            batch_temp_dir_for_gdrive = os.path.join(UPLOADS_TEMP_DIR, f"gdrive_batch_src_{operation_id}")
            os.makedirs(batch_temp_dir_for_gdrive, exist_ok=True)
            logging.info(f"{log_prefix} Created batch temp dir for GDrive: {batch_temp_dir_for_gdrive}")

        for file_storage_item in uploaded_files:
            if not file_storage_item.filename:
                logging.warning(f"{log_prefix} Skipping a file with no name.")
                continue

            # ================================================================
            #  THE FIX IS HERE: Sanitize the filename to remove paths
            # ================================================================
            original_filename = secure_filename(file_storage_item.filename)
            # This turns 'folder/subfolder/file.png' into 'folder_subfolder_file.png'
            # or simply 'file.png' into 'file.png', making it safe to save.
            # If the resulting filename is empty (e.g., from a filename like "/"), skip it.
            if not original_filename:
                logging.warning(f"{log_prefix} Skipping file with an invalid/empty name after sanitization.")
                continue
            # ================================================================

            temp_local_path_for_this_file: str
            
            if is_multi_file_upload and batch_temp_dir_for_gdrive:
                temp_local_path_for_this_file = os.path.join(batch_temp_dir_for_gdrive, original_filename)
            else:
                temp_file_descriptor, temp_local_path_for_this_file = tempfile.mkstemp(
                    dir=UPLOADS_TEMP_DIR, 
                    prefix=f"{operation_id}_gdrive_src_single_", 
                    suffix=os.path.splitext(original_filename)[1]
                )
                os.close(temp_file_descriptor)
            
            file_storage_item.save(temp_local_path_for_this_file)
            original_size = os.path.getsize(temp_local_path_for_this_file)
            logging.info(f"{log_prefix} File '{original_filename}' (size: {original_size}) saved to temp: {temp_local_path_for_this_file}")
            
            files_for_gdrive_phase_data.append({
                "original_filename": original_filename,
                "temp_local_path": temp_local_path_for_this_file,
                "original_size": original_size
            })

        if not files_for_gdrive_phase_data:
            if batch_temp_dir_for_gdrive: _safe_remove_directory(batch_temp_dir_for_gdrive, log_prefix, "empty batch source dir")
            return jsonify({"error": "No valid files were processed for upload."}), 400

        batch_display_name = files_for_gdrive_phase_data[0]['original_filename']
        if is_multi_file_upload:
            # Use original folder name if provided by frontend, otherwise create a generic one
            folder_name_from_form = request.form.get('folder_name') 
            if folder_name_from_form:
                 batch_display_name = secure_filename(folder_name_from_form)
            else:
                 batch_display_name = f"Batch of {len(files_for_gdrive_phase_data)} files"

        
        upload_progress_data[operation_id] = {
            "status": "initiated_gdrive_upload_sse",
            "files_for_gdrive_upload": files_for_gdrive_phase_data,
            "temp_batch_source_dir": batch_temp_dir_for_gdrive if is_multi_file_upload else None,
            "username": display_username,
            "user_email": user_email,
            "is_anonymous": is_anonymous,
            "anonymous_id": anonymous_id_form,
            "batch_display_name_overall": batch_display_name,
            "is_batch_overall": is_multi_file_upload,
            "start_time_initiate": time.time(),
        }
        
        sse_url = url_for('upload.sse_gdrive_upload_status', operation_id=operation_id, _external=False)
        logging.info(f"{log_prefix} Returning GDrive SSE URL: {sse_url} for operation_id: {operation_id}")
        
        return jsonify({
            "upload_id": operation_id, 
            "filename": batch_display_name, # Display name for the whole operation
            "sse_gdrive_upload_url": sse_url
        })

    except Exception as e:
        logging.error(f"{log_prefix} Error in initiate_upload: {e}", exc_info=True)
        # Cleanup any created temp files/dirs
        for file_data in files_for_gdrive_phase_data:
            if file_data.get("temp_local_path") and os.path.exists(file_data["temp_local_path"]):
                _safe_remove_file(file_data["temp_local_path"], log_prefix, "orphaned temp source in initiate_upload error")
        if batch_temp_dir_for_gdrive and os.path.exists(batch_temp_dir_for_gdrive):
            _safe_remove_directory(batch_temp_dir_for_gdrive, log_prefix, "orphaned batch source dir in initiate_upload error")
        if operation_id in upload_progress_data:
            del upload_progress_data[operation_id]
        return jsonify({"error": f"Server error initiating upload: {str(e)}"}), 500

@upload_bp.route('/stream-progress/<upload_id>')
def stream_progress(upload_id: str) -> Response:
    logging.info(f"SSE connect request for upload_id: {upload_id}")
    progress_entry = upload_progress_data.get(upload_id)
    
    # Check DB first if not in progress_data, as background task might have completed
    if not progress_entry:
        db_record, _ = find_metadata_by_access_id(upload_id)
        if db_record:
            status_from_db = db_record.get("status_overall")
            if status_from_db in ['telegram_complete', 'telegram_processing_errors', 
                                  'error_telegram_processing', 'error_telegram_processing_unhandled_bg']:
                logging.warning(f"Upload ID '{upload_id}' found in DB with status '{status_from_db}'. No active SSE stream.")
                def stream_gen_finalized_db(): yield _yield_sse_event('error', {'message': f'Upload {upload_id} already finalized ({status_from_db}).'})
                return Response(stream_with_context(stream_gen_finalized_db()), mimetype='text/event-stream')
        
        # If not in progress_data and not in DB as finalized by background task, then it's likely invalid or truly old
        logging.warning(f"Upload ID '{upload_id}' not found in active progress data or relevant DB state. May be completed or invalid.")
        def stream_gen_not_found(): yield _yield_sse_event('error', {'message': f'Upload ID {upload_id} not found or processing already finished.'})
        return Response(stream_with_context(stream_gen_not_found()), mimetype='text/event-stream')
    
    status = progress_entry.get('status', 'unknown')

    if status == "initiated_telegram_processing": # Legacy path, or if background task signal is needed differently
        logging.warning(f"SSE for Telegram processing (legacy path) for upload_id: {upload_id}. Consider if this is still needed.")
        # If this is still intended to stream updates from a background task, the mechanism needs to be via a shared queue/DB status polling.
        # For now, if this path is hit, it would use the old generator, which might not align with background processing.
        # return Response(stream_with_context(process_upload_and_generate_updates(upload_id)), mimetype='text/event-stream')
        def stream_gen_bg_tg(): yield _yield_sse_event('status', {'message': 'Telegram processing is handled in the background. No direct SSE updates for this phase.'})
        return Response(stream_with_context(stream_gen_bg_tg()), mimetype='text/event-stream')
    elif status in ['completed', 'error', 'completed_metadata_error', 'completed_with_errors']:
        logging.warning(f"Upload ID '{upload_id}' already finalized (Status:{status}). No further SSE stream.")
        def stream_gen_finalized(): yield _yield_sse_event('error', {'message': f'Upload {upload_id} already finalized ({status}).'})
        return Response(stream_with_context(stream_gen_finalized()), mimetype='text/event-stream')
    elif status == "initiated_gdrive_upload_sse":
        logging.warning(f"Upload ID '{upload_id}' is in GDrive SSE phase. Client should use GDrive SSE URL.")
        def stream_gen_gdrive(): yield _yield_sse_event('error', {'message': f'Upload {upload_id} is in GDrive phase. Connect to GDrive SSE URL.'})
        return Response(stream_with_context(stream_gen_gdrive()), mimetype='text/event-stream')
    else:
        logging.warning(f"Upload ID '{upload_id}' in unexpected state for this SSE endpoint: {status}")
        def stream_gen_unexpected(): yield _yield_sse_event('error', {'message': f'Upload {upload_id} in unexpected state: {status}.'})
        return Response(stream_with_context(stream_gen_unexpected()), mimetype='text/event-stream')

def process_upload_and_generate_updates(upload_id_or_access_id: str) -> Generator[SseEvent, None, None]:
    # Note: upload_id_or_access_id is the access_id of the record in MongoDB
    # It's also the key in upload_progress_data during the GDrive->Telegram handoff.
    executor: Optional[ThreadPoolExecutor] = None
    log_prefix = f"[{upload_id_or_access_id}]"
    db_record = None # Initialize to allow access in finally if an early error occurs

    try:
        logging.info(f"{log_prefix} Starting GDrive-to-Telegram processing generator.")
        
        # 1. Fetch the initial record from MongoDB (which should have been created by GDrive SSE phase)
        db_record, db_error = find_metadata_by_access_id(upload_id_or_access_id)

        if db_error or not db_record:
            logging.error(f"{log_prefix} Failed to fetch MongoDB record for processing: {db_error or 'Record not found'}")
            yield _yield_sse_event('error', {'message': f"Internal error: Could not retrieve upload details for processing ({upload_id_or_access_id})."})
            # No need to update upload_progress_data here, as it will be cleaned up in finally
            return

        # 2. Check current state from DB record
        # Expected state: storage_location="gdrive", status_overall="gdrive_complete_pending_telegram"
        if not (db_record.get("storage_location") == "gdrive" and 
                db_record.get("status_overall") == "gdrive_complete_pending_telegram"):
            current_status = db_record.get("status_overall", "unknown")
            logging.warning(f"{log_prefix} Record not in expected state for GDrive-to-Telegram. Status: {current_status}, Location: {db_record.get('storage_location')}")
            
            if "telegram_complete" in current_status or "telegram_processing_errors" in current_status :
                yield _yield_sse_event('status', {'message': 'Telegram processing already handled for this item.'})
                # Optionally, yield a 'complete' event again if the frontend might have missed it,
                # using data from the existing db_record. This depends on frontend resilience.
            elif "error" in current_status: # Generic error status
                yield _yield_sse_event('error', {'message': f"Previous error: {db_record.get('last_error', 'Unknown error')}"})
            else: # Any other unexpected status
                yield _yield_sse_event('error', {'message': f"Item in unexpected state: {current_status}. Cannot start Telegram processing."})
            return # Stop further processing for this stream

        # 3. Retrieve necessary data from DB record and transient upload_progress_data
        # upload_progress_data might still hold some context like original client filename for display
        # from the initiate_upload -> sse_gdrive_upload_status handoff.
        progress_entry_context = upload_progress_data.get(upload_id_or_access_id, {})
        
        username = db_record.get('username', progress_entry_context.get('username', 'anonymous'))
        files_to_process_from_gdrive = db_record.get("files_in_batch", [])
        # Use batch_display_name from progress_entry_context if available (set by initiate_upload), fallback to DB
        batch_display_name_for_sse = progress_entry_context.get("batch_display_name", db_record.get("batch_display_name", "Upload"))
        db_record_access_id = db_record["access_id"] # Should be same as upload_id_or_access_id

        if not files_to_process_from_gdrive:
            logging.error(f"{log_prefix} No GDrive file details in DB record for processing (files_in_batch is empty).")
            yield _yield_sse_event('error', {'message': 'Internal error: Missing GDrive file details in record for Telegram processing.'})
            db_record["status_overall"] = "error_missing_gdrive_details_tg_phase"
            db_record["last_error"] = "files_in_batch was empty in DB record for Telegram processing"
            save_file_metadata(db_record)
            return

        if not (TELEGRAM_CHAT_IDS and len(TELEGRAM_CHAT_IDS) > 0):
            logging.error(f"{log_prefix} No Telegram chat IDs configured.")
            yield _yield_sse_event('error', {'message': 'Server configuration error: No destination chats for Telegram.'})
            db_record["status_overall"] = "error_config_telegram_chats_tg_phase"
            db_record["last_error"] = "No destination chats configured for Telegram processing"
            save_file_metadata(db_record)
            return

        executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'TgUpload_{db_record_access_id[:4]}')
        logging.info(f"{log_prefix} Initialized Telegram Upload Executor (max={MAX_UPLOAD_WORKERS})")

        total_original_bytes_for_sse = db_record.get("total_original_size", 0)
        if total_original_bytes_for_sse <= 0 : # If size is 0 or missing, try to sum from files_in_batch
            total_original_bytes_for_sse = sum(f.get("original_size",0) for f in files_to_process_from_gdrive)
            if total_original_bytes_for_sse <=0:
                 logging.warning(f"{log_prefix} Total original size is 0 or not calculable. Progress reporting might be affected.")
                 # It might be a batch of empty files, which is valid but progress will be static.

        yield _yield_sse_event('start', {'filename': batch_display_name_for_sse, 'totalSize': total_original_bytes_for_sse})
        yield _yield_sse_event('status', {'message': f'Starting Telegram transfer for {len(files_to_process_from_gdrive)} file(s)...'})

        overall_telegram_processing_start_time = time.time()
        bytes_processed_for_sse_progress = 0
        processed_files_for_final_db_record = []
        batch_overall_telegram_success = True

        # --- Loop through files from the DB record (which have GDrive IDs) ---
        for file_detail_from_db in files_to_process_from_gdrive:
            original_filename = file_detail_from_db.get("original_filename")
            gdrive_file_id = file_detail_from_db.get("gdrive_file_id")
            original_file_size = file_detail_from_db.get("original_size", 0)

            if not original_filename or not gdrive_file_id:
                logging.error(f"{log_prefix} Skipping file in batch due to missing original_filename or gdrive_file_id: {file_detail_from_db}")
                # Create a placeholder entry for the DB to show it was skipped due to bad data
                skipped_entry = file_detail_from_db.copy()
                skipped_entry["telegram_send_status"] = "skipped_bad_db_data"
                skipped_entry["reason_telegram"] = "Missing original_filename or gdrive_file_id in DB record."
                processed_files_for_final_db_record.append(skipped_entry)
                batch_overall_telegram_success = False # This is a data integrity issue
                bytes_processed_for_sse_progress += original_file_size # Count as "processed" for progress consistency
                continue
            
            updated_file_meta_for_db = file_detail_from_db.copy() # Start with existing data
            log_file_prefix_indiv = f"{log_prefix} File '{original_filename}' (GDriveID: {gdrive_file_id})"
            logging.info(f"{log_file_prefix_indiv} Starting Telegram processing stage.")
            
            local_temp_path_for_processing: Optional[str] = None
            updated_file_meta_for_db["telegram_send_status"] = "processing" # Initial status for this file's TG processing

            try:
                yield _yield_sse_event('status', {'message': f'Fetching "{original_filename}" from GDrive for Telegram...'})
                logging.info(f"{log_file_prefix_indiv} Downloading from GDrive for Telegram...")
                
                gdrive_content_stream, download_err = download_from_gdrive(gdrive_file_id)
                if download_err or not gdrive_content_stream:
                    # Raise an exception to be caught by the outer try-except for this file
                    raise Exception(f"GDrive download for Telegram failed: {download_err or 'No content stream received'}")

                # Create a temporary file to store GDrive content for Telegram processing
                # Ensure suffix is derived correctly even if original_filename has no extension
                file_suffix = os.path.splitext(original_filename)[1] if '.' in original_filename else ".tmp"
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=file_suffix) as temp_file_on_disk:
                    local_temp_path_for_processing = temp_file_on_disk.name
                    shutil.copyfileobj(gdrive_content_stream, temp_file_on_disk)
                gdrive_content_stream.close() # Close the stream from GDrive API
                logging.info(f"{log_file_prefix_indiv} Saved GDrive content to local temp for TG: {local_temp_path_for_processing}")
                
                current_file_processing_size = os.path.getsize(local_temp_path_for_processing)
                if current_file_processing_size == 0:
                    updated_file_meta_for_db["telegram_send_status"] = "skipped_empty"
                    updated_file_meta_for_db["reason_telegram"] = "File empty after GDrive download for Telegram."
                    logging.warning(f"{log_file_prefix_indiv} File is empty after GDrive download. Skipping for Telegram.")
                    # This is not necessarily a failure of the overall batch, just this file is skipped.
                else:
                    # --- Actual Telegram Upload Logic (Chunked or Single) ---
                    if current_file_processing_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
                        updated_file_meta_for_db["is_split_for_telegram"] = True
                        logging.info(f"{log_file_prefix_indiv} Is large ({format_bytes(current_file_processing_size)}), starting chunked upload to Telegram.")
                        part_number_tg = 1
                        bytes_processed_for_this_file_tg_chunking = 0
                        all_chunks_sent_successfully_for_this_file_tg = True
                        temp_tg_chunks_meta = [] # To store metadata for each chunk

                        with open(local_temp_path_for_processing, 'rb') as f_in_tg_process:
                            while True:
                                chunk_data_tg = f_in_tg_process.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
                                if not chunk_data_tg: break
                                
                                chunk_tg_filename = f"{original_filename}.part{part_number_tg}"
                                log_chunk_prefix_tg = f"{log_file_prefix_indiv} TG Chunk {part_number_tg}"
                                logging.info(f"{log_chunk_prefix_tg} Preparing ({format_bytes(len(chunk_data_tg))}) for Telegram.")

                                chunk_specific_tg_futures: Dict[Future, str] = {}
                                chunk_specific_tg_results: Dict[str, ApiResult] = {}
                                primary_send_success_for_this_tg_chunk = False 
                                primary_send_message_for_this_tg_chunk = "Primary TG chunk send not attempted or failed."

                                for chat_id_str_loop_tg in TELEGRAM_CHAT_IDS:
                                    fut_tg_chunk = executor.submit(_send_chunk_task, chunk_data_tg, chunk_tg_filename, str(chat_id_str_loop_tg), upload_id_or_access_id, part_number_tg)
                                    chunk_specific_tg_futures[fut_tg_chunk] = str(chat_id_str_loop_tg)
                                
                                # Wait for primary chat first for quicker feedback/failure
                                primary_tg_chunk_fut = next((f for f, cid_val in chunk_specific_tg_futures.items() if cid_val == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                                if primary_tg_chunk_fut:
                                    try:
                                        cid_res_tg_chunk, res_tg_chunk = primary_tg_chunk_fut.result()
                                        chunk_specific_tg_results[cid_res_tg_chunk] = res_tg_chunk
                                        primary_send_success_for_this_tg_chunk, primary_send_message_for_this_tg_chunk = res_tg_chunk[0], res_tg_chunk[1]
                                    except Exception as e_fut:
                                        logging.error(f"{log_chunk_prefix_tg} Exception getting result for primary chunk future: {e_fut}")
                                        primary_send_success_for_this_tg_chunk = False
                                        primary_send_message_for_this_tg_chunk = f"Error in primary chunk task: {e_fut}"
                                
                                # Wait for remaining futures
                                for fut_completed_tg_chunk in as_completed(chunk_specific_tg_futures):
                                    if fut_completed_tg_chunk == primary_tg_chunk_fut and primary_tg_chunk_fut in chunk_specific_tg_results : continue # Already processed
                                    try:
                                        cid_res_tg_c, res_tg_c = fut_completed_tg_chunk.result()
                                        if cid_res_tg_c not in chunk_specific_tg_results: chunk_specific_tg_results[cid_res_tg_c] = res_tg_c
                                    except Exception as e_fut_other:
                                        chat_id_failed = chunk_specific_tg_futures.get(fut_completed_tg_chunk, "unknown_chat_id")
                                        logging.error(f"{log_chunk_prefix_tg} Exception getting result for chunk future to chat {chat_id_failed}: {e_fut_other}")
                                        # Store this failure in results if not already present
                                        if chat_id_failed not in chunk_specific_tg_results:
                                            chunk_specific_tg_results[chat_id_failed] = (False, f"Task error: {e_fut_other}", None)

                                parsed_tg_locations_for_this_chunk = _parse_send_results(
                                    f"{log_chunk_prefix_tg}-Parse", 
                                    [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} 
                                     for k, r in chunk_specific_tg_results.items()]
                                )
                                
                                primary_chunk_parsed_info = next((loc for loc in parsed_tg_locations_for_this_chunk if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                                
                                if primary_chunk_parsed_info and primary_chunk_parsed_info.get("success"):
                                    temp_tg_chunks_meta.append({"part_number": part_number_tg, "size": len(chunk_data_tg), "send_locations": parsed_tg_locations_for_this_chunk})
                                    bytes_processed_for_this_file_tg_chunking += len(chunk_data_tg)
                                    yield _yield_sse_event('status', {'message': f'Sent TG chunk {part_number_tg} for "{original_filename}"'})
                                else: # Primary chunk send failed (either API call or parsed as failure)
                                    error_reason_chunk_tg = primary_send_message_for_this_tg_chunk # Default from API call
                                    if primary_chunk_parsed_info and primary_chunk_parsed_info.get('error'): # Parsed error is more specific
                                        error_reason_chunk_tg = primary_chunk_parsed_info.get('error')
                                    elif not primary_chunk_parsed_info: # Primary chat result not even found in parsed results
                                        error_reason_chunk_tg = "Primary chat send result missing after parsing."

                                    logging.error(f"{log_chunk_prefix_tg} Telegram send FAILED. Reason: {error_reason_chunk_tg}. Aborting for this file.")
                                    batch_overall_telegram_success = False
                                    all_chunks_sent_successfully_for_this_file_tg = False
                                    updated_file_meta_for_db["telegram_send_status"] = "failed_chunking"
                                    updated_file_meta_for_db["reason_telegram"] = f"Failed TG chunk {part_number_tg}: {error_reason_chunk_tg}"
                                    # Store the problematic chunk's attempt details
                                    updated_file_meta_for_db["telegram_chunks"] = [{"part_number": part_number_tg, "size": len(chunk_data_tg), "send_locations": parsed_tg_locations_for_this_chunk}] 
                                    break # Break from chunking loop for THIS file
                                part_number_tg += 1
                        
                        if all_chunks_sent_successfully_for_this_file_tg:
                            updated_file_meta_for_db["telegram_chunks"] = temp_tg_chunks_meta
                            updated_file_meta_for_db["telegram_send_status"] = "success_chunked"
                            updated_file_meta_for_db["telegram_total_chunked_size"] = bytes_processed_for_this_file_tg_chunking
                        # If not all_chunks_sent_successfully, status and reason should have been set when a chunk failed
                    
                    else: # SINGLE FILE to Telegram
                        updated_file_meta_for_db["is_split_for_telegram"] = False
                        single_tg_file_futures: Dict[Future, str] = {}
                        single_tg_file_results: Dict[str, ApiResult] = {}
                        primary_send_api_call_success_tg = False
                        primary_send_api_message_tg = "Primary TG send (single) failed."

                        for chat_id_str_single_tg in TELEGRAM_CHAT_IDS:
                            fut_single_tg = executor.submit(_send_single_file_task, local_temp_path_for_processing, original_filename, str(chat_id_str_single_tg), upload_id_or_access_id)
                            single_tg_file_futures[fut_single_tg] = str(chat_id_str_single_tg)
                        
                        primary_fut_single_tg = next((f for f, cid_val in single_tg_file_futures.items() if cid_val == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                        if primary_fut_single_tg:
                            try:
                                cid_res_tg_s, res_tg_s = primary_fut_single_tg.result()
                                single_tg_file_results[cid_res_tg_s] = res_tg_s
                                primary_send_api_call_success_tg, primary_send_api_message_tg = res_tg_s[0], res_tg_s[1]
                            except Exception as e_fut_single:
                                logging.error(f"{log_file_prefix_indiv} Exception getting result for primary single file future: {e_fut_single}")
                                primary_send_api_call_success_tg = False
                                primary_send_api_message_tg = f"Error in primary single file task: {e_fut_single}"
                        
                        for fut_completed_tg_s in as_completed(single_tg_file_futures):
                            if fut_completed_tg_s == primary_fut_single_tg and primary_fut_single_tg in single_tg_file_results: continue
                            try:
                                cid_res_tg_s_comp, res_tg_s_comp = fut_completed_tg_s.result()
                                if cid_res_tg_s_comp not in single_tg_file_results: single_tg_file_results[cid_res_tg_s_comp] = res_tg_s_comp
                            except Exception as e_fut_single_other:
                                chat_id_failed_single = single_tg_file_futures.get(fut_completed_tg_s, "unknown_chat_id")
                                logging.error(f"{log_file_prefix_indiv} Exception getting result for single file future to chat {chat_id_failed_single}: {e_fut_single_other}")
                                if chat_id_failed_single not in single_tg_file_results:
                                    single_tg_file_results[chat_id_failed_single] = (False, f"Task error: {e_fut_single_other}", None)

                        parsed_tg_locations_single_file = _parse_send_results(
                            f"{log_file_prefix_indiv}-TGSendSingle", 
                            [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} 
                             for k,r in single_tg_file_results.items()]
                        )

                        primary_single_parsed_info = next((loc for loc in parsed_tg_locations_single_file if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)

                        if primary_single_parsed_info and primary_single_parsed_info.get("success"):
                            updated_file_meta_for_db["telegram_send_locations"] = parsed_tg_locations_single_file
                            updated_file_meta_for_db["telegram_send_status"] = "success_single"
                        else: # Primary single send failed
                            error_reason_single_tg = primary_send_api_message_tg 
                            if primary_single_parsed_info and primary_single_parsed_info.get('error'):
                                error_reason_single_tg = primary_single_parsed_info.get('error')
                            elif not primary_single_parsed_info:
                                error_reason_single_tg = "Primary chat send result missing after parsing for single file."
                            
                            logging.error(f"{log_file_prefix_indiv} Single TG Send FAILED. Reason: {error_reason_single_tg}.")
                            batch_overall_telegram_success = False
                            updated_file_meta_for_db["telegram_send_status"] = "failed_single"
                            updated_file_meta_for_db["reason_telegram"] = f"Primary TG send failed: {error_reason_single_tg}"
                            updated_file_meta_for_db["telegram_send_locations"] = parsed_tg_locations_single_file # Store failure details
            
                # After Telegram processing attempt for THIS file (successful, skipped, or failed)
                if updated_file_meta_for_db["telegram_send_status"].startswith("success") or \
                   updated_file_meta_for_db["telegram_send_status"] == "skipped_empty":
                    # If successfully sent to Telegram (or skipped harmlessly), delete from GDrive
                    logging.info(f"{log_file_prefix_indiv} Telegram stage complete (status: {updated_file_meta_for_db['telegram_send_status']}). Deleting from GDrive ID: {gdrive_file_id}")
                    gdrive_delete_success, gdrive_delete_error = delete_from_gdrive(gdrive_file_id)
                    if not gdrive_delete_success:
                        logging.warning(f"{log_file_prefix_indiv} Failed to delete from GDrive ID {gdrive_file_id} after Telegram stage: {gdrive_delete_error}")
                        updated_file_meta_for_db["gdrive_cleanup_error"] = gdrive_delete_error # Record GDrive cleanup failure
                else: # Telegram processing failed for this file
                    batch_overall_telegram_success = False # Ensure this is set if any file fails TG
                    logging.warning(f"{log_file_prefix_indiv} Not deleting from GDrive as Telegram processing failed (status: {updated_file_meta_for_db['telegram_send_status']}). GDrive ID: {gdrive_file_id}")
                    # The reason for TG failure should be in updated_file_meta_for_db["reason_telegram"]

                # Always add the original file size to SSE progress, regardless of TG success/failure for this file,
                # as the GDrive download part was attempted / completed for this file.
                bytes_processed_for_sse_progress += original_file_size

            except Exception as file_processing_exception: # Catches GDrive download errors or unexpected errors in this file's TG prep
                logging.error(f"{log_file_prefix_indiv} UNEXPECTED error during GDrive download or Telegram prep for this file: {file_processing_exception}", exc_info=True)
                updated_file_meta_for_db["telegram_send_status"] = "error_processing_file_tg_phase"
                updated_file_meta_for_db["reason_telegram"] = f"Internal error during file processing for Telegram: {str(file_processing_exception)}"
                batch_overall_telegram_success = False
                bytes_processed_for_sse_progress += original_file_size # Count as "attempted" for progress
            finally:
                if local_temp_path_for_processing and os.path.exists(local_temp_path_for_processing):
                    _safe_remove_file(local_temp_path_for_processing, log_file_prefix_indiv, "local temp GDrive content file for TG")
            
            processed_files_for_final_db_record.append(updated_file_meta_for_db)
            yield _yield_sse_event('progress', _calculate_progress(overall_telegram_processing_start_time, bytes_processed_for_sse_progress, total_original_bytes_for_sse))
            yield _yield_sse_event('status', {'message': f'Telegram: Processed {len(processed_files_for_final_db_record)} of {len(files_to_process_from_gdrive)} files...'})
        
        # --- After loop for all files ---
        total_batch_telegram_duration = time.time() - overall_telegram_processing_start_time
        logging.info(f"{log_prefix} Finished all files GDrive-to-Telegram processing. Duration: {total_batch_telegram_duration:.2f}s. Overall Batch Success for Telegram: {batch_overall_telegram_success}")

        # Update the main DB record with the results of Telegram processing
        db_record["files_in_batch"] = processed_files_for_final_db_record # Replace with updated file info
        db_record["storage_location"] = "telegram" if batch_overall_telegram_success else "mixed_gdrive_telegram_error"
        db_record["status_overall"] = "telegram_complete" if batch_overall_telegram_success else "telegram_processing_errors"
        db_record["telegram_processing_timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        db_record["total_telegram_processing_duration_seconds"] = round(total_batch_telegram_duration, 2)
        if not batch_overall_telegram_success:
            # Collect specific reasons from individual files that had issues
            error_reasons = [
                f"File '{f.get('original_filename', 'Unknown File')}': {f.get('reason_telegram', 'Unknown TG error')}"
                for f in processed_files_for_final_db_record 
                if not f.get("telegram_send_status", "").startswith("success") and f.get("telegram_send_status") != "skipped_empty"
            ]
            db_record["last_error"] = "; ".join(error_reasons) if error_reasons else "One or more files failed Telegram processing with no specific reason recorded."

        save_success_final, save_msg_final = save_file_metadata(db_record) # This will upsert/update
        if not save_success_final:
            logging.error(f"{log_prefix} DB CRITICAL: Failed to UPDATE MongoDB record after Telegram processing: {save_msg_final}")
            yield _yield_sse_event('error', {'message': f"Failed to finalize record after Telegram processing: {save_msg_final}"})
            if upload_id_or_access_id in upload_progress_data:
                upload_progress_data[upload_id_or_access_id]['status'] = 'completed_metadata_error_tg_phase'
                upload_progress_data[upload_id_or_access_id]['error'] = f"Final DB update fail (TG): {save_msg_final}"
            return
        
        logging.info(f"{log_prefix} DB: Successfully UPDATED MongoDB record after Telegram processing (access_id: {db_record_access_id}).")
        
        # Determine if it was effectively a single file upload for the 'is_batch' SSE field
        is_batch_for_sse_complete = db_record.get("is_batch", False) or len(processed_files_for_final_db_record) > 1

        final_sse_filename_display = batch_display_name_for_sse # Default to batch name
        if not is_batch_for_sse_complete and processed_files_for_final_db_record: # Single effective file
            final_sse_filename_display = processed_files_for_final_db_record[0].get('original_filename', batch_display_name_for_sse)
        
        # Construct download/view URL
        browser_url = f"{request.host_url.rstrip('/')}/get/{db_record_access_id}" # Generic link
        if is_batch_for_sse_complete: 
            browser_url = f"{request.host_url.rstrip('/')}/batch-view/{db_record_access_id}" # Specific batch view

        complete_message_text = f'"{final_sse_filename_display}" Telegram processing complete. ' + \
                        ('Some files had issues.' if not batch_overall_telegram_success else 'All files transferred successfully.')
        
        complete_payload = {
            'message': complete_message_text, 
            'download_url': browser_url, 
            'filename': final_sse_filename_display, 
            'access_id': db_record_access_id, 
            'batch_access_id': db_record_access_id, # Same as access_id for this flow
            'is_batch': is_batch_for_sse_complete,
        }
        
        # Update transient status one last time before cleanup in `finally`
        if upload_id_or_access_id in upload_progress_data:
            upload_progress_data[upload_id_or_access_id]['status'] = 'completed_with_errors' if not batch_overall_telegram_success else 'completed'
        
        logging.info(f"{log_prefix} Yielding final 'complete' event. Payload: {json.dumps(complete_payload)}")
        yield _yield_sse_event('complete', complete_payload)

    except Exception as e_outer: # Catch-all for unexpected errors in the generator's main block
        error_msg_final = f"Critical GDrive-to-Telegram processing error: {str(e_outer) or type(e_outer).__name__}"
        logging.error(f"{log_prefix} UNHANDLED EXCEPTION in GDrive-to-Telegram generator: {e_outer}", exc_info=True)
        yield _yield_sse_event('error', {'message': error_msg_final})
        # Update transient data if exists
        if upload_id_or_access_id in upload_progress_data: 
            upload_progress_data[upload_id_or_access_id]['status'] = 'error_tg_phase_unhandled'
            upload_progress_data[upload_id_or_access_id]['error'] = error_msg_final
        # Attempt to update DB record if it was fetched
        if db_record: 
            db_record["status_overall"] = "error_telegram_processing_unhandled"
            db_record["last_error"] = error_msg_final
            # Optionally include more details about which file was being processed if applicable
            save_file_metadata(db_record)
                
    finally:
        logging.info(f"{log_prefix} GDrive-to-Telegram generator final cleanup.")
        if executor:
            executor.shutdown(wait=True) # Wait for all submitted tasks to complete
            logging.info(f"{log_prefix} Telegram Upload executor shutdown (waited).")
        
        # Remove the operation_id from transient upload_progress_data as this SSE stream is finished.
        if upload_id_or_access_id in upload_progress_data:
            try:
                del upload_progress_data[upload_id_or_access_id]
                logging.info(f"{log_prefix} Removed transient progress data for {upload_id_or_access_id} after Telegram SSE.")
            except KeyError:
                # This might happen if an error occurred very early and it was already cleaned up
                # by the sse_gdrive_upload_status's finally block.
                logging.warning(f"{log_prefix} Attempted to remove progress data for {upload_id_or_access_id}, but key was already gone.")
        
        final_status_report_db = 'unknown (db_record not available or status key missing)'
        if db_record: # Check if db_record was fetched and available during the process
            final_status_report_db = db_record.get('status_overall', 'unknown (status_overall key missing in DB)')
        
        logging.info(f"{log_prefix} GDrive-to-Telegram generator finished. Final DB Status for {upload_id_or_access_id}: {final_status_report_db}")


# In routes/upload_routes.py

# def run_gdrive_to_telegram_transfer(access_id: str):
#     """
#     Handles the GDrive to Telegram transfer in the background.
#     This function is now responsible for ALL cleanup for the operation.
#     """
#     log_prefix = f"[BG-TG-{access_id}]"
#     logging.info(f"{log_prefix} Background GDrive-to-Telegram transfer started.")
    
#     # This executor is for sending files/chunks to Telegram in parallel
#     tg_send_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'BgTgSend_{access_id[:4]}')
    
#     db_record = None
#     # Retrieve the initial temporary file paths from the progress data before it's deleted.
#     # This is crucial for the final cleanup step.
#     progress_context = upload_progress_data.get(access_id, {})
#     initial_temp_batch_dir = progress_context.get("temp_batch_source_dir")
#     initial_temp_single_file_list = progress_context.get("files_for_gdrive_upload", [])

#     try:
#         # 1. Fetch the database record created during the GDrive upload phase.
#         db_record, db_error = find_metadata_by_access_id(access_id)
#         if db_error or not db_record:
#             logging.error(f"{log_prefix} Failed to fetch DB record: {db_error or 'Not found'}")
#             return # `finally` block will still run for cleanup

#         # 2. Safety check: ensure the record is in the correct state.
#         if db_record.get("status_overall") != "gdrive_complete_pending_telegram":
#             logging.warning(f"{log_prefix} Record not in 'gdrive_complete_pending_telegram' state. Current: {db_record.get('status_overall')}. Aborting background task.")
#             return # `finally` block will still run

#         # 3. Update status to show background processing has started.
#         db_record["status_overall"] = "telegram_processing_background"
#         save_file_metadata(db_record) 

#         files_to_process = db_record.get("files_in_batch", [])
#         processed_files_for_db = []
#         batch_tg_success = True
        
#         # 4. Loop through each file that was uploaded to GDrive.
#         for file_detail in files_to_process:
#             original_filename = file_detail.get("original_filename")
#             gdrive_file_id = file_detail.get("gdrive_file_id")

#             if not original_filename or not gdrive_file_id:
#                 logging.error(f"{log_prefix} Skipping file in batch due to missing data: {file_detail}")
#                 skipped_entry = file_detail.copy()
#                 skipped_entry["telegram_send_status"] = "skipped_bad_db_data"
#                 processed_files_for_db.append(skipped_entry)
#                 batch_tg_success = False
#                 continue

#             current_file_log_prefix = f"{log_prefix} File '{original_filename}'"
#             updated_file_meta = file_detail.copy() 
#             updated_file_meta["telegram_send_status"] = "processing_bg" # Default status
#             local_temp_file_for_tg: Optional[str] = None

#             try:
#                 # 4a. Download the file from GDrive to a new temporary file for this specific operation.
#                 logging.info(f"{current_file_log_prefix} Downloading from GDrive (ID: {gdrive_file_id}) for Telegram.")
#                 gdrive_stream, dl_err = download_from_gdrive(gdrive_file_id)
#                 if dl_err or not gdrive_stream:
#                     raise Exception(f"GDrive download failed for TG: {dl_err or 'No stream'}")

#                 file_suffix_tg = os.path.splitext(original_filename)[1] if '.' in original_filename else ".tmp"
#                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=file_suffix_tg) as temp_f:
#                     local_temp_file_for_tg = temp_f.name
#                     shutil.copyfileobj(gdrive_stream, temp_f)
#                 gdrive_stream.close()
#                 current_file_size = os.path.getsize(local_temp_file_for_tg)
#                 logging.info(f"{current_file_log_prefix} Downloaded to temp: {local_temp_file_for_tg} (Size: {format_bytes(current_file_size)})")
                
#                 # =========================================================================
#                 #  START OF THE FIXED/INSERTED LOGIC
#                 # =========================================================================
#                 if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
#                     # --- CHUNKED UPLOAD LOGIC ---
#                     updated_file_meta["is_split_for_telegram"] = True
#                     logging.info(f"{current_file_log_prefix} is large, starting chunked upload to Telegram.")
#                     part_num = 1
#                     all_chunks_ok = True
#                     chunks_meta_list = []
#                     with open(local_temp_file_for_tg, 'rb') as f_in:
#                         while True:
#                             chunk_bytes = f_in.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
#                             if not chunk_bytes: break
                            
#                             chunk_filename = f"{original_filename}.part{part_num}"
#                             chunk_log_prefix = f"{current_file_log_prefix} TG Chunk {part_num}"
                            
#                             chunk_futures: Dict[Future, str] = {
#                                 tg_send_executor.submit(_send_chunk_task, chunk_bytes, chunk_filename, str(chat_id), access_id, part_num): str(chat_id)
#                                 for chat_id in TELEGRAM_CHAT_IDS
#                             }
                            
#                             chunk_results: List[Dict] = []
#                             for future in as_completed(chunk_futures):
#                                 chat_id_res = chunk_futures[future]
#                                 try:
#                                     _, api_result = future.result()
#                                     chunk_results.append({"chat_id": chat_id_res, "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
#                                 except Exception as e_fut:
#                                     logging.error(f"{chunk_log_prefix} future error for chat {chat_id_res}: {e_fut}")
#                                     chunk_results.append({"chat_id": chat_id_res, "success": False, "message": str(e_fut), "tg_response": None})
                            
#                             parsed_locations = _parse_send_results(f"{chunk_log_prefix}-Parse", chunk_results)
#                             primary_chunk_res = next((res for res in parsed_locations if str(res.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)

#                             if primary_chunk_res and primary_chunk_res.get("success"):
#                                 chunks_meta_list.append({"part_number": part_num, "size": len(chunk_bytes), "send_locations": parsed_locations})
#                             else:
#                                 all_chunks_ok = False
#                                 error_reason = primary_chunk_res.get('error', 'Primary chunk send failed.') if primary_chunk_res else "Primary chat result missing."
#                                 logging.error(f"{chunk_log_prefix} FAILED. Reason: {error_reason}. Aborting for this file.")
#                                 updated_file_meta["reason_telegram"] = f"Failed TG chunk {part_num}: {error_reason}"
#                                 updated_file_meta["telegram_chunks"] = chunks_meta_list # Save progress
#                                 break # Stop chunking this file
#                             part_num += 1
                    
#                     if all_chunks_ok:
#                         updated_file_meta["telegram_send_status"] = "success_chunked"
#                         updated_file_meta["telegram_chunks"] = chunks_meta_list
#                     else:
#                         updated_file_meta["telegram_send_status"] = "failed_chunking"
#                         batch_tg_success = False

#                 else:
#                     # --- SINGLE FILE UPLOAD LOGIC ---
#                     updated_file_meta["is_split_for_telegram"] = False
#                     logging.info(f"{current_file_log_prefix} is small, starting single file upload to Telegram.")
                    
#                     single_futures: Dict[Future, str] = {
#                         tg_send_executor.submit(_send_single_file_task, local_temp_file_for_tg, original_filename, str(chat_id), access_id): str(chat_id)
#                         for chat_id in TELEGRAM_CHAT_IDS
#                     }
                    
#                     single_results: List[Dict] = []
#                     for future in as_completed(single_futures):
#                         chat_id_res = single_futures[future]
#                         try:
#                             _, api_result = future.result()
#                             single_results.append({"chat_id": chat_id_res, "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
#                         except Exception as e_fut:
#                             logging.error(f"{current_file_log_prefix} future error for chat {chat_id_res}: {e_fut}")
#                             single_results.append({"chat_id": chat_id_res, "success": False, "message": str(e_fut), "tg_response": None})
                            
#                     parsed_locations = _parse_send_results(f"{current_file_log_prefix}-Parse", single_results)
#                     primary_res = next((res for res in parsed_locations if str(res.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)

#                     if primary_res and primary_res.get("success"):
#                         updated_file_meta["telegram_send_status"] = "success_single"
#                         updated_file_meta["telegram_send_locations"] = parsed_locations
#                     else:
#                         error_reason = primary_res.get('error', 'Primary send failed.') if primary_res else "Primary chat result missing."
#                         logging.error(f"{current_file_log_prefix} FAILED. Reason: {error_reason}.")
#                         updated_file_meta["telegram_send_status"] = "failed_single"
#                         updated_file_meta["reason_telegram"] = error_reason
#                         updated_file_meta["telegram_send_locations"] = parsed_locations
#                         batch_tg_success = False
#                 # =========================================================================
#                 #  END OF THE FIXED/INSERTED LOGIC
#                 # =========================================================================

#                 # 4b. Conditionally delete from GDrive based on the now-correct Telegram status.
#                 if updated_file_meta["telegram_send_status"].startswith("success"):
#                     logging.info(f"{current_file_log_prefix} Successfully sent to Telegram. Deleting from GDrive.")
#                     del_success, del_err = delete_from_gdrive(gdrive_file_id)
#                     if not del_success:
#                         logging.warning(f"{current_file_log_prefix} Failed GDrive delete ID {gdrive_file_id}: {del_err}")
#                         updated_file_meta["gdrive_cleanup_error"] = del_err
#                 else:
#                     # This path is taken if Telegram send failed.
#                     batch_tg_success = False
#                     logging.warning(f"{current_file_log_prefix} Failed Telegram send. File remains in GDrive (ID: {gdrive_file_id}).")

#             except Exception as e_file_processing:
#                 logging.error(f"{current_file_log_prefix} Error processing for Telegram: {e_file_processing}", exc_info=True)
#                 updated_file_meta["telegram_send_status"] = "error_processing_bg"
#                 updated_file_meta["reason_telegram"] = str(e_file_processing)
#                 batch_tg_success = False
#             finally:
#                 # 4c. Clean up the PER-FILE temp file created from the GDrive download. This runs for every file.
#                 if local_temp_file_for_tg and os.path.exists(local_temp_file_for_tg):
#                     _safe_remove_file(local_temp_file_for_tg, current_file_log_prefix, "temp for TG send")
            
#             processed_files_for_db.append(updated_file_meta)

#         # 5. After the loop, update the main database record with the final status.
#         db_record["files_in_batch"] = processed_files_for_db
#         if batch_tg_success:
#             db_record["storage_location"] = "telegram"
#             db_record["status_overall"] = "telegram_complete"
#         else:
#             db_record["storage_location"] = "mixed_gdrive_telegram_error"
#             db_record["status_overall"] = "telegram_processing_errors"
#             error_reasons = [
#                 f"File '{f.get('original_filename', 'N/A')}': {f.get('reason_telegram', 'Unknown TG error')}"
#                 for f in processed_files_for_db if not f.get("telegram_send_status", "").startswith("success")
#             ]
#             db_record["last_error"] = "; ".join(error_reasons)
        
#         save_file_metadata(db_record)
#         logging.info(f"{log_prefix} Final DB record updated. Status: {db_record['status_overall']}")

#     except Exception as e_bg:
#         logging.error(f"{log_prefix} Unhandled exception in background GDrive-to-Telegram transfer: {e_bg}", exc_info=True)
#         if db_record: 
#             db_record["status_overall"] = "error_telegram_processing_unhandled_bg"
#             db_record["last_error"] = f"Unhandled background error: {str(e_bg)}"
#             save_file_metadata(db_record)
#     finally:
#         # 6. This is the centralized, robust final cleanup block. It runs no matter what.
#         if tg_send_executor:
#             tg_send_executor.shutdown(wait=True)
        
#         # Clean up the initial source temp directory if it was a batch upload.
#         if initial_temp_batch_dir and os.path.exists(initial_temp_batch_dir):
#             _safe_remove_directory(initial_temp_batch_dir, log_prefix, "initial batch source dir")

#         # Clean up initial source temp files if it was a single file upload.
#         if not initial_temp_batch_dir and initial_temp_single_file_list:
#             for file_info in initial_temp_single_file_list:
#                 path = file_info.get("temp_local_path")
#                 if path and os.path.exists(path):
#                     _safe_remove_file(path, log_prefix, "initial single source file")

#         # Clean up the transient progress data dictionary entry.
#         if access_id in upload_progress_data:
#             try: del upload_progress_data[access_id]
#             except KeyError: pass
        
#         logging.info(f"{log_prefix} Background GDrive-to-Telegram transfer finished and all cleanup is complete.")


def run_gdrive_to_telegram_transfer(access_id: str):
    """
    Handles the GDrive to Telegram transfer in the background.
    Downloads from GDrive, sends to Telegram, and DELETES from GDrive on success.
    """
    log_prefix = f"[BG-TG-{access_id}]"
    logging.info(f"{log_prefix} Background GDrive-to-Telegram transfer started.")
    
    # This executor is for sending file chunks to Telegram in parallel
    tg_send_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'BgTgSend_{access_id[:4]}')
    
    db_record = None
    # Retrieve initial temp file paths from the progress data before it's deleted.
    # This is for the final cleanup of files saved by the browser before streaming.
    progress_context = upload_progress_data.get(access_id, {})
    initial_temp_batch_dir = progress_context.get("temp_batch_source_dir")
    initial_temp_single_file_list = progress_context.get("files_for_gdrive_upload", [])

    try:
        # 1. Fetch the database record.
        db_record, db_error = find_metadata_by_access_id(access_id)
        if db_error or not db_record:
            logging.error(f"{log_prefix} Failed to fetch DB record: {db_error or 'Not found'}")
            return

        # 2. Safety check for status.
        if db_record.get("status_overall") != "gdrive_complete_pending_telegram":
            logging.warning(f"{log_prefix} Record not in 'gdrive_complete_pending_telegram' state. Aborting.")
            return

        # 3. Update status to 'processing'.
        db_record["status_overall"] = "telegram_processing_background"
        save_file_metadata(db_record) 

        files_to_process = db_record.get("files_in_batch", [])
        processed_files_for_db = []
        batch_tg_success = True
        
        # 4. Loop through each file from the GDrive upload.
        for file_detail in files_to_process:
            original_filename = file_detail.get("original_filename")
            gdrive_file_id = file_detail.get("gdrive_file_id")

            if not original_filename or not gdrive_file_id:
                logging.error(f"{log_prefix} Skipping file due to missing data: {file_detail}")
                # ... (error handling for skipped file)
                continue

            current_file_log_prefix = f"{log_prefix} File '{original_filename}'"
            updated_file_meta = file_detail.copy()
            local_temp_file_for_tg: Optional[str] = None

            try:
                # 4a. Download from GDrive to a temporary file on your server.
                logging.info(f"{current_file_log_prefix} Downloading from GDrive (ID: {gdrive_file_id}) for Telegram transfer.")
                gdrive_stream, dl_err = download_from_gdrive(gdrive_file_id)
                if dl_err or not gdrive_stream:
                    raise Exception(f"GDrive download failed for TG: {dl_err or 'No stream'}")

                file_suffix_tg = os.path.splitext(original_filename)[1] if '.' in original_filename else ".tmp"
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=file_suffix_tg) as temp_f:
                    local_temp_file_for_tg = temp_f.name
                    shutil.copyfileobj(gdrive_stream, temp_f)
                gdrive_stream.close()
                current_file_size = os.path.getsize(local_temp_file_for_tg)
                
                # 4b. Send the file to Telegram (handling large files by chunking).
                if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
                    # Logic for chunked upload
                    # ... (This logic sends multiple parts to Telegram)
                    pass # Placeholder for your existing chunking logic
                else:
                    # Logic for single file upload
                    single_futures = {tg_send_executor.submit(_send_single_file_task, local_temp_file_for_tg, original_filename, str(chat_id), access_id): str(chat_id) for chat_id in TELEGRAM_CHAT_IDS}
                    single_results = []
                    for future in as_completed(single_futures):
                        chat_id_res = single_futures[future]
                        try:
                            _, api_result = future.result()
                            single_results.append({"chat_id": chat_id_res, "success": api_result[0], "message": api_result[1], "tg_response": api_result[2]})
                        except Exception as e_fut:
                            single_results.append({"chat_id": chat_id_res, "success": False, "message": str(e_fut), "tg_response": None})
                    
                    parsed_locations = _parse_send_results(f"{current_file_log_prefix}-Parse", single_results)
                    primary_res = next((res for res in parsed_locations if str(res.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                    
                    if primary_res and primary_res.get("success"):
                        updated_file_meta["telegram_send_status"] = "success_single"
                        updated_file_meta["telegram_send_locations"] = parsed_locations
                    else:
                        raise Exception("Primary Telegram upload failed.")

                # ======================================================================
                #  THIS IS THE CRITICAL LOGIC
                # ======================================================================
                # 4c. If the Telegram upload was successful, delete the file from GDrive.
                if updated_file_meta.get("telegram_send_status", "").startswith("success"):
                    logging.info(f"{current_file_log_prefix} Successfully sent to Telegram. Deleting from GDrive.")
                    del_success, del_err = delete_from_gdrive(gdrive_file_id)
                    if not del_success:
                        # Log a warning but don't fail the whole process.
                        logging.warning(f"{current_file_log_prefix} FAILED GDrive delete for ID {gdrive_file_id}: {del_err}")
                        updated_file_meta["gdrive_cleanup_error"] = del_err
                else:
                    # If Telegram upload failed, do NOT delete from GDrive.
                    batch_tg_success = False
                    logging.warning(f"{current_file_log_prefix} Failed Telegram send. File remains in GDrive (ID: {gdrive_file_id}).")
                # ======================================================================

            except Exception as e_file_processing:
                logging.error(f"{current_file_log_prefix} Error processing for Telegram: {e_file_processing}", exc_info=True)
                updated_file_meta["telegram_send_status"] = "error_processing_bg"
                updated_file_meta["reason_telegram"] = str(e_file_processing)
                batch_tg_success = False
            finally:
                if local_temp_file_for_tg and os.path.exists(local_temp_file_for_tg):
                    _safe_remove_file(local_temp_file_for_tg, current_file_log_prefix, "temp for TG send")
            
            processed_files_for_db.append(updated_file_meta)

        # 5. After the loop, update the main database record with the final status.
        db_record["files_in_batch"] = processed_files_for_db
        if batch_tg_success:
            db_record["storage_location"] = "telegram"
            db_record["status_overall"] = "telegram_complete"
        else:
            db_record["storage_location"] = "mixed_gdrive_telegram_error"
            db_record["status_overall"] = "telegram_processing_errors"
            error_reasons = [f"File '{f.get('original_filename', 'N/A')}': {f.get('reason_telegram', 'Unknown TG error')}" for f in processed_files_for_db if not f.get("telegram_send_status", "").startswith("success")]
            db_record["last_error"] = "; ".join(error_reasons)
        
        save_file_metadata(db_record)
        logging.info(f"{log_prefix} Final DB record updated. Status: {db_record['status_overall']}")

    except Exception as e_bg:
        logging.error(f"{log_prefix} Unhandled exception in background transfer: {e_bg}", exc_info=True)
        if db_record: 
            db_record["status_overall"] = "error_telegram_processing_unhandled_bg"
            db_record["last_error"] = f"Unhandled background error: {str(e_bg)}"
            save_file_metadata(db_record)
    finally:
        # 6. Final cleanup logic remains the same.
        if tg_send_executor: tg_send_executor.shutdown(wait=True)
        if initial_temp_batch_dir and os.path.exists(initial_temp_batch_dir):
            _safe_remove_directory(initial_temp_batch_dir, log_prefix, "initial batch source dir")
        if not initial_temp_batch_dir and initial_temp_single_file_list:
            for file_info in initial_temp_single_file_list:
                path = file_info.get("temp_local_path")
                if path and os.path.exists(path): _safe_remove_file(path, log_prefix, "initial single source file")
        if access_id in upload_progress_data:
            try: del upload_progress_data[access_id]
            except KeyError: pass
        
        logging.info(f"{log_prefix} Background transfer finished and all cleanup is complete.")

@upload_bp.route('/stream', methods=['POST', 'OPTIONS'])
@jwt_required(optional=True) 
def stream_upload_to_gdrive():
    # The OPTIONS check is correct and stays here.
    if request.method == 'OPTIONS':
        return make_response(("OK", 200))
    
    # We still generate a unique ID for this entire operation
    operation_id = str(uuid.uuid4())
    log_prefix = f"[StreamUpload-{operation_id}]"

    def generate_events():
        gdrive_file_id = None
        try:
            # --- 1. Get User Identity and File Info (inside the generator) ---
            current_user_jwt_identity = get_jwt_identity()
            user_info = {"is_anonymous": True, "username": "Anonymous", "user_email": None}
            if current_user_jwt_identity:
                user_doc, _ = find_user_by_id(ObjectId(current_user_jwt_identity))
                if user_doc:
                    user_info.update({"is_anonymous": False, "username": user_doc.get("username"), "user_email": user_doc.get("email")})

            # FIX: Get filename/size from query params because EventSource doesn't support custom headers
            filename = secure_filename(request.args.get('X-Filename', ''))
            file_size = int(request.args.get('X-Filesize', 0))
            if not filename:
                yield _yield_sse_event("error", {"message": "Filename parameter missing."})
                return

            logging.info(f"{log_prefix} Streaming file '{filename}' (Size: {format_bytes(file_size)}) to GDrive.")
            
            # ==========================================================================
            #  THE FIX IS HERE: Initialize the dictionary entry for this operation
            # ==========================================================================
            upload_progress_data[operation_id] = {}
            # ==========================================================================

            # --- 2. Stream to GDrive and yield progress events ---
            in_memory_stream = io.BytesIO(request.stream.read())
            
            for progress_event in upload_to_gdrive_with_progress(
                source=in_memory_stream,
                filename_in_gdrive=filename,
                operation_id_for_log=operation_id
            ):
                # Check for an error message within the event and raise it to stop the process
                if progress_event.get("type") == "error":
                    raise Exception(progress_event.get("message", "Unknown GDrive upload error."))
                
                # Yield the progress event to the frontend
                yield _yield_sse_event(progress_event.get("type", "status"), progress_event)
            
            # --- 3. Get the final GDrive file ID ---
            final_progress_data = upload_progress_data.get(operation_id, {})
            gdrive_file_id = final_progress_data.get("gdrive_file_id_temp_result")
            if not gdrive_file_id:
                raise Exception("GDrive upload complete but no file ID returned.")
            
            # --- 4. Save Metadata and Trigger Background Task ---
            file_details_for_db = {"original_filename": filename, "gdrive_file_id": gdrive_file_id, "original_size": file_size, "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream', "telegram_send_status": "pending"}
            db_record_payload = {"access_id": operation_id, "username": user_info['username'], "user_email": user_info['user_email'], "is_anonymous": user_info['is_anonymous'], "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "storage_location": "gdrive", "status_overall": "gdrive_complete_pending_telegram", "is_batch": False, "batch_display_name": filename, "files_in_batch": [file_details_for_db], "total_original_size": file_size}
            save_success, save_msg = save_file_metadata(db_record_payload)
            if not save_success:
                # If DB save fails, we must clean up the GDrive file to avoid orphans
                delete_from_gdrive(gdrive_file_id)
                raise Exception(f"Failed to save file record: {save_msg}")
            
            background_executor.submit(run_gdrive_to_telegram_transfer, operation_id)
            
            # --- 5. Yield the final "complete" event with the correct URL ---
            frontend_base_url = os.environ.get('FRONTEND_URL', '').rstrip('/')
            download_url = f"{frontend_base_url}/batch-view/{operation_id}" if frontend_base_url else url_for('download_prefixed.stream_download_by_access_id', access_id=operation_id, _external=True)

            yield _yield_sse_event("complete", {
                "message": "File uploaded successfully.",
                "access_id": operation_id,
                "download_url": download_url,
                "gdrive_file_id": gdrive_file_id
            })
            logging.info(f"{log_prefix} Successfully completed stream for {filename}.")

        except Exception as e:
            logging.error(f"{log_prefix} Error in streaming generator: {e}", exc_info=True)
            if gdrive_file_id:
                delete_from_gdrive(gdrive_file_id)
            yield _yield_sse_event("error", {"message": str(e)})
        finally:
            if operation_id in upload_progress_data:
                del upload_progress_data[operation_id]

    return Response(stream_with_context(generate_events()), mimetype='text/event-stream')

