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
from database import find_metadata_by_access_id
from .utils import _yield_sse_event, _calculate_progress, _safe_remove_directory, _safe_remove_file
import tempfile
from google_drive_api import download_from_gdrive, delete_from_gdrive, upload_to_gdrive_with_progress
from flask_jwt_extended import jwt_required, get_jwt_identity
from bson import ObjectId

from database import User, find_user_by_id, save_file_metadata
from extensions import upload_progress_data
from config import (
    TELEGRAM_CHAT_IDS, PRIMARY_TELEGRAM_CHAT_ID, CHUNK_SIZE,
    UPLOADS_TEMP_DIR, MAX_UPLOAD_WORKERS, TELEGRAM_MAX_CHUNK_SIZE_BYTES,
    format_bytes # format_bytes is used by _send_single_file_task & _send_chunk_task
)
from telegram_api import send_file_to_telegram
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

# def _send_single_file_task(file_path: str, filename: str, chat_id: str, upload_id: str) -> Tuple[str, ApiResult]:
#     log_prefix = f"[{upload_id}] Task for '{filename}' to {chat_id}"
#     try:
#         with open(file_path, 'rb') as f_handle:
#             file_size = os.path.getsize(file_path) 
#             logging.info(f"{log_prefix} Sending single file ({format_bytes(file_size)}) from path: {file_path}")
#             result = send_file_to_telegram(f_handle, filename, chat_id)
#         logging.info(f"{log_prefix} Single file send result: Success={result[0]}")
#         return str(chat_id), result
#     except FileNotFoundError:
#          logging.error(f"{log_prefix} Single file not found at path: {file_path}")
#          return str(chat_id), (False, f"File not found: {filename}", None)
#     except Exception as e:
#         logging.error(f"{log_prefix} Unexpected error opening/sending single file: {e}", exc_info=True)
#         return str(chat_id), (False, f"Thread error processing single file: {e}", None)

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

# def _send_chunk_task(chunk_data: bytes, filename: str, chat_id: str, upload_id: str, chunk_num: int) -> Tuple[str, ApiResult]:
#     log_prefix = f"[{upload_id}] Chunk {chunk_num} ('{filename}') to {chat_id}"
#     try:
#         buffer = io.BytesIO(chunk_data)
#         logging.info(f"{log_prefix} Sending chunk ({format_bytes(len(chunk_data))})")
#         result = send_file_to_telegram(buffer, filename, chat_id)
#         buffer.close()
#         logging.info(f"{log_prefix} Send chunk result: Success={result[0]}")
#         return str(chat_id), result
#     except Exception as e: 
#         logging.error(f"{log_prefix} Unexpected error sending chunk: {e}", exc_info=True)
#         return str(chat_id), (False, f"Thread error processing chunk: {e}", None)

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

# @upload_bp.route('/sse/gdrive-upload-status/<operation_id>')
# def sse_gdrive_upload_status(operation_id: str) -> Response:
#     log_prefix_sse_gdrive = f"[SSE-GDrive-{operation_id}]"
#     logging.info(f"{log_prefix_sse_gdrive} SSE connection established for GDrive upload phase.")

#     def generate_gdrive_upload_events():
#         gdrive_temp_file_path_to_upload = None # Path to the file saved by initiate_upload
#         original_filename_for_gdrive = "unknown_file"
#         access_id_for_db = operation_id # The operation_id becomes the access_id for the DB record
#         gdrive_file_id_final = None # Initialize here
#         gdrive_upload_error_final = None

#         try:
#             # 1. Retrieve initial upload data stored by initiate_upload
#             initial_upload_data = upload_progress_data.get(operation_id)
#             if not initial_upload_data:
#                 logging.error(f"{log_prefix_sse_gdrive} No initial data found for operation_id.")
#                 yield _yield_sse_event("error", {"message": "Upload session not found or expired."})
#                 return

#             gdrive_temp_file_path_to_upload = initial_upload_data.get("gdrive_temp_file_path_source") # Key we'll set in initiate_upload
#             original_filename_for_gdrive = initial_upload_data.get("original_filename", "unknown_file")
#             display_username = initial_upload_data.get("username", "anonymous")
#             user_email = initial_upload_data.get("user_email")
#             is_anonymous = initial_upload_data.get("is_anonymous", True)
#             anonymous_id_form = initial_upload_data.get("anonymous_id") # Guest ID if provided
#             original_size_from_initiate = initial_upload_data.get("original_size", 0)
#             overall_batch_display_name_for_tg_phase = initial_upload_data.get("batch_display_name_overall", original_filename_for_gdrive)


#             if not gdrive_temp_file_path_to_upload or not os.path.exists(gdrive_temp_file_path_to_upload):
#                 logging.error(f"{log_prefix_sse_gdrive} Temporary file for GDrive upload not found at: {gdrive_temp_file_path_to_upload}")
#                 gdrive_upload_error_final = "Temporary file for upload is missing."
#                 yield _yield_sse_event("error", {"message": "Temporary file for upload is missing."})
#                 return
            
#             yield _yield_sse_event("status", {"message": f"Initializing GDrive upload for {original_filename_for_gdrive}..."})
#             yield _yield_sse_event("start", {"filename": original_filename_for_gdrive, "totalSize": original_size_from_initiate})


#             # 2. Call the generator function for GDrive upload
#             gdrive_file_id_final = None
#             gdrive_upload_error_final = None

#             # The upload_to_gdrive_with_progress is now a generator
#             for progress_event in upload_to_gdrive_with_progress(
#                 source=gdrive_temp_file_path_to_upload, # Pass the path to the local temp file
#                 filename_in_gdrive=original_filename_for_gdrive,
#                 operation_id_for_log=operation_id
#             ):
#                 if progress_event.get("type") == "progress":
#                     yield _yield_sse_event("progress", {
#                         "percentage": progress_event.get("percentage", 0),
#                         "bytesSent": int(original_size_from_initiate * (progress_event.get("percentage", 0) / 100.0)) if original_size_from_initiate > 0 else 0,
#                         "totalBytes": original_size_from_initiate
#                         # Speed and ETA are harder to calculate accurately here without more info from GDrive API
#                         # For now, focus on percentage.
#                     })
#                 elif progress_event.get("type") == "error":
#                     gdrive_upload_error_final = progress_event.get("message", "Unknown GDrive upload error")
#                     logging.error(f"{log_prefix_sse_gdrive} Error during GDrive upload generator: {gdrive_upload_error_final}")
#                     yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#                     break # Stop processing on error

#             # After the generator finishes, the result is in the exception's value for `return`
#             # This is a bit non-standard; let's adjust upload_to_gdrive_with_progress to yield a final status.
#             # For now, we assume if gdrive_upload_error_final is None, it might have succeeded.
#             # We need to get the gdrive_file_id.
#             # A better way: modify upload_to_gdrive_with_progress to yield a final 'result' event.

#             # --- Let's assume for now, the last state of gdrive_upload_error_final determines outcome ---
#             # --- and that upload_to_gdrive_with_progress will be modified to return ID or store it ---
#             # --- For this step, we'll simulate success if no error was yielded. ---
            
#             # This part will be expanded in Step D.2 (Initial DB Save & GDrive Link)
#             if gdrive_upload_error_final: # If an error was explicitly yielded and caught
#                 logging.error(f"{log_prefix_sse_gdrive} GDrive upload failed with explicit error: {gdrive_upload_error_final}")
#                 # SIMULATE GETTING GDRIVE FILE ID - THIS NEEDS PROPER IMPLEMENTATION
#                 # In a real scenario, upload_to_gdrive_with_progress should return this
#                 # or store it in upload_progress_data[operation_id]
                
#                 # Placeholder: Try to retrieve it if the generator was modified to store it
#                 # (This is a forward-looking placeholder assuming upload_to_gdrive_with_progress is updated)
#             else:
#                 updated_op_data = upload_progress_data.get(operation_id, {})
#                 gdrive_file_id_final = updated_op_data.get("gdrive_file_id_temp_result") 

#                 if gdrive_file_id_final: # This check is now more realistic
#                     logging.info(f"{log_prefix_sse_gdrive} GDrive upload appears successful. File ID: {gdrive_file_id_final}")
                    
#                     # --- STEP D.2 will go here: Initial DB Save ---
#                     # --- and yielding the 'gdrive_complete' event with GDrive download link ---
#                     # For now, just signal completion of this phase:
#                     yield _yield_sse_event("status", {"message": "GDrive upload complete. Preparing for background processing."})
                    
#                     # Prepare for the next phase (Telegram processing)
#                     # The `upload_progress_data` entry for `operation_id` should now be updated
#                     # by `upload_to_gdrive_with_progress` or here to include gdrive_file_id
#                     # and set status to 'initiated_telegram_processing'
#                     # so that the existing `/stream-progress/` can pick it up.
                    
#                     # This is where we'd save the initial MongoDB record (Task 3)
#                     # and then yield the initial GDrive download link (Task 4)
                    
#                     # Example (to be fully fleshed out in next step):
#                     initial_db_record = {
#                         "access_id": access_id_for_db,
#                         "username": display_username, "user_email": user_email, "is_anonymous": is_anonymous,
#                         "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#                         "storage_location": "gdrive",
#                         "status_overall": "gdrive_complete_pending_telegram",
#                         "is_batch": initial_upload_data.get("is_batch_concept", False), # True if multiple files were in original request
#                         "batch_display_name": initial_upload_data.get("batch_display_name_concept", original_filename_for_gdrive),
#                         "files_in_batch": [{ # If single file, this array has one entry
#                             "original_filename": original_filename_for_gdrive,
#                             "gdrive_file_id": gdrive_file_id_final,
#                             "original_size": original_size_from_initiate,
#                             "mime_type": mimetypes.guess_type(original_filename_for_gdrive)[0] or 'application/octet-stream',
#                             "telegram_send_status": "pending",
#                             # ... other fields ...
#                         }],
#                         "total_original_size": original_size_from_initiate,
#                     }
#                     if is_anonymous and anonymous_id_form:
#                         initial_db_record["anonymous_id_form"] = anonymous_id_form

#                     save_success, save_msg = save_file_metadata(initial_db_record)
#                     if save_success:
#                         logging.info(f"{log_prefix_sse_gdrive} Initial MongoDB record saved for {access_id_for_db} (GDrive stage).")
#                         # Provide a *conceptual* download URL; actual download route needs to be stateful
#                         gdrive_download_url = url_for('download.stream_download_by_access_id', access_id=access_id_for_db, _external=False) # Assuming /get/ leads to stream_download
                        
#                         yield _yield_sse_event("gdrive_complete", {
#                             "message": f"'{original_filename_for_gdrive}' is stored temporarily. Processing for final storage.",
#                             "download_url": gdrive_download_url, # This link should now work via GDrive initially
#                             "access_id": access_id_for_db,
#                             "filename": original_filename_for_gdrive
#                         })
#                         # Now, update the transient progress_data to trigger the Telegram phase
#                         # The existing /stream-progress/<access_id> will pick this up.
#                         current_progress_entry = upload_progress_data.get(access_id_for_db, {})
#                         current_progress_entry.update({
#                             "status": "initiated_telegram_processing",
#                             "access_id": access_id_for_db, # Critical
#                             "username": display_username,
#                             "batch_display_name": overall_batch_display_name_for_tg_phase,
#                             "is_anonymous": is_anonymous,
#                             "anonymous_id": anonymous_id_form,
#                             "start_time": initial_db_record["upload_timestamp"] # Use DB timestamp
#                         })
#                         # Remove the temporary GDrive ID as it's now handled via DB for the next stage
#                         if "gdrive_file_id_temp_result" in current_progress_entry:
#                             del current_progress_entry["gdrive_file_id_temp_result"]
#                         upload_progress_data[access_id_for_db] = current_progress_entry
#                     else:
#                         gdrive_upload_error_final = f"Failed to save initial record after GDrive upload: {save_msg}"
#                         logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
#                         yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#                         # Clean up GDrive file if DB save failed
#                         if gdrive_file_id_final:
#                             delete_from_gdrive(gdrive_file_id_final)
#                             logging.info(f"{log_prefix_sse_gdrive} Cleaned up GDrive file {gdrive_file_id_final} due to DB save failure.")


#                 else: # GDrive upload failed, gdrive_upload_error_final should be set
#                     gdrive_upload_error_final = "GDrive upload process completed, but file ID was not retrieved. Possible silent failure or internal issue."
#                     logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
#                     yield _yield_sse_event("error", {"message": gdrive_upload_error_final})

            
#         except Exception as e_gen:
#             gdrive_upload_error_final = f"Internal server error during GDrive upload phase: {str(e_gen)}"
#             logging.error(f"{log_prefix_sse_gdrive} Unhandled exception in GDrive upload SSE generator: {e_gen}", exc_info=True)
#             yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
        
#         finally:
#             # Clean up the very short-lived local temp file that was the source for GDrive upload
#             if gdrive_temp_file_path_to_upload and os.path.exists(gdrive_temp_file_path_to_upload):
#                 _safe_remove_file(gdrive_temp_file_path_to_upload, log_prefix_sse_gdrive, "short-lived source for GDrive upload")
#             op_data_at_finally = upload_progress_data.get(operation_id, {})
#             # Clean up the operation from upload_progress_data *if this stream is fully responsible and not handing off*
#             # For now, the handoff to the next SSE happens by setting its status.
#             # If GDrive upload failed or DB save failed, we should clean up progress_data here.
#             if gdrive_upload_error_final or op_data_at_finally.get("status") != "initiated_telegram_processing":
#                 if operation_id in upload_progress_data:
#                     del upload_progress_data[operation_id]
#                     logging.info(f"{log_prefix_sse_gdrive} Cleaned up progress data for {operation_id} due to GDrive phase error or incomplete handoff.")
            
#             logging.info(f"{log_prefix_sse_gdrive} SSE stream for GDrive upload phase ended.")

#     return Response(stream_with_context(generate_gdrive_upload_events()), mimetype='text/event-stream')

# @upload_bp.route('/sse/gdrive-upload-status/<operation_id>')
# def sse_gdrive_upload_status(operation_id: str) -> Response:
#     log_prefix_sse_gdrive = f"[SSE-GDrive-{operation_id}]"
#     logging.info(f"{log_prefix_sse_gdrive} SSE connection established for GDrive upload phase.")

#     def generate_gdrive_upload_events():
#         gdrive_temp_file_path_to_upload = None
#         gdrive_file_id_final = None
#         gdrive_upload_error_final = None

#         try:
#             initial_upload_data = upload_progress_data.get(operation_id)
#             if not initial_upload_data:
#                 logging.error(f"{log_prefix_sse_gdrive} No initial data for operation_id.")
#                 yield _yield_sse_event("error", {"message": "Upload session not found or expired."})
#                 return

#             gdrive_temp_file_path_to_upload = initial_upload_data.get("gdrive_temp_file_path_source")
#             original_filename_for_gdrive = initial_upload_data.get("original_filename", "unknown_file")
#             original_size_from_initiate = initial_upload_data.get("original_size", 0)
#             # User details for DB record
#             display_username = initial_upload_data.get("username", "anonymous")
#             user_email = initial_upload_data.get("user_email")
#             is_anonymous_user = initial_upload_data.get("is_anonymous", True)
#             anonymous_id_form_val = initial_upload_data.get("anonymous_id")
#             is_batch_overall_val = initial_upload_data.get("is_batch_overall", False)
#             batch_display_name_overall_val = initial_upload_data.get("batch_display_name_overall", original_filename_for_gdrive)


#             if not gdrive_temp_file_path_to_upload or not os.path.exists(gdrive_temp_file_path_to_upload):
#                 gdrive_upload_error_final = "Temporary file for GDrive upload is missing."
#                 logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
#                 yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#                 return

#             yield _yield_sse_event("status", {"message": f"Initializing temporary storage for {original_filename_for_gdrive}..."})
#             yield _yield_sse_event("start", {"filename": original_filename_for_gdrive, "totalSize": original_size_from_initiate})

#             # Call GDrive upload generator
#             # This generator is now expected to store the gdrive_file_id in upload_progress_data[operation_id]
#             for progress_event in upload_to_gdrive_with_progress(
#                 source=gdrive_temp_file_path_to_upload,
#                 filename_in_gdrive=original_filename_for_gdrive,
#                 operation_id_for_log=operation_id # This is crucial for upload_to_gdrive_with_progress to update the correct entry
#             ):
#                 if progress_event.get("type") == "progress":
#                     yield _yield_sse_event("progress", {
#                         "percentage": progress_event.get("percentage", 0),
#                         "bytesSent": int(original_size_from_initiate * (progress_event.get("percentage", 0) / 100.0)) if original_size_from_initiate > 0 else 0,
#                         "totalBytes": original_size_from_initiate
#                     })
#                 elif progress_event.get("type") == "error":
#                     gdrive_upload_error_final = progress_event.get("message", "Unknown GDrive upload error during stream.")
#                     logging.error(f"{log_prefix_sse_gdrive} Error from GDrive upload generator: {gdrive_upload_error_final}")
#                     yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#                     break # Stop processing on GDrive error

#             if gdrive_upload_error_final:
#                  # Error already yielded by the loop, just ensure cleanup happens
#                 pass
#             else:
#                 # GDrive upload generator finished without explicit error. Check for the ID.
#                 updated_op_data_after_gdrive = upload_progress_data.get(operation_id, {})
#                 gdrive_file_id_final = updated_op_data_after_gdrive.get("gdrive_file_id_temp_result")

#                 if gdrive_file_id_final:
#                     logging.info(f"{log_prefix_sse_gdrive} GDrive upload successful. File ID: {gdrive_file_id_final}")
                    
#                     # Create initial DB record
#                     db_record_payload = {
#                         "access_id": operation_id, # operation_id is the access_id
#                         "username": display_username, "user_email": user_email, "is_anonymous": is_anonymous_user,
#                         "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#                         "storage_location": "gdrive", # Initially stored in GDrive
#                         "status_overall": "gdrive_complete_pending_telegram", # Ready for TG background
#                         "is_batch": is_batch_overall_val,
#                         "batch_display_name": batch_display_name_overall_val,
#                         "files_in_batch": [{
#                             "original_filename": original_filename_for_gdrive,
#                             "gdrive_file_id": gdrive_file_id_final, # Storing GDrive ID
#                             "original_size": original_size_from_initiate,
#                             "mime_type": mimetypes.guess_type(original_filename_for_gdrive)[0] or 'application/octet-stream',
#                             "telegram_send_status": "pending", # For the background task
#                         }],
#                         "total_original_size": original_size_from_initiate,
#                     }
#                     if is_anonymous_user and anonymous_id_form_val:
#                         db_record_payload["anonymous_id_form"] = anonymous_id_form_val

#                     save_success, save_msg = save_file_metadata(db_record_payload)

#                     if save_success:
#                         logging.info(f"{log_prefix_sse_gdrive} Initial MongoDB record saved for {operation_id} (GDrive stage).")
                        
#                         # This URL should point to a route that can serve from GDrive or Telegram based on DB record
#                         download_url_for_client = url_for('download.stream_download_by_access_id', access_id=operation_id, _external=False) 
                        
#                         yield _yield_sse_event("gdrive_complete", {
#                             "message": f"'{original_filename_for_gdrive}' is temporarily stored and ready for download.",
#                             "download_url": download_url_for_client, 
#                             "access_id": operation_id,
#                             "filename": original_filename_for_gdrive
#                         })
                        
#                         # Trigger background Telegram transfer
#                         # The 'upload_progress_data' for operation_id is now mainly for the background task's context,
#                         # if it needs anything beyond what's in the DB.
#                         # Or, the background task can just take operation_id and fetch all from DB.
#                         logging.info(f"{log_prefix_sse_gdrive} Submitting Telegram transfer for {operation_id} to background executor.")
#                         background_executor.submit(run_gdrive_to_telegram_transfer, operation_id)
#                         # No more SSE events from this stream after gdrive_complete for this client.

#                     else:
#                         gdrive_upload_error_final = f"Failed to save initial DB record after GDrive upload: {save_msg}"
#                         logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
#                         yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#                         if gdrive_file_id_final:
#                             delete_from_gdrive(gdrive_file_id_final)
#                 else:
#                     gdrive_upload_error_final = "GDrive upload process completed, but file ID was not retrieved from shared state."
#                     logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
#                     yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
        
#         except Exception as e_gen:
#             gdrive_upload_error_final = f"Internal server error during GDrive upload SSE: {str(e_gen)}"
#             logging.error(f"{log_prefix_sse_gdrive} Unhandled exception in GDrive upload SSE: {e_gen}", exc_info=True)
#             yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#         finally:
#             if gdrive_temp_file_path_to_upload and os.path.exists(gdrive_temp_file_path_to_upload):
#                 _safe_remove_file(gdrive_temp_file_path_to_upload, log_prefix_sse_gdrive, "short-lived source for GDrive")
            
#             # If an error occurred, or if it completed successfully (gdrive_complete was sent),
#             # this specific SSE stream's job is done. The upload_progress_data entry for operation_id
#             # might be used by the background task, so we don't delete it here unconditionally.
#             # Let the background task clean it up or let it expire if not used.
#             # However, if GDrive upload itself failed catastrophically before handoff, clean it.
#             if gdrive_upload_error_final and operation_id in upload_progress_data:
#                  # Check if it was not handed off for background processing
#                 op_data_status = upload_progress_data.get(operation_id, {}).get("status")
#                 if op_data_status != "background_telegram_triggered": # A hypothetical status background task might set
#                     logging.info(f"{log_prefix_sse_gdrive} GDrive phase errored. Cleaning up progress data for {operation_id}.")
#                     del upload_progress_data[operation_id]

#             logging.info(f"{log_prefix_sse_gdrive} SSE stream for GDrive upload phase ended for client.")

#     return Response(stream_with_context(generate_gdrive_upload_events()), mimetype='text/event-stream')

# @upload_bp.route('/sse/gdrive-upload-status/<operation_id>')
# def sse_gdrive_upload_status(operation_id: str) -> Response:
#     log_prefix_sse_gdrive = f"[SSE-GDrive-{operation_id}]"
#     logging.info(f"{log_prefix_sse_gdrive} SSE connection established for GDrive upload phase.")

#     def generate_gdrive_upload_events():
#         # ... (initialization as before) ...
#         gdrive_temp_file_path_to_upload = None
#         gdrive_file_id_final = None
#         gdrive_upload_error_final = None

#         try:
#             initial_upload_data = upload_progress_data.get(operation_id)
#             # ... (rest of initial_upload_data retrieval and checks) ...
#             if not initial_upload_data:
#                 logging.error(f"{log_prefix_sse_gdrive} No initial data for operation_id.")
#                 yield _yield_sse_event("error", {"message": "Upload session not found or expired."})
#                 return

#             gdrive_temp_file_path_to_upload = initial_upload_data.get("gdrive_temp_file_path_source")
#             original_filename_for_gdrive = initial_upload_data.get("original_filename", "unknown_file")
#             original_size_from_initiate = initial_upload_data.get("original_size", 0)
#             display_username = initial_upload_data.get("username", "anonymous")
#             user_email = initial_upload_data.get("user_email")
#             is_anonymous_user = initial_upload_data.get("is_anonymous", True)
#             anonymous_id_form_val = initial_upload_data.get("anonymous_id")
#             is_batch_overall_val = initial_upload_data.get("is_batch_overall", False) # From initiate_upload
#             # Use batch_display_name_overall if it's a batch, otherwise original_filename
#             filename_for_gdrive_complete_event = initial_upload_data.get("batch_display_name_overall") if is_batch_overall_val else original_filename_for_gdrive


#             if not gdrive_temp_file_path_to_upload or not os.path.exists(gdrive_temp_file_path_to_upload):
#                 # ... (error handling) ...
#                 gdrive_upload_error_final = "Temporary file for GDrive upload is missing."
#                 logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
#                 yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#                 return

#             yield _yield_sse_event("status", {"message": f"Initializing temporary storage for {original_filename_for_gdrive}..."})
#             yield _yield_sse_event("start", {"filename": original_filename_for_gdrive, "totalSize": original_size_from_initiate})

#             for progress_event in upload_to_gdrive_with_progress(
#                 source=gdrive_temp_file_path_to_upload,
#                 filename_in_gdrive=original_filename_for_gdrive,
#                 operation_id_for_log=operation_id
#             ):
#                 # ... (yield progress or error as before) ...
#                 if progress_event.get("type") == "progress":
#                     yield _yield_sse_event("progress", {
#                         "percentage": progress_event.get("percentage", 0),
#                         "bytesSent": int(original_size_from_initiate * (progress_event.get("percentage", 0) / 100.0)) if original_size_from_initiate > 0 else 0,
#                         "totalBytes": original_size_from_initiate
#                     })
#                 elif progress_event.get("type") == "error":
#                     gdrive_upload_error_final = progress_event.get("message", "Unknown GDrive upload error during stream.")
#                     logging.error(f"{log_prefix_sse_gdrive} Error from GDrive upload generator: {gdrive_upload_error_final}")
#                     yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#                     break

#             if not gdrive_upload_error_final:
#                 updated_op_data_after_gdrive = upload_progress_data.get(operation_id, {})
#                 gdrive_file_id_final = updated_op_data_after_gdrive.get("gdrive_file_id_temp_result")

#                 if gdrive_file_id_final:
#                     logging.info(f"{log_prefix_sse_gdrive} GDrive upload successful. File ID: {gdrive_file_id_final}")
                    
#                     db_record_payload = {
#                         "access_id": operation_id,
#                         "username": display_username, "user_email": user_email, "is_anonymous": is_anonymous_user,
#                         "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#                         "storage_location": "gdrive",
#                         "status_overall": "gdrive_complete_pending_telegram",
#                         "is_batch": is_batch_overall_val, # Use the flag determined at initiate_upload
#                         "batch_display_name": filename_for_gdrive_complete_event, # Use the correct display name
#                         "files_in_batch": [{
#                             "original_filename": original_filename_for_gdrive,
#                             "gdrive_file_id": gdrive_file_id_final,
#                             "original_size": original_size_from_initiate,
#                             "mime_type": mimetypes.guess_type(original_filename_for_gdrive)[0] or 'application/octet-stream',
#                             "telegram_send_status": "pending",
#                         }],
#                         "total_original_size": original_size_from_initiate,
#                     }
#                     if is_anonymous_user and anonymous_id_form_val:
#                         db_record_payload["anonymous_id_form"] = anonymous_id_form_val

#                     save_success, save_msg = save_file_metadata(db_record_payload)

#                     if save_success:
#                         logging.info(f"{log_prefix_sse_gdrive} Initial MongoDB record saved for {operation_id} (GDrive stage).")
                        
#                         # --- MODIFICATION: Do NOT send backend download_url ---
#                         # The frontend will construct the /batch-view/ or /file-view/ link
#                         yield _yield_sse_event("gdrive_complete", {
#                             "message": f"'{filename_for_gdrive_complete_event}' is stored and ready. Archival to final storage in progress.",
#                             # "download_url": NO LONGER SENDING THIS FROM BACKEND HERE
#                             "access_id": operation_id, # Essential for frontend to build its route
#                             "filename": filename_for_gdrive_complete_event, # For display confirmation
#                             "is_batch": is_batch_overall_val # So frontend knows if it's a batch
#                         })
#                         # --- END MODIFICATION ---
                        
#                         logging.info(f"{log_prefix_sse_gdrive} Submitting Telegram transfer for {operation_id} to background executor.")
#                         background_executor.submit(run_gdrive_to_telegram_transfer, operation_id)
#                     else:
#                         # ... (DB save error handling as before) ...
#                         gdrive_upload_error_final = f"Failed to save initial DB record after GDrive upload: {save_msg}"
#                         logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
#                         yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#                         if gdrive_file_id_final:
#                             delete_from_gdrive(gdrive_file_id_final)
#                 else:
#                     gdrive_upload_error_final = "GDrive upload process completed, but GDrive file ID was not retrieved from shared state."
#                     logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
#                     yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
        
#         # ... (rest of try-except-finally as before) ...
#         except Exception as e_gen:
#             gdrive_upload_error_final = f"Internal server error during GDrive upload SSE: {str(e_gen)}"
#             logging.error(f"{log_prefix_sse_gdrive} Unhandled exception in GDrive upload SSE: {e_gen}", exc_info=True)
#             yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
#         finally:
#             if gdrive_temp_file_path_to_upload and os.path.exists(gdrive_temp_file_path_to_upload):
#                 _safe_remove_file(gdrive_temp_file_path_to_upload, log_prefix_sse_gdrive, "short-lived source for GDrive")
            
#             if gdrive_upload_error_final and operation_id in upload_progress_data:
#                 op_data_status = upload_progress_data.get(operation_id, {}).get("status")
#                 # Check if it was NOT successfully handed off (e.g. DB save failed AFTER gdrive_id was set)
#                 # or if the gdrive_complete event that triggers background task was not sent.
#                 # A simple check is if the status is still related to GDrive SSE phase.
#                 if op_data_status == "initiated_gdrive_upload_sse":
#                     logging.info(f"{log_prefix_sse_gdrive} GDrive phase errored before potential handoff. Cleaning up progress data for {operation_id}.")
#                     del upload_progress_data[operation_id]
#             elif not gdrive_upload_error_final and upload_progress_data.get(operation_id, {}).get("status") == "initiated_telegram_processing":
#                  # This status is set by the old flow, not the new background one directly.
#                  # If background task is triggered, this specific SSE stream is done.
#                  # The upload_progress_data for operation_id might be used by the background task.
#                  # For now, let the background task be responsible for its cleanup of upload_progress_data[operation_id]
#                  pass

#             logging.info(f"{log_prefix_sse_gdrive} SSE stream for GDrive upload phase ended for client.")

#     return Response(stream_with_context(generate_gdrive_upload_events()), mimetype='text/event-stream')

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
        temp_batch_source_dir = initial_upload_data.get("temp_batch_source_dir") # Will be None for single file

        if not files_to_upload_to_gdrive:
            gdrive_upload_error_final = "No files specified for GDrive upload in session data."
            logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
            yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
            return

        # User details for DB record (common for all files in this operation)
        display_username = initial_upload_data.get("username", "anonymous")
        user_email = initial_upload_data.get("user_email")
        is_anonymous_user = initial_upload_data.get("is_anonymous", True)
        anonymous_id_form_val = initial_upload_data.get("anonymous_id")
        is_batch_overall = initial_upload_data.get("is_batch_overall", False)
        batch_display_name_overall = initial_upload_data.get("batch_display_name_overall", "Uploaded Files")
        
        # Yield 'start' event for the whole operation (batch or single)
        total_operation_size = sum(f.get("original_size", 0) for f in files_to_upload_to_gdrive)
        yield _yield_sse_event("start", {"filename": batch_display_name_overall, "totalSize": total_operation_size})

        gdrive_files_details_for_db = [] # Collects details of successfully GDrive-uploaded files

        try:
            for index, file_to_upload_info in enumerate(files_to_upload_to_gdrive):
                current_local_temp_path = file_to_upload_info.get("temp_local_path")
                current_original_filename = file_to_upload_info.get("original_filename")
                current_original_size = file_to_upload_info.get("original_size", 0)

                if not current_local_temp_path or not os.path.exists(current_local_temp_path) or not current_original_filename:
                    logging.error(f"{log_prefix_sse_gdrive} Invalid file info or missing temp file for GDrive upload: {file_to_upload_info}")
                    gdrive_upload_error_final = f"Missing or invalid temporary file for {current_original_filename or 'unknown file'}."
                    # For a batch, we might decide to skip this file and continue, or fail the whole batch.
                    # For simplicity, let's assume if one temp file is bad, the batch GDrive phase fails here.
                    yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                    # Clean up already uploaded GDrive files if any, and the local temp batch dir
                    for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
                    return # Exit generator

                status_message = f"Storing {current_original_filename} ({index + 1}/{len(files_to_upload_to_gdrive)})..."
                yield _yield_sse_event("status", {"message": status_message})
                
                # Call GDrive upload generator for the current file
                current_file_gdrive_id = None
                current_file_gdrive_error = None

                for progress_event in upload_to_gdrive_with_progress(
                    source=current_local_temp_path,
                    filename_in_gdrive=current_original_filename,
                    operation_id_for_log=operation_id # For context logging within GDrive API
                ):
                    if progress_event.get("type") == "progress":
                        yield _yield_sse_event("progress", { # Progress is per-file for GDrive part
                            "percentage": progress_event.get("percentage", 0),
                            "bytesSent": int(current_original_size * (progress_event.get("percentage", 0) / 100.0)) if current_original_size > 0 else 0,
                            "totalBytes": current_original_size, # For this file
                            "currentFile": current_original_filename # Add context
                        })
                    elif progress_event.get("type") == "error":
                        current_file_gdrive_error = progress_event.get("message", f"Unknown GDrive upload error for {current_original_filename}")
                        logging.error(f"{log_prefix_sse_gdrive} Error from GDrive upload generator for {current_original_filename}: {current_file_gdrive_error}")
                        break # Stop processing this file on error
                
                if current_file_gdrive_error:
                    gdrive_upload_error_final = f"Failed GDrive upload for {current_original_filename}: {current_file_gdrive_error}"
                    yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                    for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
                    return

                # After successful GDrive upload for the current file, get its ID
                # upload_to_gdrive_with_progress should store it in initial_upload_data (upload_progress_data[operation_id])
                updated_op_data_after_current_gdrive = upload_progress_data.get(operation_id, {})
                current_file_gdrive_id = updated_op_data_after_current_gdrive.get("gdrive_file_id_temp_result")

                if not current_file_gdrive_id:
                    gdrive_upload_error_final = f"GDrive file ID not retrieved for {current_original_filename} after upload."
                    logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
                    yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                    for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
                    return
                
                logging.info(f"{log_prefix_sse_gdrive} GDrive upload successful for {current_original_filename}. File ID: {current_file_gdrive_id}")
                gdrive_files_details_for_db.append({
                    "original_filename": current_original_filename,
                    "gdrive_file_id": current_file_gdrive_id,
                    "original_size": current_original_size,
                    "mime_type": mimetypes.guess_type(current_original_filename)[0] or 'application/octet-stream',
                    "telegram_send_status": "pending", # For the background task
                })
                # Clean up the individual local temp file for this successfully GDrive-uploaded file
                _safe_remove_file(current_local_temp_path, log_prefix_sse_gdrive, f"temp source for GDrive: {current_original_filename}")
            
            # All files processed for GDrive upload
            if not gdrive_files_details_for_db: # Should not happen if loop ran and no errors
                gdrive_upload_error_final = "No files were successfully uploaded to GDrive."
                logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
                yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                return

            # Create ONE DB record for the entire operation (batch or single)
            db_record_payload = {
                "access_id": operation_id,
                "username": display_username, "user_email": user_email, "is_anonymous": is_anonymous_user,
                "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "storage_location": "gdrive",
                "status_overall": "gdrive_complete_pending_telegram",
                "is_batch": is_batch_overall, # Correctly reflects if original request was a batch
                "batch_display_name": batch_display_name_overall,
                "files_in_batch": gdrive_files_details_for_db, # List of all GDrive-uploaded files
                "total_original_size": sum(f.get("original_size", 0) for f in gdrive_files_details_for_db),
            }
            if is_anonymous_user and anonymous_id_form_val:
                db_record_payload["anonymous_id_form"] = anonymous_id_form_val

            save_success, save_msg = save_file_metadata(db_record_payload)
            if not save_success:
                gdrive_upload_error_final = f"Failed to save DB record after GDrive uploads: {save_msg}"
                logging.error(f"{log_prefix_sse_gdrive} {gdrive_upload_error_final}")
                yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
                for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
                return

            logging.info(f"{log_prefix_sse_gdrive} MongoDB record saved for {operation_id} (GDrive stage complete).")
            
            yield _yield_sse_event("gdrive_complete", {
                "message": f"'{batch_display_name_overall}' is stored and ready. Archival to final storage in progress.",
                "access_id": operation_id,
                "filename": batch_display_name_overall, # Display name for the whole operation
                "is_batch": is_batch_overall
            })
            
            logging.info(f"{log_prefix_sse_gdrive} Submitting Telegram transfer for {operation_id} to background executor.")
            background_executor.submit(run_gdrive_to_telegram_transfer, operation_id)

        except Exception as e_gen:
            gdrive_upload_error_final = f"Internal server error during GDrive upload SSE: {str(e_gen)}"
            logging.error(f"{log_prefix_sse_gdrive} Unhandled exception in GDrive upload SSE: {e_gen}", exc_info=True)
            yield _yield_sse_event("error", {"message": gdrive_upload_error_final})
            # Clean up any GDrive files uploaded so far in this failed batch
            for gd_detail in gdrive_files_details_for_db: delete_from_gdrive(gd_detail["gdrive_file_id"])
        
        finally:
            # Clean up the main batch temp directory if it was created
            if temp_batch_source_dir and os.path.exists(temp_batch_source_dir):
                _safe_remove_directory(temp_batch_source_dir, log_prefix_sse_gdrive, "batch source dir for GDrive")
            
            # If not handed off to background (e.g., GDrive error or DB save error), clean up progress data.
            # The background task is responsible for cleaning up upload_progress_data[operation_id] if it's successfully initiated.
            if gdrive_upload_error_final and operation_id in upload_progress_data:
                del upload_progress_data[operation_id]
                logging.info(f"{log_prefix_sse_gdrive} Cleaned up progress data for {operation_id} due to GDrive phase error.")

            logging.info(f"{log_prefix_sse_gdrive} SSE stream for GDrive upload phase ended for client.")

    return Response(stream_with_context(generate_gdrive_upload_events()), mimetype='text/event-stream')

# @upload_bp.route('/initiate-upload', methods=['POST'])
# @jwt_required(optional=True)
# def initiate_upload() -> Response:
#     upload_id = str(uuid.uuid4())
#     log_prefix = f"[{upload_id}]"
#     logging.info(f"{log_prefix} Request to initiate upload.")
    
#     current_user_jwt_identity = get_jwt_identity()
#     display_username: Optional[str] = None
#     user_email: Optional[str] = None
#     is_anonymous: bool = False
#     anonymous_id: Optional[str] = None
    
#     if current_user_jwt_identity:
#         is_anonymous = False
#         try:
#             user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
#             if error or not user_doc:
#                 logging.error(f"{log_prefix} User not found for JWT identity '{current_user_jwt_identity}'. Error: {error}")
#                 return jsonify({"error": "Invalid user token or user not found"}), 401 
#             # Use _ensure_username_in_user_doc if it's available and User class needs it
#             # For now, direct access, assuming User class handles missing username or it's guaranteed
#             user_object_from_jwt = User(user_doc) # This might fail if username is missing and User requires it
#             display_username = user_object_from_jwt.username
#             user_email = user_object_from_jwt.email
#             logging.info(f"{log_prefix} User identified via JWT: Username='{display_username}'")
#         except ValueError as ve: 
#             logging.error(f"{log_prefix} Failed to instantiate User for JWT identity '{current_user_jwt_identity}':{ve}")
#             return jsonify({"error": "User data inconsistency"}), 500
#         except Exception as e: 
#             logging.error(f"{log_prefix} Error processing JWT identity '{current_user_jwt_identity}': {e}", exc_info=True)
#             return jsonify({"error": "Server error processing authentication"}), 500
#     else:
#         is_anonymous = True
#         anonymous_id = request.form.get('anonymous_upload_id')
#         if not anonymous_id:
#             return jsonify({"error": "Missing required anonymous identifier for anonymous upload."}), 400
#         display_username = f"AnonymousUser-{anonymous_id[:6]}" 
#         user_email = None 
#         logging.info(f"{log_prefix} Anonymous upload identified by temp ID: {anonymous_id}")
    
#     if display_username is None: # Should not happen if logic above is correct
#         return jsonify({"error": "Internal server error processing user identity."}), 500
    
#     uploaded_files = request.files.getlist('files[]')
#     if not uploaded_files or all(not f.filename for f in uploaded_files):
#         return jsonify({"error": "No files selected or files are invalid"}), 400

#     batch_temp_dir = os.path.join(UPLOADS_TEMP_DIR, f"batch_{upload_id}")
#     original_filenames_in_batch = []
#     try:
#         os.makedirs(batch_temp_dir, exist_ok=True)
#         for file_storage_item in uploaded_files:
#             if file_storage_item and file_storage_item.filename:
#                 original_filename = file_storage_item.filename 
#                 individual_temp_file_path = os.path.join(batch_temp_dir, original_filename)
#                 file_parent_dir = os.path.dirname(individual_temp_file_path)
#                 if not os.path.exists(file_parent_dir):
#                     os.makedirs(file_parent_dir, exist_ok=True)
#                 file_storage_item.save(individual_temp_file_path)
#                 original_filenames_in_batch.append(original_filename)
#         if not original_filenames_in_batch:
#             _safe_remove_directory(batch_temp_dir, log_prefix, "empty batch temp dir")
#             return jsonify({"error": "No valid files were processed in the batch."}), 400

#         batch_display_name = f"{original_filenames_in_batch[0]} (+{len(original_filenames_in_batch)-1} others)" if len(original_filenames_in_batch) > 1 else original_filenames_in_batch[0]
            
#         progress_entry = {
#             "status": "initiated", "is_batch": True, "batch_directory_path": batch_temp_dir,
#             "original_filenames_in_batch": original_filenames_in_batch, "batch_display_name": batch_display_name, 
#             "username": display_username, "user_email": user_email, "is_anonymous": is_anonymous, 
#             "error": None, "start_time": time.time()
#         }
#         if is_anonymous and anonymous_id: progress_entry["anonymous_id"] = anonymous_id
            
#         upload_progress_data[upload_id] = progress_entry
#         return jsonify({"upload_id": upload_id, "filename": batch_display_name})
#     except Exception as e:
#         logging.error(f"{log_prefix} Error processing batch upload: {e}", exc_info=True)
#         _safe_remove_directory(batch_temp_dir, log_prefix, "failed batch temp dir")
#         if upload_id in upload_progress_data: del upload_progress_data[upload_id]
#         return jsonify({"error": f"Server error processing batch: {str(e)}"}), 500
    
# @upload_bp.route('/initiate-upload', methods=['POST'])
# @jwt_required(optional=True)
# def initiate_upload() -> Response:
#     upload_id = str(uuid.uuid4()) # This will be primarily for tracking the overall operation state
#     log_prefix = f"[{upload_id}]"
#     logging.info(f"{log_prefix} Request to initiate upload (target: Google Drive).")
    
#     current_user_jwt_identity = get_jwt_identity()
#     display_username: Optional[str] = None
#     user_email: Optional[str] = None
#     is_anonymous: bool = False
#     anonymous_id_form: Optional[str] = None # Renamed to avoid conflict with a potential gdrive_id
    
#     if current_user_jwt_identity:
#         is_anonymous = False
#         try:
#             user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
#             if error or not user_doc:
#                 logging.error(f"{log_prefix} User not found for JWT identity '{current_user_jwt_identity}'. Error: {error}")
#                 return jsonify({"error": "Invalid user token or user not found"}), 401
#             user_object_from_jwt = User(user_doc)
#             display_username = user_object_from_jwt.username
#             user_email = user_object_from_jwt.email
#             logging.info(f"{log_prefix} User identified via JWT: Username='{display_username}'")
#         except ValueError as ve: 
#             logging.error(f"{log_prefix} Failed to instantiate User for JWT identity '{current_user_jwt_identity}':{ve}")
#             return jsonify({"error": "User data inconsistency"}), 500
#         except Exception as e: 
#             logging.error(f"{log_prefix} Error processing JWT identity '{current_user_jwt_identity}': {e}", exc_info=True)
#             return jsonify({"error": "Server error processing authentication"}), 500
#     else:
#         is_anonymous = True
#         anonymous_id_form = request.form.get('anonymous_upload_id')
#         if not anonymous_id_form:
#             return jsonify({"error": "Missing required anonymous identifier for anonymous upload."}), 400
#         display_username = f"AnonymousUser-{anonymous_id_form[:6]}" 
#         user_email = None 
#         logging.info(f"{log_prefix} Anonymous upload identified by form temp ID: {anonymous_id_form}")
    
#     if display_username is None:
#         return jsonify({"error": "Internal server error processing user identity."}), 500
    
#     uploaded_files = request.files.getlist('files[]')
#     if not uploaded_files or all(not f.filename for f in uploaded_files):
#         return jsonify({"error": "No files selected or files are invalid"}), 400

#     # --- MODIFIED SECTION: Upload to Google Drive ---
#     # For batch uploads, we'll process each file and store its GDrive ID.
#     # The concept of a "batch_directory_path" locally is now removed for GDrive temp storage.
    
#     files_in_gdrive_details = [] # Store details of files successfully uploaded to GDrive

#     for file_storage_item in uploaded_files:
#         if file_storage_item and file_storage_item.filename:
#             original_filename = file_storage_item.filename
#             logging.info(f"{log_prefix} Processing file '{original_filename}' for GDrive upload.")

#             # Read file content into a BytesIO stream
#             file_stream = io.BytesIO()
#             file_storage_item.save(file_stream) # Saves to the BytesIO stream
#             file_stream.seek(0) # Reset stream position to the beginning

#             gdrive_file_id, upload_error = upload_to_gdrive(file_stream, original_filename)
#             file_stream.close() # Close the BytesIO stream

#             if upload_error or not gdrive_file_id:
#                 logging.error(f"{log_prefix} Failed to upload '{original_filename}' to Google Drive: {upload_error or 'No GDrive File ID returned'}")
#                 # Decide on error handling: skip this file, or fail the whole batch?
#                 # For now, let's try to continue with other files if possible, but this file won't be processed.
#                 # We could also return an immediate error to the user for this file.
#                 # For a robust system, you might collect all errors and report them.
#                 # Let's assume for now if one file fails GDrive upload, we might want to inform user and stop this specific file.
#                 # For simplicity of this step, we'll just log and it won't be added to files_in_gdrive_details.
#                 # A more complete implementation would provide feedback to the user about partial failures.
#                 continue # Skip to the next file if this one failed

#             files_in_gdrive_details.append({
#                 "original_filename": original_filename,
#                 "gdrive_file_id": gdrive_file_id,
#                 "size": len(file_stream.getvalue()) # Get size from stream before closing (or get from GDrive metadata if preferred)
#                 # We can get the exact size from GDrive metadata later if needed,
#                 # but having an approximate size here is fine.
#             })
#             logging.info(f"{log_prefix} Successfully uploaded '{original_filename}' to GDrive. ID: {gdrive_file_id}")
#         else:
#             logging.warning(f"{log_prefix} Skipped an invalid file item in the batch during GDrive upload.")

#     if not files_in_gdrive_details:
#         logging.warning(f"{log_prefix} No files were successfully uploaded to Google Drive for this batch.")
#         return jsonify({"error": "Failed to temporarily store any files. Please try again."}), 500

#     batch_display_name = f"{files_in_gdrive_details[0]['original_filename']} (+{len(files_in_gdrive_details)-1} others)" \
#                          if len(files_in_gdrive_details) > 1 else files_in_gdrive_details[0]['original_filename']
            
#     # Update progress_entry:
#     # - No more 'batch_directory_path'
#     # - 'files_in_gdrive_details' holds the list of {original_filename, gdrive_file_id, size}
#     progress_entry = {
#         "status": "initiated_gdrive", # New status to indicate GDrive upload done
#         "is_batch": True, # Still conceptually a batch operation
#         "files_in_gdrive_details": files_in_gdrive_details, # <--- NEW: List of GDrive file info
#         "batch_display_name": batch_display_name, 
#         "username": display_username, 
#         "user_email": user_email,     
#         "is_anonymous": is_anonymous, 
#         "error": None,
#         "start_time": time.time(),
#         "upload_id_op": upload_id # Store the operation ID for SSE linking
#     }
#     if is_anonymous and anonymous_id_form: # Use the form ID
#         progress_entry["anonymous_id"] = anonymous_id_form
            
#     upload_progress_data[upload_id] = progress_entry # Use the operation ID as key
#     logging.debug(f"{log_prefix} Initial progress data (post-GDrive) stored: {upload_progress_data[upload_id]}")
    
#     # The `upload_id` returned to the client is for the SSE stream to track this operation.
#     return jsonify({"upload_id": upload_id, "filename": batch_display_name})    

# @upload_bp.route('/initiate-upload', methods=['POST'])
# @jwt_required(optional=True)
# def initiate_upload() -> Response:
#     operation_id = str(uuid.uuid4())
#     log_prefix = f"[{operation_id}]"
#     logging.info(f"{log_prefix} Request to initiate upload (target: Google Drive).")
    
#     current_user_jwt_identity = get_jwt_identity()
#     display_username: Optional[str] = None
#     user_email: Optional[str] = None
#     is_anonymous: bool = False
#     anonymous_id_form: Optional[str] = None
    
#     if current_user_jwt_identity:
#         is_anonymous = False
#         try:
#             user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
#             if error or not user_doc:
#                 return jsonify({"error": "Invalid user token or user not found"}), 401
#             user_object_from_jwt = User(user_doc)
#             display_username = user_object_from_jwt.username
#             user_email = user_object_from_jwt.email
#         except Exception as e: 
#             return jsonify({"error": "Server error processing authentication"}), 500
#     else:
#         is_anonymous = True
#         anonymous_id_form = request.form.get('anonymous_upload_id')
#         if not anonymous_id_form:
#             return jsonify({"error": "Missing anonymous identifier."}), 400
#         display_username = f"AnonymousUser-{anonymous_id_form[:6]}"
    
#     if display_username is None: # Should technically be caught by logic above
#         logging.error(f"{log_prefix} Display username became None unexpectedly.")
#         return jsonify({"error": "Internal server error (user identity)."}), 500
    
#     uploaded_files = request.files.getlist('files[]')
#     if not uploaded_files or all(not f.filename for f in uploaded_files):
#         return jsonify({"error": "No files selected."}), 400
    
#     # files_for_db_gdrive_stage = []
#     # # files_in_gdrive_details = []

#     # for file_storage_item in uploaded_files:
#     #     if file_storage_item and file_storage_item.filename:
#     #         original_filename = file_storage_item.filename
#     #         logging.info(f"{log_prefix} Processing file '{original_filename}' for GDrive upload.")

#     #         file_stream = io.BytesIO()
#     #         file_storage_item.save(file_stream)
#     #         file_stream_size = len(file_stream.getvalue())
#     #         file_stream.seek(0)

#     #         # # --- Get the size BEFORE closing the stream ---
#     #         # file_size_in_bytes = len(file_stream.getvalue())
#     #         # # --- Rewind the stream again for upload_to_gdrive ---
#     #         # file_stream.seek(0) 

#     #         gdrive_file_id, upload_error = upload_to_gdrive(file_stream, original_filename)
            
#     #         # --- Close the stream AFTER all operations that need its content are done ---
#     #         file_stream.close() 

#     #         if upload_error or not gdrive_file_id:
#     #             logging.error(f"{log_prefix} Failed to upload '{original_filename}' to Google Drive: {upload_error or 'No GDrive File ID returned'}")
#     #             # If a GDrive upload fails, we should probably fail the whole operation here
#     #             # or at least not include it in the successful list for DB record.
#     #             # For now, return an error for the whole batch if any GDrive upload fails.
#     #             return jsonify({"error": f"Failed to upload '{original_filename}' to temporary storage. Please try again."}), 500

#     #         # files_in_gdrive_details.append({
#     #         #     "original_filename": original_filename,
#     #         #     "gdrive_file_id": gdrive_file_id,
#     #         #     "size": file_size_in_bytes # Use the size obtained before closing
#     #         # })
#     #         # logging.info(f"{log_prefix} Successfully uploaded '{original_filename}' to GDrive. ID: {gdrive_file_id}")
#     #         files_for_db_gdrive_stage.append({
#     #             "original_filename": original_filename,
#     #             "gdrive_file_id": gdrive_file_id, # This is crucial for the background task
#     #             "original_size": file_stream_size,
#     #             "mime_type": mimetypes.guess_type(original_filename)[0] or 'application/octet-stream',
#     #             # Initial state for Telegram processing (will be updated later)
#     #             "is_split_for_telegram": False, 
#     #             "is_compressed_for_telegram": False, # Or True if original was already a zip
#     #             "telegram_send_status": "pending", # New status field
#     #             "telegram_send_locations": [],
#     #             "telegram_chunks": []
#     #         })
#     #         logging.info(f"{log_prefix} Successfully uploaded '{original_filename}' to GDrive. ID: {gdrive_file_id}")
#     #     else:
#     #         logging.warning(f"{log_prefix} Skipped an invalid file item in the batch during GDrive upload.")

#     # if not files_for_db_gdrive_stage:
#     #     logging.warning(f"{log_prefix} No files were successfully uploaded to Google Drive for this batch.")
#     #     return jsonify({"error": "Failed to temporarily store any files. Please try again."}), 500

#     # batch_display_name = f"{files_for_db_gdrive_stage[0]['original_filename']} (+{len(files_for_db_gdrive_stage)-1} others)" \
#     #                  if len(files_for_db_gdrive_stage) > 1 else files_for_db_gdrive_stage[0]['original_filename']
            
#     # initial_db_record = {
#     #     "access_id": operation_upload_id, # This is the main ID for the whole operation/batch
#     #     "username": display_username,
#     #     "user_email": user_email,
#     #     "is_anonymous": is_anonymous,
#     #     "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time())), # GDrive upload completion time
#     #     "storage_location": "gdrive", # New field
#     #     "status_overall": "gdrive_complete_pending_telegram", # New overall status
#     #     "is_batch": len(files_for_db_gdrive_stage) > 1, # True if more than one file
#     #     "batch_display_name": batch_display_name,
#     #     "files_in_batch": files_for_db_gdrive_stage, # List of files with their GDrive IDs
#     #     "total_original_size": sum(f.get("original_size", 0) for f in files_for_db_gdrive_stage),
#     # }
#     # if is_anonymous and anonymous_id_form:
#     #     initial_db_record["anonymous_id_form"] = anonymous_id_form
            
#     # # upload_progress_data[upload_id] = progress_entry
#     # # logging.debug(f"{log_prefix} Initial progress data (post-GDrive) stored: {upload_progress_data[upload_id]}")
    
#     # # return jsonify({"upload_id": upload_id, "filename": batch_display_name})
    
#     # save_success, save_msg = save_file_metadata(initial_db_record)
#     # if not save_success:
#     #     logging.error(f"{log_prefix} CRITICAL: Files uploaded to GDrive, but FAILED to save initial MongoDB record: {save_msg}")
#     #     # This is a problematic state. Files are in GDrive but no DB record.
#     #     # Ideally, implement a cleanup for GDrive files here, or a retry mechanism for DB save.
#     #     # For now, return error to user.
#     #     # Consider deleting GDrive files if DB save fails to avoid orphaned GDrive files.
#     #     for gdrive_file_detail in files_for_db_gdrive_stage:
#     #         delete_from_gdrive(gdrive_file_detail["gdrive_file_id"]) # Attempt to clean up
#     #         logging.info(f"{log_prefix} Cleaned up GDrive file {gdrive_file_detail['gdrive_file_id']} due to DB save failure.")
#     #     return jsonify({"error": "Failed to record upload after storing to temporary space. Please try again."}), 500
    
#     # logging.info(f"{log_prefix} Initial MongoDB record saved for access_id: {operation_upload_id} (storage: gdrive)")

#     # # --- Populate upload_progress_data for the SSE stream (which now handles GDrive->Telegram) ---
#     # # The SSE stream will now primarily be for the GDrive->Telegram part.
#     # # The initial GDrive upload progress (Task 2) is NOT covered by this SSE stream yet.
#     # upload_progress_data[operation_upload_id] = {
#     #     "status": "initiated_telegram_processing", # New status, tells SSE to start TG part
#     #     "access_id": operation_upload_id, # Pass the DB record's access_id
#     #     "username": display_username, # For logging/context within the generator
#     #     "batch_display_name": batch_display_name, # For SSE 'start' event
#     #     # The generator will fetch files_from_gdrive_details from the DB record by access_id
#     #     "is_anonymous": is_anonymous, # Needed for Telegram processing context if any logic depends on it
#     #     "anonymous_id": anonymous_id_form if is_anonymous else None, # Pass along guest ID
#     #     "start_time": initial_db_record["upload_timestamp"] # Use the GDrive completion time as start for this phase
#     # }
#     # logging.debug(f"{log_prefix} Progress data for Telegram processing phase stored: {upload_progress_data[operation_upload_id]}")
    
#     # # Return the operation_upload_id (which is the access_id for the DB record and SSE)
#     # # The filename is for display on the frontend.
#     # return jsonify({"upload_id": operation_upload_id, "filename": batch_display_name})
#     if len(uploaded_files) > 1:
#         # TODO: Future enhancement - handle batch uploads with individual GDrive progress streams
#         # or a single stream that reports progress for each file sequentially to GDrive.
#         # For now, this example focuses on a single file for clarity of GDrive SSE progress.
#         # If you need to handle multiple files here, you'd loop, save each to a temp path,
#         # and then decide how to manage multiple SSE streams or one stream for all.
#         logging.warning(f"{log_prefix} Received multiple files; current GDrive SSE progress example handles one. Processing first.")
#         # Fallback: Process only the first file for this example to keep SSE simple.
#         # In a real scenario, you'd return an error or implement full batch GDrive progress.
#         # For now, let's simplify and just process the first file if multiple are sent.
#         # return jsonify({"error": "Batch GDrive upload with individual progress not yet fully implemented in this SSE step."}), 400
    
#     file_storage_item = uploaded_files[0]
#     original_filename = file_storage_item.filename
#     if not original_filename: # Should be caught by the check above, but good to be safe
#         return jsonify({"error": "Selected file has no name."}), 400
    
#     short_lived_temp_file_path = None
#     try:
#         # Save to a VERY temporary local file first. This file's lifecycle is tied to this request/SSE stream.
#         # UPLOADS_TEMP_DIR should be a directory cleaned periodically by a separate mechanism if files get orphaned.
#         # We use delete=False because the SSE stream needs to access it after this function returns.
#         # The SSE stream generator will be responsible for deleting it.
#         temp_file_descriptor, short_lived_temp_file_path = tempfile.mkstemp(
#             dir=UPLOADS_TEMP_DIR, 
#             prefix=f"{operation_id}_gdrive_src_", 
#             suffix=os.path.splitext(original_filename)[1]
#         )
#         os.close(temp_file_descriptor) # Close descriptor, we have the path
#         file_storage_item.save(short_lived_temp_file_path)
#         original_size = os.path.getsize(short_lived_temp_file_path)
#         logging.info(f"{log_prefix} User file '{original_filename}' saved to short-lived temp: {short_lived_temp_file_path}")

#         # Store info needed by the GDrive upload SSE stream
#         upload_progress_data[operation_id] = {
#             "status": "initiated_gdrive_upload_sse", 
#             "gdrive_temp_file_path_source": short_lived_temp_file_path,
#             "original_filename": original_filename, # This is the actual file being processed by GDrive SSE
#             "original_size": original_size,
#             "username": display_username,
#             "user_email": user_email,
#             "is_anonymous": is_anonymous,
#             "anonymous_id": anonymous_id_form,
#             # Store the name that represents the user's entire upload request:
#             "batch_display_name_overall": f"{original_filename} (+{len(uploaded_files)-1} others)" if len(uploaded_files) > 1 else original_filename,
#             "is_batch_overall": len(uploaded_files) > 1, # Was the initial request for multiple files?
#             "start_time_initiate": time.time()
#         }
        
#         sse_url = url_for('upload.sse_gdrive_upload_status', operation_id=operation_id, _external=False)
#         logging.info(f"{log_prefix} Returning operation_id for GDrive SSE stream: {sse_url}")
        
#         return jsonify({
#             "upload_id": operation_id, 
#             "filename": original_filename, # Or batch_display_name if handling multiple
#             "sse_gdrive_upload_url": sse_url
#         })

#     except Exception as e:
#         logging.error(f"{log_prefix} Error in initiate_upload before GDrive SSE: {e}", exc_info=True)
#         if short_lived_temp_file_path and os.path.exists(short_lived_temp_file_path):
#             _safe_remove_file(short_lived_temp_file_path, log_prefix, "orphaned short-lived GDrive source")
#         if operation_id in upload_progress_data:
#             del upload_progress_data[operation_id]
#         return jsonify({"error": f"Server error initiating upload: {str(e)}"}), 500

# @upload_bp.route('/initiate-upload', methods=['POST'])
# @jwt_required(optional=True)
# def initiate_upload() -> Response:
#     operation_id = str(uuid.uuid4())
#     log_prefix = f"[{operation_id}]"
#     logging.info(f"{log_prefix} Request to initiate GDrive upload stage.")
    
#     current_user_jwt_identity = get_jwt_identity()
#     display_username: Optional[str] = None
#     user_email: Optional[str] = None
#     is_anonymous: bool = False
#     anonymous_id_form: Optional[str] = None
    
#     if current_user_jwt_identity:
#         is_anonymous = False
#         try:
#             user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
#             if error or not user_doc:
#                 return jsonify({"error": "Invalid user token or user not found"}), 401
#             user_object_from_jwt = User(user_doc) # Assuming User class can handle user_doc
#             display_username = user_object_from_jwt.username
#             user_email = user_object_from_jwt.email
#         except Exception as e: 
#             logging.error(f"{log_prefix} Error processing JWT: {e}", exc_info=True)
#             return jsonify({"error": "Server error processing authentication"}), 500
#     else:
#         is_anonymous = True
#         anonymous_id_form = request.form.get('anonymous_upload_id')
#         if not anonymous_id_form:
#             return jsonify({"error": "Missing anonymous identifier."}), 400
#         display_username = f"AnonymousUser-{anonymous_id_form[:6]}"
    
#     if display_username is None:
#         return jsonify({"error": "Internal server error (user identity)."}), 500
    
#     uploaded_files = request.files.getlist('files[]')
#     if not uploaded_files or all(not f.filename for f in uploaded_files):
#         return jsonify({"error": "No files selected."}), 400
    
    
#     is_multi_file_upload = len(uploaded_files) > 1
#     files_for_gdrive_phase_data = []
#     batch_temp_dir_for_gdrive = None
    
#     # Current design: GDrive SSE handles one file at a time for progress.
#     # If multiple files, we process the first one for this SSE stream.
#     # Batch handling for GDrive progress would require a different SSE design.
#     if len(uploaded_files) > 1:
#         logging.warning(f"{log_prefix} Received {len(uploaded_files)} files; GDrive SSE will process the first one for progress display.")
    
#     file_storage_item = uploaded_files[0]
#     original_filename = file_storage_item.filename
#     if not original_filename:
#         return jsonify({"error": "Selected file has no name."}), 400
    
#     short_lived_temp_file_path = None
#     try:
#         temp_file_descriptor, short_lived_temp_file_path = tempfile.mkstemp(
#             dir=UPLOADS_TEMP_DIR, 
#             prefix=f"{operation_id}_gdrive_src_", 
#             suffix=os.path.splitext(original_filename)[1]
#         )
#         os.close(temp_file_descriptor)
#         file_storage_item.save(short_lived_temp_file_path)
#         original_size = os.path.getsize(short_lived_temp_file_path)
#         logging.info(f"{log_prefix} User file '{original_filename}' (size: {original_size}) saved to temp: {short_lived_temp_file_path}")

#         # Store all necessary info for sse_gdrive_upload_status AND for the subsequent background Telegram task
#         upload_progress_data[operation_id] = {
#             "status": "initiated_gdrive_upload_sse", # For the GDrive SSE stream
#             "gdrive_temp_file_path_source": short_lived_temp_file_path,
#             "original_filename": original_filename,
#             "original_size": original_size,
#             "username": display_username,
#             "user_email": user_email,
#             "is_anonymous": is_anonymous,
#             "anonymous_id": anonymous_id_form,
#             "batch_display_name_overall": f"{original_filename} (+{len(uploaded_files)-1} others)" if len(uploaded_files) > 1 else original_filename,
#             "is_batch_overall": len(uploaded_files) > 1,
#             "start_time_initiate": time.time(),
#             # gdrive_file_id_temp_result will be added by upload_to_gdrive_with_progress
#         }
        
#         sse_url = url_for('upload.sse_gdrive_upload_status', operation_id=operation_id, _external=False)
#         logging.info(f"{log_prefix} Returning GDrive SSE URL: {sse_url} for operation_id: {operation_id}")
        
#         return jsonify({
#             "upload_id": operation_id, 
#             "filename": original_filename, 
#             "sse_gdrive_upload_url": sse_url # Client connects to this first
#         })

#     except Exception as e:
#         logging.error(f"{log_prefix} Error in initiate_upload: {e}", exc_info=True)
#         if short_lived_temp_file_path and os.path.exists(short_lived_temp_file_path):
#             _safe_remove_file(short_lived_temp_file_path, log_prefix, "orphaned temp source")
#         if operation_id in upload_progress_data:
#             del upload_progress_data[operation_id]
#         return jsonify({"error": f"Server error initiating upload: {str(e)}"}), 500

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

            original_filename = file_storage_item.filename
            temp_local_path_for_this_file: str
            
            if is_multi_file_upload and batch_temp_dir_for_gdrive: # Should always be true if multi-file
                temp_local_path_for_this_file = os.path.join(batch_temp_dir_for_gdrive, original_filename)
            else: # Single file upload
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
            batch_display_name = f"{batch_display_name} (+{len(files_for_gdrive_phase_data)-1} others)"
        
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
   
# @upload_bp.route('/stream-progress/<upload_id>')
# def stream_progress(upload_id: str) -> Response:
#     logging.info(f"SSE connect request for upload_id: {upload_id}")
#     progress_entry = upload_progress_data.get(upload_id)
#     if not progress_entry:
#         # If not in transient data, it might have completed or errored out before this SSE connected,
#         # or it's an invalid ID.
#         logging.warning(f"Upload ID '{upload_id}' not found in active progress data. May be completed or invalid.")
#         # Optionally, you could check DB here if it makes sense for your flow,
#         # but for an active SSE stream, it should be in progress_data.
#         def stream_gen_not_found(): yield _yield_sse_event('error', {'message': f'Upload ID {upload_id} not found or processing already finished.'})
#         return Response(stream_with_context(stream_gen_not_found()), mimetype='text/event-stream')
    
#     status = progress_entry.get('status', 'unknown')
    
#     # Check if the status indicates it's ready for Telegram processing
#     if status == "initiated_telegram_processing":
#         logging.info(f"SSE for Telegram processing phase for upload_id: {upload_id}")
#         return Response(stream_with_context(process_upload_and_generate_updates(upload_id)), mimetype='text/event-stream')
#     elif status in ['completed', 'error', 'completed_metadata_error', 'completed_with_errors']:
#         logging.warning(f"Upload ID '{upload_id}' already finalized (Status:{status}). No further SSE stream.")
#         def stream_gen_finalized(): yield _yield_sse_event('error', {'message': f'Upload {upload_id} already finalized ({status}).'})
#         return Response(stream_with_context(stream_gen_finalized()), mimetype='text/event-stream')
#     elif status == "initiated_gdrive_upload_sse":
#         logging.warning(f"Upload ID '{upload_id}' is in GDrive SSE phase. Client should use GDrive SSE URL.")
#         def stream_gen_gdrive(): yield _yield_sse_event('error', {'message': f'Upload {upload_id} is in GDrive phase. Connect to GDrive SSE URL.'})
#         return Response(stream_with_context(stream_gen_gdrive()), mimetype='text/event-stream')
#     else:
#         logging.warning(f"Upload ID '{upload_id}' in unexpected state for Telegram SSE: {status}")
#         def stream_gen_unexpected(): yield _yield_sse_event('error', {'message': f'Upload {upload_id} in unexpected state: {status}.'})
#         return Response(stream_with_context(stream_gen_unexpected()), mimetype='text/event-stream')

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
    
    # This route is now OBSOLETE for Telegram processing if it's handled by `run_gdrive_to_telegram_transfer`
    # The client should not be connecting to this for the Telegram phase.
    # GDrive phase uses sse_gdrive_upload_status.
    # Telegram phase is background.
    # This SSE route might be repurposed for other progress if needed, or removed.
    
    # For now, let's assume if status is "initiated_telegram_processing" (old flow), it might still be called.
    # However, with the new flow, this status might not be set by sse_gdrive_upload_status anymore.
    # If `run_gdrive_to_telegram_transfer` is used, this `stream_progress` isn't for client's TG updates.

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

# def process_upload_and_generate_updates(upload_id_or_access_id: str) -> Generator[SseEvent, None, None]:
#     try:
#         executor: Optional[ThreadPoolExecutor] = None
#         log_prefix = f"[{upload_id_or_access_id}]" # Use the access_id for logging this phase
        
#         logging.info(f"{log_prefix} Starting GDrive-to-Telegram processing generator.")
        
#         db_record, db_error = find_metadata_by_access_id(upload_id_or_access_id)
        
        
#         upload_data = upload_progress_data.get(upload_id)

#         if not upload_data:
#             logging.error(f"{log_prefix} Critical: Upload data missing for operation ID.")
#             yield _yield_sse_event('error', {'message': 'Internal error: Upload data not found.'})
#             return

#         if upload_data.get('status') != 'initiated_gdrive':
#             logging.warning(f"{log_prefix} Process called in unexpected state: {upload_data.get('status')}. Expected 'initiated_gdrive'.")
#             if upload_data.get('status') not in ['processing_telegram', 'completed', 'completed_with_errors', 'error']:
#                 yield _yield_sse_event('error', {'message': f"Processing error: Invalid state '{upload_data.get('status')}'."})
#             return

#         username = upload_data['username']
#         files_from_gdrive_details = upload_data.get("files_in_gdrive_details", [])
#         batch_display_name = upload_data.get("batch_display_name", "Upload")
#         db_record_access_id: str = upload_data.get('access_id') # This is the access_id for the MongoDB record

#         if not upload_data.get("is_batch") or not files_from_gdrive_details:
#             logging.error(f"{log_prefix} Invalid batch data or no GDrive file details found.")
#             yield _yield_sse_event('error', {'message': 'Internal error: Invalid GDrive batch data.'})
#             upload_data['status'] = 'error'; upload_data['error'] = 'Invalid GDrive batch data'
#             return

#         if not db_record_access_id: # Should have been set in initiate_upload
#             logging.error(f"{log_prefix} Critical: db_record_access_id (for MongoDB) missing from upload_data.")
#             yield _yield_sse_event('error', {'message': 'Internal error: Missing record identifier.'})
#             upload_data['status'] = 'error'; upload_data['error'] = 'Missing record identifier'
#             return

#         upload_data['status'] = 'processing_telegram'

#         if TELEGRAM_CHAT_IDS and len(TELEGRAM_CHAT_IDS) > 0:
#             executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'TgUpload_{upload_id[:4]}')
#             logging.info(f"{log_prefix} Initialized Telegram Upload Executor (max={MAX_UPLOAD_WORKERS})")
#         else:
#             logging.error(f"{log_prefix} No Telegram chat IDs configured. Cannot upload.")
#             yield _yield_sse_event('error', {'message': 'Server configuration error: No destination chats.'})
#             upload_data['status'] = 'error'; upload_data['error'] = 'No destination chats configured.'
#             if executor: executor.shutdown() # Should not be initialized, but good practice
#             return

#         total_original_bytes_for_sse = sum(file_detail.get("size", 0) for file_detail in files_from_gdrive_details)
#         yield _yield_sse_event('start', {'filename': batch_display_name, 'totalSize': total_original_bytes_for_sse})
#         yield _yield_sse_event('status', {'message': f'Preparing to send {len(files_from_gdrive_details)} files to Telegram...'})

#         overall_telegram_processing_start_time = time.time()
#         bytes_processed_for_sse_progress = 0 # Tracks original file bytes processed for SSE progress
#         all_files_metadata_for_db_record = [] 
#         batch_overall_telegram_success = True 

#         for gdrive_file_detail in files_from_gdrive_details:
#             original_filename = gdrive_file_detail["original_filename"]
#             gdrive_file_id = gdrive_file_detail["gdrive_file_id"]
#             original_file_size = gdrive_file_detail.get("size", 0)
            
#             log_file_prefix_indiv = f"{log_prefix} File '{original_filename}' (GDriveID: {gdrive_file_id})"
#             logging.info(f"{log_file_prefix_indiv} Starting Telegram processing.")

#             current_file_tg_meta_entry: Dict[str, Any] = {
#                 "original_filename": original_filename, "original_size": original_file_size,
#                 "gdrive_file_id_source": gdrive_file_id, "is_split": False, "is_compressed": False,
#                 "skipped": False, "failed": False, "reason": None,
#                 "send_locations": [], "chunks": [],
#                 "mime_type": mimetypes.guess_type(original_filename)[0] or 'application/octet-stream'
#             }
            
#             local_temp_path_for_processing: Optional[str] = None

#             try:
#                 yield _yield_sse_event('status', {'message': f'Downloading "{original_filename}" from temp storage...'})
#                 logging.info(f"{log_file_prefix_indiv} Downloading from GDrive...")
                
#                 gdrive_content_stream, download_err = download_from_gdrive(gdrive_file_id)
#                 if download_err or not gdrive_content_stream:
#                     raise Exception(f"GDrive download error: {download_err or 'No content'}")

#                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=os.path.splitext(original_filename)[1]) as temp_file_on_disk:
#                     local_temp_path_for_processing = temp_file_on_disk.name
#                     shutil.copyfileobj(gdrive_content_stream, temp_file_on_disk)
#                 gdrive_content_stream.close()
#                 logging.info(f"{log_file_prefix_indiv} Saved GDrive content to local temp file: {local_temp_path_for_processing}")
                
#                 current_file_processing_size = os.path.getsize(local_temp_path_for_processing)
#                 if current_file_processing_size == 0:
#                     current_file_tg_meta_entry["skipped"] = True
#                     current_file_tg_meta_entry["reason"] = "File is empty after GDrive download."
#                     # Still add to metadata and update progress
#                     all_files_metadata_for_db_record.append(current_file_tg_meta_entry)
#                     bytes_processed_for_sse_progress += original_file_size # Mark original size as "processed"
#                     yield _yield_sse_event('progress', _calculate_progress(overall_telegram_processing_start_time, bytes_processed_for_sse_progress, total_original_bytes_for_sse))
#                     yield _yield_sse_event('status', {'message': f'Skipped empty file "{original_filename}" for Telegram.'})
#                     # Attempt to delete from GDrive even if skipped, as it was successfully retrieved
#                     delete_success_skipped, delete_error_skipped = delete_from_gdrive(gdrive_file_id)
#                     if not delete_success_skipped:
#                         logging.warning(f"{log_file_prefix_indiv} Failed to delete empty file from GDrive ID {gdrive_file_id}: {delete_error_skipped}")
#                     continue # Move to the next file


#                 # --- TELEGRAM SEND LOGIC ---
#                 # Determine if the file (from local_temp_path_for_processing) needs chunking for Telegram
#                 if current_file_processing_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
#                     current_file_tg_meta_entry["is_split"] = True # For Telegram context
#                     part_number = 1
#                     bytes_processed_for_this_file_tg_chunking = 0
#                     all_chunks_sent_successfully_for_this_file_tg = True

#                     with open(local_temp_path_for_processing, 'rb') as f_in_tg:
#                         while True:
#                             chunk_data = f_in_tg.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
#                             if not chunk_data: break
                            
#                             chunk_tg_filename = f"{original_filename}.part{part_number}"
#                             log_chunk_prefix_tg = f"{log_file_prefix_indiv} TG Chunk {part_number}"
                            
#                             # ... (Rest of the chunk sending logic using _send_chunk_task, executor, _parse_send_results) ...
#                             # This part is similar to your original process_upload_and_generate_updates
#                             # Ensure it updates primary_send_success_for_this_tg_chunk
#                             # and handles chunk_specific_tg_results.
#                             chunk_specific_tg_futures: Dict[Future, str] = {}
#                             chunk_specific_tg_results: Dict[str, ApiResult] = {}
#                             primary_send_success_for_this_tg_chunk = False
#                             primary_send_message_for_this_tg_chunk = "Primary TG chunk send failed."

#                             if executor:
#                                 for chat_id_str_loop_tg in TELEGRAM_CHAT_IDS:
#                                     fut_tg_chunk = executor.submit(_send_chunk_task, chunk_data, chunk_tg_filename, str(chat_id_str_loop_tg), upload_id, part_number)
#                                     chunk_specific_tg_futures[fut_tg_chunk] = str(chat_id_str_loop_tg)
#                             else: # Should not happen if executor is checked earlier
#                                 _, res_no_exec_tg_chunk = _send_chunk_task(chunk_data, chunk_tg_filename, str(TELEGRAM_CHAT_IDS[0]), upload_id, part_number)
#                                 chunk_specific_tg_results[str(TELEGRAM_CHAT_IDS[0])] = res_no_exec_tg_chunk
#                                 primary_send_success_for_this_tg_chunk, primary_send_message_for_this_tg_chunk = res_no_exec_tg_chunk[0], res_no_exec_tg_chunk[1]

#                             if chunk_specific_tg_futures:
#                                 primary_tg_chunk_fut = next((f for f, cid in chunk_specific_tg_futures.items() if cid == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
#                                 if primary_tg_chunk_fut:
#                                     cid_res_tg_chunk, res_tg_chunk = primary_tg_chunk_fut.result()
#                                     chunk_specific_tg_results[cid_res_tg_chunk] = res_tg_chunk
#                                     primary_send_success_for_this_tg_chunk, primary_send_message_for_this_tg_chunk = res_tg_chunk[0], res_tg_chunk[1]
#                                 for fut_completed_tg_chunk in as_completed(chunk_specific_tg_futures):
#                                     cid_res_tg_c, res_tg_c = fut_completed_tg_chunk.result()
#                                     if cid_res_tg_c not in chunk_specific_tg_results: chunk_specific_tg_results[cid_res_tg_c] = res_tg_c
                            
#                             parsed_tg_locations_for_this_chunk = _parse_send_results(log_chunk_prefix_tg, 
#                                 [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k, r in chunk_specific_tg_results.items()])

#                             # Check success from parsed results for primary chat
#                             primary_chunk_parsed_info = next((loc for loc in parsed_tg_locations_for_this_chunk if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                            
#                             if primary_chunk_parsed_info and primary_chunk_parsed_info.get("success"):
#                                 current_file_tg_meta_entry["chunks"].append({"part_number": part_number, "size": len(chunk_data), "send_locations": parsed_tg_locations_for_this_chunk})
#                                 # bytes_sent_to_telegram_so_far += len(chunk_data) # This tracks actual TG bytes
#                                 bytes_processed_for_this_file_tg_chunking += len(chunk_data)
#                                 # For SSE progress, we'll update it after the whole file is done based on original_file_size
#                                 yield _yield_sse_event('status', {'message': f'Sent TG chunk {part_number} for "{original_filename}"'})
#                             else:
#                                 error_reason_chunk_tg = primary_send_message_for_this_tg_chunk
#                                 if primary_chunk_parsed_info and primary_chunk_parsed_info.get('error'):
#                                     error_reason_chunk_tg = primary_chunk_parsed_info.get('error')
                                
#                                 logging.error(f"{log_chunk_prefix_tg} Failed. Reason: {error_reason_chunk_tg}. Aborting for this file.")
#                                 batch_overall_telegram_success = False
#                                 all_chunks_sent_successfully_for_this_file_tg = False
#                                 current_file_tg_meta_entry["failed"] = True
#                                 current_file_tg_meta_entry["reason"] = f"Failed TG chunk {part_number}: {error_reason_chunk_tg}"
#                                 current_file_tg_meta_entry["chunks"] = parsed_tg_locations_for_this_chunk # Store failure details
#                                 break 
#                             part_number += 1
                    
#                     if all_chunks_sent_successfully_for_this_file_tg:
#                         current_file_tg_meta_entry["telegram_total_chunked_size"] = bytes_processed_for_this_file_tg_chunking
                
#                 else: # SINGLE FILE to Telegram
#                     single_tg_file_futures: Dict[Future, str] = {}
#                     single_tg_file_results: Dict[str, ApiResult] = {}
#                     primary_send_success_for_single_tg_file = False
#                     primary_send_message_single_tg_file = "Primary TG send (single) failed."

#                     if executor:
#                         for chat_id_str_single_tg in TELEGRAM_CHAT_IDS:
#                             fut_single_tg = executor.submit(_send_single_file_task, local_temp_path_for_processing, original_filename, str(chat_id_str_single_tg), upload_id)
#                             single_tg_file_futures[fut_single_tg] = str(chat_id_str_single_tg)
#                     else: # Should not happen
#                         _, res_single_no_exec_tg = _send_single_file_task(local_temp_path_for_processing, original_filename, str(TELEGRAM_CHAT_IDS[0]), upload_id)
#                         single_tg_file_results[str(TELEGRAM_CHAT_IDS[0])] = res_single_no_exec_tg
#                         primary_send_success_for_single_tg_file, primary_send_message_single_tg_file = res_single_no_exec_tg[0], res_single_no_exec_tg[1]
                    
#                     if single_tg_file_futures:
#                         primary_fut_single_tg = next((f for f, cid in single_tg_file_futures.items() if cid == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
#                         if primary_fut_single_tg:
#                             cid_res_tg_s, res_tg_s = primary_fut_single_tg.result()
#                             single_tg_file_results[cid_res_tg_s] = res_tg_s
#                             primary_send_success_for_single_tg_file, primary_send_message_single_tg_file = res_tg_s[0], res_tg_s[1]
#                         for fut_completed_tg_s in as_completed(single_tg_file_futures):
#                             cid_res_tg_s_comp, res_tg_s_comp = fut_completed_tg_s.result()
#                             if cid_res_tg_s_comp not in single_tg_file_results: single_tg_file_results[cid_res_tg_s_comp] = res_tg_s_comp
                    
#                     parsed_tg_locations_single_file = _parse_send_results(f"{log_file_prefix_indiv}-TGSend", 
#                         [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k,r in single_tg_file_results.items()])

#                     primary_single_parsed_info = next((loc for loc in parsed_tg_locations_single_file if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)

#                     if primary_single_parsed_info and primary_single_parsed_info.get("success"):
#                         # bytes_sent_to_telegram_so_far += current_file_processing_size # This refers to local temp size
#                         current_file_tg_meta_entry["send_locations"] = parsed_tg_locations_single_file
#                     else:
#                         error_reason_single_tg = primary_send_message_single_tg_file
#                         if primary_single_parsed_info and primary_single_parsed_info.get("error"):
#                             error_reason_single_tg = primary_single_parsed_info.get("error")
                        
#                         batch_overall_telegram_success = False
#                         current_file_tg_meta_entry["failed"] = True
#                         current_file_tg_meta_entry["reason"] = f"Primary TG send failed: {error_reason_single_tg}"
#                         current_file_tg_meta_entry["send_locations"] = parsed_tg_locations_single_file
                
#                 # After processing this file for Telegram (chunked or single)
#                 if not current_file_tg_meta_entry["failed"] and not current_file_tg_meta_entry["skipped"]:
#                     bytes_processed_for_sse_progress += original_file_size # File processed successfully for TG
#                     logging.info(f"{log_file_prefix_indiv} Successfully processed for Telegram. Deleting from GDrive ID: {gdrive_file_id}")
#                     gdrive_delete_success, gdrive_delete_error = delete_from_gdrive(gdrive_file_id)
#                     if not gdrive_delete_success:
#                         logging.warning(f"{log_file_prefix_indiv} Failed to delete from GDrive ID {gdrive_file_id} after Telegram upload: {gdrive_delete_error}")
#                         current_file_tg_meta_entry["gdrive_cleanup_error"] = gdrive_delete_error
#                 else:
#                     bytes_processed_for_sse_progress += original_file_size # Still count as "processed" for SSE progress even if failed/skipped for TG
#                     logging.warning(f"{log_file_prefix_indiv} Not deleting from GDrive as Telegram processing failed or was skipped.")

#             except Exception as file_processing_exception:
#                 logging.error(f"{log_file_prefix_indiv} Error during GDrive download or Telegram prep: {file_processing_exception}", exc_info=True)
#                 current_file_tg_meta_entry["failed"] = True
#                 current_file_tg_meta_entry["reason"] = f"Internal error: {str(file_processing_exception)}"
#                 batch_overall_telegram_success = False
#                 bytes_processed_for_sse_progress += original_file_size # Count as processed for progress calculation
#             finally:
#                 if local_temp_path_for_processing and os.path.exists(local_temp_path_for_processing):
#                     _safe_remove_file(local_temp_path_for_processing, log_file_prefix_indiv, "local temp GDrive content file")
            
#             all_files_metadata_for_db_record.append(current_file_tg_meta_entry)
#             yield _yield_sse_event('progress', _calculate_progress(overall_telegram_processing_start_time, bytes_processed_for_sse_progress, total_original_bytes_for_sse))
#             yield _yield_sse_event('status', {'message': f'Processed {len(all_files_metadata_for_db_record)} of {len(files_from_gdrive_details)} files for Telegram...'})
        
#         total_batch_telegram_duration = time.time() - overall_telegram_processing_start_time
#         logging.info(f"{log_prefix} Finished all files processing for Telegram. Duration: {total_batch_telegram_duration:.2f}s. Overall TG Batch Success: {batch_overall_telegram_success}")

#         if not all_files_metadata_for_db_record:
#             logging.error(f"{log_prefix} CRITICAL: No Telegram metadata generated after processing loop.")
#             yield _yield_sse_event('error', {'message': 'Internal server error: Failed to record Telegram upload details.'})
#             upload_data['status'] = 'error'; upload_data['error'] = "No Telegram metadata generated"
#             if executor: executor.shutdown(wait=False)
#             return

#         final_db_record = {
#             "access_id": db_record_access_id, "username": username,
#             "is_anonymous": upload_data.get('is_anonymous', False), "anonymous_id": upload_data.get('anonymous_id'),
#             "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(upload_data.get('start_time', time.time()))), 
#             "telegram_processing_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#             "is_batch": True, "batch_display_name": batch_display_name, 
#             "files_in_batch": all_files_metadata_for_db_record,
#             "total_original_size": total_original_bytes_for_sse,
#             "total_telegram_processing_duration_seconds": round(total_batch_telegram_duration, 2),
#         }
#         if final_db_record["anonymous_id"] is None: del final_db_record["anonymous_id"]
        
#         save_success, save_msg = save_file_metadata(final_db_record)
#         if not save_success:
#             logging.error(f"{log_prefix} DB CRITICAL: Failed to save final Telegram batch metadata (access_id: {db_record_access_id}): {save_msg}")
#             upload_data['status'] = 'completed_metadata_error'; upload_data['error'] = f"DB save fail: {save_msg}"
#             yield _yield_sse_event('error', {'message': f"Upload processed, but failed to save final details: {save_msg}"})
#             if executor: executor.shutdown(wait=False)
#             return
#         logging.info(f"{log_prefix} DB: Successfully saved final Telegram batch metadata (access_id: {db_record_access_id}).")
        
#         is_single_effective_file = len(files_from_gdrive_details) == 1
#         final_sse_filename = batch_display_name
#         # The URL should ideally point to a page where the user can see/download the result.
#         # For a single file, it could be a direct preview/download page.
#         # For a batch, it's likely a page listing all files in the batch.
#         if is_single_effective_file:
#             # If your /get/<access_id> route serves a download page that then uses SSE for actual download:
#             browser_url = f"{request.host_url.rstrip('/')}/get/{db_record_access_id}" 
#             final_sse_filename = all_files_metadata_for_db_record[0].get('original_filename', batch_display_name)
#         else:
#             browser_url = f"{request.host_url.rstrip('/')}/batch-view/{db_record_access_id}" # Assuming you have such a route

#         complete_message_text = f'"{final_sse_filename}" processed for Telegram. ' + \
#                         ('Some files may have errors.' if not batch_overall_telegram_success else 'All files processed successfully!')
        
#         complete_payload = {
#             'message': complete_message_text, 'download_url': browser_url, 
#             'filename': final_sse_filename, 'access_id': db_record_access_id, 'batch_access_id': db_record_access_id,
#             'is_batch': not is_single_effective_file,
#         }
        
#         upload_data['status'] = 'completed_with_errors' if not batch_overall_telegram_success else 'completed'
#         logging.info(f"{log_prefix} Yielding '{upload_data['status']}' event. Payload: {json.dumps(complete_payload)}")
#         yield _yield_sse_event('complete', complete_payload)
    
#     except Exception as e:
#         error_msg_final = f"Critical GDrive/Telegram processing error: {str(e) or type(e).__name__}"
#         logging.error(f"{log_prefix} UNHANDLED EXCEPTION in GDrive/TG processing generator: {e}", exc_info=True)
#         yield _yield_sse_event('error', {'message': error_msg_final})
#         if upload_id in upload_progress_data: 
#             upload_progress_data[upload_id]['status'] = 'error'
#             upload_progress_data[upload_id]['error'] = error_msg_final
                
#     finally:
#         logging.info(f"{log_prefix} GDrive/TG processing generator final cleanup.")
#         if executor:
#             executor.shutdown(wait=True)
#             logging.info(f"{log_prefix} Telegram Upload executor shutdown (waited).")
        
#         final_status_report = 'unknown (upload_data missing)'
#         if upload_id in upload_progress_data and upload_progress_data[upload_id]: # Check if key exists and value is not None
#             final_status_report = upload_progress_data[upload_id].get('status', 'unknown (status key missing)')
        
#         logging.info(f"{log_prefix} GDrive/TG processing generator finished. Final Status: {final_status_report}")

# def process_upload_and_generate_updates(upload_id_or_access_id: str) -> Generator[SseEvent, None, None]:
#     # Note: upload_id_or_access_id is the access_id of the record in MongoDB
#     try:
#         executor: Optional[ThreadPoolExecutor] = None
#         log_prefix = f"[{upload_id_or_access_id}]" # Use the access_id for logging this phase
#         db_record = None
        
#         logging.info(f"{log_prefix} Starting GDrive-to-Telegram processing generator.")
        
#         # Fetch the initial record from MongoDB
#         db_record, db_error = find_metadata_by_access_id(upload_id_or_access_id)

#         if db_error or not db_record:
#             logging.error(f"{log_prefix} Failed to fetch MongoDB record for processing: {db_error or 'Record not found'}")
#             yield _yield_sse_event('error', {'message': f"Internal error: Could not retrieve upload details for processing ({upload_id_or_access_id})."})
#             # Update progress_data if it exists, though ideally it shouldn't for a missing DB record
#             if upload_id_or_access_id in upload_progress_data:
#                 upload_progress_data[upload_id_or_access_id]['status'] = 'error'
#                 upload_progress_data[upload_id_or_access_id]['error'] = 'DB record not found for TG processing'
#             return

#         # --- Check current state from DB record ---
#         if db_record.get("storage_location") != "gdrive" or db_record.get("status_overall") != "gdrive_complete_pending_telegram":
#             current_status = db_record.get("status_overall", "unknown")
#             logging.warning(f"{log_prefix} Record not in expected state for GDrive-to-Telegram processing. Status: {current_status}. Location: {db_record.get('storage_location')}")
#             # If already processed or in error, don't re-process.
#             # Yield an appropriate message or just complete if already done.
#             if "telegram_complete" in current_status:
#                 yield _yield_sse_event('status', {'message': 'Telegram processing already completed for this item.'})
#                 # Optionally yield a 'complete' event again if frontend needs it.
#             elif "error" in current_status:
#                 yield _yield_sse_event('error', {'message': f"Previous error encountered: {db_record.get('last_error', 'Unknown error')}"})
#             else:
#                 yield _yield_sse_event('status', {'message': f"Item in unexpected state: {current_status}. Cannot start Telegram processing."})
#             if upload_id_or_access_id in upload_progress_data: # Clean up transient progress data
#                 del upload_progress_data[upload_id_or_access_id]
#             return

#         # Update transient progress_data status (if it's still being used for SSE linking)
#         if upload_id_or_access_id in upload_progress_data:
#             upload_progress_data[upload_id_or_access_id]['status'] = 'processing_telegram'
        
#         username = db_record['username'] # Get from DB record
#         files_to_process_from_gdrive = db_record.get("files_in_batch", []) # These have gdrive_file_id
#         batch_display_name = db_record.get("batch_display_name", "Upload")
#         db_record_access_id = db_record["access_id"] # Should be same as upload_id_or_access_id

#         if not files_to_process_from_gdrive: # Should have been caught by initiate_upload
#             logging.error(f"{log_prefix} No GDrive file details in DB record for processing.")
#             yield _yield_sse_event('error', {'message': 'Internal error: Missing GDrive file details in record.'})
#             # Update DB record to error state
#             db_record["status_overall"] = "error_missing_gdrive_details"
#             db_record["last_error"] = "Missing GDrive file details"
#             save_file_metadata(db_record)
#             if upload_id_or_access_id in upload_progress_data: del upload_progress_data[upload_id_or_access_id]
#             return

#         if TELEGRAM_CHAT_IDS and len(TELEGRAM_CHAT_IDS) > 0:
#             executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'TgUpload_{db_record_access_id[:4]}')
#             logging.info(f"{log_prefix} Initialized Telegram Upload Executor (max={MAX_UPLOAD_WORKERS})")
#         else: # Should have been caught by initiate_upload but double check
#             logging.error(f"{log_prefix} No Telegram chat IDs configured.")
#             yield _yield_sse_event('error', {'message': 'Server configuration error: No destination chats.'})
#             db_record["status_overall"] = "error_config_telegram_chats"
#             db_record["last_error"] = "No destination chats configured for Telegram"
#             save_file_metadata(db_record)
#             if upload_id_or_access_id in upload_progress_data: del upload_progress_data[upload_id_or_access_id]
#             if executor: executor.shutdown()
#             return

#         total_original_bytes_for_sse = db_record.get("total_original_size", 0)
#         # SSE 'start' event: filename here is the batch_display_name or original_filename if single
#         # totalSize is the sum of ORIGINAL file sizes
#         yield _yield_sse_event('start', {'filename': batch_display_name, 'totalSize': total_original_bytes_for_sse})
#         yield _yield_sse_event('status', {'message': f'Starting Telegram transfer for {len(files_to_process_from_gdrive)} file(s)...'})

#         overall_telegram_processing_start_time = time.time()
#         bytes_processed_for_sse_progress = 0
#         processed_files_for_final_db_record = [] # This will store the updated metadata for each file
#         batch_overall_telegram_success = True

#         for file_detail_from_db in files_to_process_from_gdrive:
#             original_filename = file_detail_from_db["original_filename"]
#             gdrive_file_id = file_detail_from_db["gdrive_file_id"] # Get GDrive ID from DB record
#             original_file_size = file_detail_from_db.get("original_size", 0)
            
#             # Copy existing details, then update with Telegram info
#             updated_file_meta_for_db = file_detail_from_db.copy() 
            
#             log_file_prefix_indiv = f"{log_prefix} File '{original_filename}' (GDriveID: {gdrive_file_id})"
#             logging.info(f"{log_file_prefix_indiv} Starting Telegram processing stage.")
            
#             local_temp_path_for_processing: Optional[str] = None
#             updated_file_meta_for_db["telegram_send_status"] = "processing" # Update status

#             try:
#                 yield _yield_sse_event('status', {'message': f'Fetching "{original_filename}" from temp storage for Telegram...'})
#                 logging.info(f"{log_file_prefix_indiv} Downloading from GDrive for Telegram...")
                
#                 gdrive_content_stream, download_err = download_from_gdrive(gdrive_file_id)
#                 if download_err or not gdrive_content_stream:
#                     raise Exception(f"GDrive download for Telegram failed: {download_err or 'No content'}")

#                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=os.path.splitext(original_filename)[1]) as temp_file_on_disk:
#                     local_temp_path_for_processing = temp_file_on_disk.name
#                     shutil.copyfileobj(gdrive_content_stream, temp_file_on_disk)
#                 gdrive_content_stream.close()
#                 logging.info(f"{log_file_prefix_indiv} Saved GDrive content to local temp for TG: {local_temp_path_for_processing}")
                
#                 current_file_processing_size = os.path.getsize(local_temp_path_for_processing)
#                 if current_file_processing_size == 0:
#                     updated_file_meta_for_db["skipped_for_telegram"] = True # New flag
#                     updated_file_meta_for_db["telegram_send_status"] = "skipped_empty"
#                     updated_file_meta_for_db["reason_telegram"] = "File empty after GDrive download for Telegram."
#                     # No batch_overall_telegram_success = False here, as skipping isn't a hard failure of the process.
#                 else: # File is not empty, proceed with Telegram upload
#                     # --- ACTUAL TELEGRAM UPLOAD LOGIC (OPERATES ON local_temp_path_for_processing) ---
#                     if current_file_processing_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
#                         updated_file_meta_for_db["is_split_for_telegram"] = True
#                         logging.info(f"{log_file_prefix_indiv} Is large ({format_bytes(current_file_processing_size)}), starting chunked upload to Telegram.")
#                         part_number_tg = 1
#                         bytes_processed_for_this_file_tg_chunking = 0
#                         all_chunks_sent_successfully_for_this_file_tg = True
#                         temp_tg_chunks_meta = []

#                         with open(local_temp_path_for_processing, 'rb') as f_in_tg_process:
#                             while True:
#                                 chunk_data_tg = f_in_tg_process.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
#                                 if not chunk_data_tg:
#                                     break
                                
#                                 chunk_tg_filename = f"{original_filename}.part{part_number_tg}"
#                                 log_chunk_prefix_tg = f"{log_file_prefix_indiv} TG Chunk {part_number_tg}"
#                                 logging.info(f"{log_chunk_prefix_tg} Preparing ({format_bytes(len(chunk_data_tg))}) for Telegram.")

#                                 chunk_specific_tg_futures: Dict[Future, str] = {}
#                                 chunk_specific_tg_results: Dict[str, ApiResult] = {}
#                                 primary_send_success_for_this_tg_chunk = False # API call success
#                                 # Message from the primary API call, whether success or failure
#                                 primary_send_message_for_this_tg_chunk = "Primary TG chunk send not attempted or failed."

#                                 if executor:
#                                     for chat_id_str_loop_tg in TELEGRAM_CHAT_IDS:
#                                         fut_tg_chunk = executor.submit(_send_chunk_task, chunk_data_tg, chunk_tg_filename, str(chat_id_str_loop_tg), upload_id_or_access_id, part_number_tg)
#                                         chunk_specific_tg_futures[fut_tg_chunk] = str(chat_id_str_loop_tg)
#                                 else: # Should not happen given earlier checks
#                                     _, res_no_exec_tg_chunk = _send_chunk_task(chunk_data_tg, chunk_tg_filename, str(TELEGRAM_CHAT_IDS[0]), upload_id_or_access_id, part_number_tg)
#                                     chunk_specific_tg_results[str(TELEGRAM_CHAT_IDS[0])] = res_no_exec_tg_chunk
#                                     primary_send_success_for_this_tg_chunk = res_no_exec_tg_chunk[0]
#                                     primary_send_message_for_this_tg_chunk = res_no_exec_tg_chunk[1]

#                                 if chunk_specific_tg_futures:
#                                     primary_tg_chunk_fut = next((f for f, cid_val in chunk_specific_tg_futures.items() if cid_val == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
#                                     if primary_tg_chunk_fut:
#                                         cid_res_tg_chunk, res_tg_chunk = primary_tg_chunk_fut.result()
#                                         chunk_specific_tg_results[cid_res_tg_chunk] = res_tg_chunk
#                                         primary_send_success_for_this_tg_chunk = res_tg_chunk[0]
#                                         primary_send_message_for_this_tg_chunk = res_tg_chunk[1]
#                                     else: # Primary chat ID not in the list, or something went wrong
#                                         primary_send_success_for_this_tg_chunk = False
#                                         primary_send_message_for_this_tg_chunk = "Primary chat ID not configured for chunk send or task init failed."
#                                     for fut_completed_tg_chunk in as_completed(chunk_specific_tg_futures):
#                                         cid_res_tg_c, res_tg_c = fut_completed_tg_chunk.result()
#                                         if cid_res_tg_c not in chunk_specific_tg_results:
#                                             chunk_specific_tg_results[cid_res_tg_c] = res_tg_c
                                
#                                 parsed_tg_locations_for_this_chunk = _parse_send_results(
#                                     f"{log_chunk_prefix_tg}-Parse", 
#                                     [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} 
#                                      for k, r in chunk_specific_tg_results.items()]
#                                 )
                                
#                                 # Check success from parsed results for primary chat
#                                 primary_chunk_parsed_info = next((loc for loc in parsed_tg_locations_for_this_chunk if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
                                
#                                 if primary_chunk_parsed_info and primary_chunk_parsed_info.get("success"):
#                                     temp_tg_chunks_meta.append({"part_number": part_number_tg, "size": len(chunk_data_tg), "send_locations": parsed_tg_locations_for_this_chunk})
#                                     bytes_processed_for_this_file_tg_chunking += len(chunk_data_tg)
#                                     yield _yield_sse_event('status', {'message': f'Sent TG chunk {part_number_tg} for "{original_filename}"'})
#                                 else:
#                                     error_reason_chunk_tg = primary_send_message_for_this_tg_chunk # Default to API call message
#                                     if primary_chunk_parsed_info and primary_chunk_parsed_info.get('error'): # Parsed error is more specific
#                                         error_reason_chunk_tg = primary_chunk_parsed_info.get('error')
                                    
#                                     logging.error(f"{log_chunk_prefix_tg} Telegram send FAILED. Reason: {error_reason_chunk_tg}. Aborting for this file.")
#                                     batch_overall_telegram_success = False
#                                     all_chunks_sent_successfully_for_this_file_tg = False
#                                     updated_file_meta_for_db["failed_telegram"] = True # More specific flag
#                                     updated_file_meta_for_db["reason_telegram"] = f"Failed TG chunk {part_number_tg}: {error_reason_chunk_tg}"
#                                     # Store the problematic chunk's attempt details
#                                     updated_file_meta_for_db["telegram_chunks"] = [{"part_number": part_number_tg, "size": len(chunk_data_tg), "send_locations": parsed_tg_locations_for_this_chunk}] 
#                                     break # Break from chunking loop for THIS file
#                                 part_number_tg += 1
                        
#                         if all_chunks_sent_successfully_for_this_file_tg:
#                             updated_file_meta_for_db["telegram_chunks"] = temp_tg_chunks_meta
#                             updated_file_meta_for_db["telegram_send_status"] = "success_chunked"
#                             updated_file_meta_for_db["telegram_total_chunked_size"] = bytes_processed_for_this_file_tg_chunking
#                         else:
#                             updated_file_meta_for_db["telegram_send_status"] = "failed_chunking"
#                             # The reason should have been set when a chunk failed
                    
#                     else: # SINGLE FILE to Telegram
#                         updated_file_meta_for_db["is_split_for_telegram"] = False
#                         single_tg_file_futures: Dict[Future, str] = {}
#                         single_tg_file_results: Dict[str, ApiResult] = {}
#                         primary_send_api_call_success_tg = False
#                         primary_send_api_message_tg = "Primary TG send (single) failed."

#                         if executor:
#                             for chat_id_str_single_tg in TELEGRAM_CHAT_IDS:
#                                 fut_single_tg = executor.submit(_send_single_file_task, local_temp_path_for_processing, original_filename, str(chat_id_str_single_tg), upload_id_or_access_id)
#                                 single_tg_file_futures[fut_single_tg] = str(chat_id_str_single_tg)
#                         else: # Should not happen
#                             _, res_single_no_exec_tg = _send_single_file_task(local_temp_path_for_processing, original_filename, str(TELEGRAM_CHAT_IDS[0]), upload_id_or_access_id)
#                             single_tg_file_results[str(TELEGRAM_CHAT_IDS[0])] = res_single_no_exec_tg
#                             primary_send_api_call_success_tg = res_single_no_exec_tg[0]
#                             primary_send_api_message_tg = res_single_no_exec_tg[1]
                        
#                         if single_tg_file_futures:
#                             primary_fut_single_tg = next((f for f, cid_val in single_tg_file_futures.items() if cid_val == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
#                             if primary_fut_single_tg:
#                                 cid_res_tg_s, res_tg_s = primary_fut_single_tg.result()
#                                 single_tg_file_results[cid_res_tg_s] = res_tg_s
#                                 primary_send_api_call_success_tg = res_tg_s[0]
#                                 primary_send_api_message_tg = res_tg_s[1]
#                             else:
#                                 primary_send_api_call_success_tg = False
#                                 primary_send_api_message_tg = "Primary chat ID not configured for single send or task init failed."

#                             for fut_completed_tg_s in as_completed(single_tg_file_futures):
#                                 cid_res_tg_s_comp, res_tg_s_comp = fut_completed_tg_s.result()
#                                 if cid_res_tg_s_comp not in single_tg_file_results:
#                                     single_tg_file_results[cid_res_tg_s_comp] = res_tg_s_comp
                        
#                         parsed_tg_locations_single_file = _parse_send_results(
#                             f"{log_file_prefix_indiv}-TGSendSingle", 
#                             [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} 
#                              for k,r in single_tg_file_results.items()]
#                         )

#                         primary_single_parsed_info = next((loc for loc in parsed_tg_locations_single_file if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)

#                         if primary_single_parsed_info and primary_single_parsed_info.get("success"):
#                             updated_file_meta_for_db["telegram_send_locations"] = parsed_tg_locations_single_file
#                             updated_file_meta_for_db["telegram_send_status"] = "success_single"
#                         else:
#                             error_reason_single_tg = primary_send_api_message_tg # Default to API call message
#                             if primary_single_parsed_info and primary_single_parsed_info.get('error'): # Parsed error is more specific
#                                 error_reason_single_tg = primary_single_parsed_info.get('error')
                            
#                             batch_overall_telegram_success = False
#                             updated_file_meta_for_db["failed_telegram"] = True # More specific flag
#                             updated_file_meta_for_db["reason_telegram"] = f"Primary TG send failed: {error_reason_single_tg}"
#                             updated_file_meta_for_db["telegram_send_locations"] = parsed_tg_locations_single_file # Store failure details
#                             updated_file_meta_for_db["telegram_send_status"] = "failed_single"
            
#                 # After successful Telegram processing for this file (or if skipped but GDrive download was ok)
#                 if updated_file_meta_for_db["telegram_send_status"].startswith("success") or updated_file_meta_for_db["telegram_send_status"] == "skipped_empty":
#                     # Only add original_file_size to SSE progress if Telegram part was successful or skipped harmlessly
#                     bytes_processed_for_sse_progress += original_file_size
#                     logging.info(f"{log_file_prefix_indiv} Telegram stage complete (status: {updated_file_meta_for_db['telegram_send_status']}). Deleting from GDrive ID: {gdrive_file_id}")
#                     gdrive_delete_success, gdrive_delete_error = delete_from_gdrive(gdrive_file_id)
#                     if not gdrive_delete_success:
#                         logging.warning(f"{log_file_prefix_indiv} Failed to delete from GDrive ID {gdrive_file_id} after Telegram stage: {gdrive_delete_error}")
#                         updated_file_meta_for_db["gdrive_cleanup_error"] = gdrive_delete_error
#                 else: # Telegram processing failed for this file
#                     batch_overall_telegram_success = False # Ensure this is set
#                     bytes_processed_for_sse_progress += original_file_size # Still count original size as "attempted" for progress
#                     logging.warning(f"{log_file_prefix_indiv} Not deleting from GDrive as Telegram processing failed (status: {updated_file_meta_for_db['telegram_send_status']}).")
#                     # The reason should be in updated_file_meta_for_db["reason_telegram"]

#             except Exception as file_processing_exception:
#                 # This catches errors from GDrive download, local file ops, or unexpected issues in TG logic
#                 logging.error(f"{log_file_prefix_indiv} Error during GDrive download or Telegram prep for this file: {file_processing_exception}", exc_info=True)
#                 updated_file_meta_for_db["telegram_send_status"] = "error_processing_file" # More specific status
#                 updated_file_meta_for_db["reason_telegram"] = f"Internal error during file processing: {str(file_processing_exception)}"
#                 updated_file_meta_for_db["failed_telegram"] = True # Mark as failed for Telegram part
#                 batch_overall_telegram_success = False
#                 bytes_processed_for_sse_progress += original_file_size # Count as processed for progress
#             finally:
#                 if local_temp_path_for_processing and os.path.exists(local_temp_path_for_processing):
#                     _safe_remove_file(local_temp_path_for_processing, log_file_prefix_indiv, "local temp GDrive content file for TG")
            
#             processed_files_for_final_db_record.append(updated_file_meta_for_db)
#             yield _yield_sse_event('progress', _calculate_progress(overall_telegram_processing_start_time, bytes_processed_for_sse_progress, total_original_bytes_for_sse))
#             yield _yield_sse_event('status', {'message': f'Telegram: Processed {len(processed_files_for_final_db_record)} of {len(files_to_process_from_gdrive)} files...'})
        
#         # --- After loop for all files ---
#         # ... (rest of the function: total_batch_telegram_duration, MongoDB update, final SSE 'complete' event) ...
#         # This part should be largely the same as your last provided full code.
#         total_batch_telegram_duration = time.time() - overall_telegram_processing_start_time
#         logging.info(f"{log_prefix} Finished all files GDrive-to-Telegram processing. Duration: {total_batch_telegram_duration:.2f}s. Overall Success: {batch_overall_telegram_success}")

#         # Update the main DB record
#         db_record["files_in_batch"] = processed_files_for_final_db_record # Replace with updated file info
#         db_record["storage_location"] = "telegram" if batch_overall_telegram_success else "mixed_gdrive_telegram_error"
#         db_record["status_overall"] = "telegram_complete" if batch_overall_telegram_success else "telegram_processing_errors"
#         db_record["telegram_processing_timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
#         db_record["total_telegram_processing_duration_seconds"] = round(total_batch_telegram_duration, 2)
#         if not batch_overall_telegram_success:
#             # Collect reasons from individual files
#             error_reasons = [f["reason_telegram"] for f in processed_files_for_final_db_record if f.get("failed_telegram") and f.get("reason_telegram")]
#             db_record["last_error"] = "; ".join(error_reasons) if error_reasons else "One or more files failed Telegram processing."


#         save_success_final, save_msg_final = save_file_metadata(db_record) # This will upsert/update
#         if not save_success_final:
#             logging.error(f"{log_prefix} DB CRITICAL: Failed to UPDATE MongoDB record after Telegram processing: {save_msg_final}")
#             yield _yield_sse_event('error', {'message': f"Failed to finalize record after Telegram processing: {save_msg_final}"})
#             if upload_id_or_access_id in upload_progress_data:
#                 upload_progress_data[upload_id_or_access_id]['status'] = 'completed_metadata_error'
#                 upload_progress_data[upload_id_or_access_id]['error'] = f"Final DB update fail: {save_msg_final}"
#             if executor: executor.shutdown(wait=False)
#             return
        
#         logging.info(f"{log_prefix} DB: Successfully UPDATED MongoDB record after Telegram processing (access_id: {db_record_access_id}).")
        
#         # --- Final SSE 'complete' event ---
#         is_single_effective_file_final = (len(processed_files_for_final_db_record) == 1) and \
#                                     (not db_record.get('is_batch') or len(db_record.get('files_in_batch', [])) == 1)


#         final_sse_filename = batch_display_name
#         if is_single_effective_file_final and processed_files_for_final_db_record:
#             final_sse_filename = processed_files_for_final_db_record[0].get('original_filename', batch_display_name)
        
#         browser_url = f"{request.host_url.rstrip('/')}/get/{db_record_access_id}"
#         if not is_single_effective_file_final : 
#             browser_url = f"{request.host_url.rstrip('/')}/batch-view/{db_record_access_id}"


#         complete_message_text = f'"{final_sse_filename}" Telegram processing complete. ' + \
#                         ('Some files had issues.' if not batch_overall_telegram_success else 'All files successful.')
        
#         complete_payload = {
#             'message': complete_message_text, 'download_url': browser_url, 
#             'filename': final_sse_filename, 'access_id': db_record_access_id, 
#             'batch_access_id': db_record_access_id, 
#             'is_batch': not is_single_effective_file_final,
#         }
        
#         if upload_id_or_access_id in upload_progress_data:
#             upload_progress_data[upload_id_or_access_id]['status'] = 'completed_with_errors' if not batch_overall_telegram_success else 'completed'
        
#         logging.info(f"{log_prefix} Yielding final 'complete' event. Payload: {json.dumps(complete_payload)}")
#         yield _yield_sse_event('complete', complete_payload)

#     except Exception as e:
#         error_msg_final = f"Critical GDrive-to-Telegram processing error: {str(e) or type(e).__name__}"
#         logging.error(f"{log_prefix} UNHANDLED EXCEPTION in GDrive-to-Telegram generator: {e}", exc_info=True)
#         yield _yield_sse_event('error', {'message': error_msg_final})
#         if upload_id_or_access_id in upload_progress_data: 
#             upload_progress_data[upload_id_or_access_id]['status'] = 'error'
#             upload_progress_data[upload_id_or_access_id]['error'] = error_msg_final
#         if 'db_record' in locals() and db_record: 
#             db_record["status_overall"] = "error_telegram_processing"
#             db_record["last_error"] = error_msg_final
#             save_file_metadata(db_record)
                
#     finally:
#         logging.info(f"{log_prefix} GDrive-to-Telegram generator final cleanup.")
#         if executor:
#             executor.shutdown(wait=True)
#             logging.info(f"{log_prefix} Telegram Upload executor shutdown (waited).")
        
#         if upload_id_or_access_id in upload_progress_data:
#             try:
#                 del upload_progress_data[upload_id_or_access_id]
#                 logging.info(f"{log_prefix} Removed transient progress data for {upload_id_or_access_id}.")
#             except KeyError: pass
        
#         final_status_report = 'unknown (db_record or progress_data missing)'
#         if 'db_record' in locals() and db_record: # Check if db_record was defined
#             final_status_report = db_record.get('status_overall', 'unknown (status_overall key missing in DB)')
        
#         logging.info(f"{log_prefix} GDrive-to-Telegram generator finished. Final DB Status: {final_status_report}")

# upload_routes.py

# ... (all other imports and functions like _parse_send_results, _send_single_file_task, _send_chunk_task,
# sse_gdrive_upload_status, initiate_upload, stream_progress remain as in the previous response) ...

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

# def run_gdrive_to_telegram_transfer(access_id: str):
#     """
#     Handles the GDrive to Telegram transfer in the background.
#     This function is submitted to the background_executor.
#     It does NOT yield SSE events directly to a client.
#     """
#     log_prefix = f"[BG-TG-{access_id}]"
#     logging.info(f"{log_prefix} Background GDrive-to-Telegram transfer started.")
    
#     # Use a new ThreadPoolExecutor for tasks within this background job if needed,
#     # or manage send_file_to_telegram calls if they are blocking.
#     # For simplicity, we'll assume _send_single_file_task and _send_chunk_task are fine.
#     # If they are also long-running and CPU-bound, this executor might get starved.
#     # Consider a dedicated executor for TG sends if they are numerous/long.
#     tg_send_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'BgTgSend_{access_id[:4]}')
    
#     db_record = None
#     try:
#         db_record, db_error = find_metadata_by_access_id(access_id)
#         if db_error or not db_record:
#             logging.error(f"{log_prefix} Failed to fetch DB record: {db_error or 'Not found'}")
#             return

#         if db_record.get("status_overall") != "gdrive_complete_pending_telegram":
#             logging.warning(f"{log_prefix} Record not in 'gdrive_complete_pending_telegram' state. Current: {db_record.get('status_overall')}. Aborting background task.")
#             return

#         # Mark as processing Telegram in DB
#         db_record["status_overall"] = "telegram_processing_background"
#         save_file_metadata(db_record) # Save intermediate state

#         files_to_process = db_record.get("files_in_batch", [])
#         processed_files_for_db = []
#         batch_tg_success = True
        
#         for file_detail in files_to_process:
#             original_filename = file_detail.get("original_filename")
#             gdrive_file_id = file_detail.get("gdrive_file_id")
#             # original_size = file_detail.get("original_size", 0) # Not directly used for progress here

#             if not original_filename or not gdrive_file_id:
#                 logging.error(f"{log_prefix} Skipping file due to missing data: {file_detail}")
#                 failed_entry = file_detail.copy()
#                 failed_entry["telegram_send_status"] = "skipped_bad_data_bg"
#                 processed_files_for_db.append(failed_entry)
#                 batch_tg_success = False
#                 continue

#             current_file_log_prefix = f"{log_prefix} File '{original_filename}'"
#             updated_file_meta = file_detail.copy() # Work on a copy
#             local_temp_file_for_tg: Optional[str] = None

#             try:
#                 logging.info(f"{current_file_log_prefix} Downloading from GDrive (ID: {gdrive_file_id}) for Telegram.")
#                 gdrive_stream, dl_err = download_from_gdrive(gdrive_file_id)
#                 if dl_err or not gdrive_stream:
#                     raise Exception(f"GDrive download failed for TG: {dl_err or 'No stream'}")

#                 with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=os.path.splitext(original_filename)[1]) as temp_f:
#                     local_temp_file_for_tg = temp_f.name
#                     shutil.copyfileobj(gdrive_stream, temp_f)
#                 gdrive_stream.close()
#                 logging.info(f"{current_file_log_prefix} Downloaded to temp: {local_temp_file_for_tg}")

#                 file_size_for_tg = os.path.getsize(local_temp_file_for_tg)
#                 if file_size_for_tg == 0:
#                     updated_file_meta["telegram_send_status"] = "skipped_empty_bg"
#                     logging.warning(f"{current_file_log_prefix} File is empty. Skipping Telegram send.")
#                 else:
#                     # Perform Telegram upload (chunked or single)
#                     # This logic is condensed from your original process_upload_and_generate_updates
#                     if file_size_for_tg > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
#                         updated_file_meta["is_split_for_telegram"] = True
#                         # ... Chunking logic using tg_send_executor, _send_chunk_task, _parse_send_results ...
#                         # ... This part needs to be carefully adapted to not yield SSE events ...
#                         # ... and to correctly update updated_file_meta with chunk details or errors ...
#                         # For brevity, I'm simplifying here. You need to integrate your full chunking logic.
#                         # Example sketch:
#                         all_chunks_ok = True
#                         chunk_meta_list = []
#                         with open(local_temp_file_for_tg, 'rb') as f_chunk:
#                             part_num = 1
#                             while True:
#                                 chunk_data = f_chunk.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
#                                 if not chunk_data: break
#                                 chunk_filename = f"{original_filename}.part{part_num}"
                                
#                                 futures_dict: Dict[Future, str] = {
#                                     tg_send_executor.submit(_send_chunk_task, chunk_data, chunk_filename, str(chat_id), access_id, part_num): str(chat_id)
#                                     for chat_id in TELEGRAM_CHAT_IDS
#                                 }
#                                 results_dict: Dict[str, ApiResult] = {}
#                                 primary_ok_chunk = False
#                                 for future in as_completed(futures_dict):
#                                     chat_id_res = futures_dict[future]
#                                     try:
#                                         _, api_res = future.result()
#                                         results_dict[chat_id_res] = api_res
#                                         if chat_id_res == str(PRIMARY_TELEGRAM_CHAT_ID) and api_res[0]:
#                                             primary_ok_chunk = True
#                                     except Exception as e_f:
#                                         results_dict[chat_id_res] = (False, f"Task exc: {e_f}", None)
                                
#                                 parsed_locs = _parse_send_results(f"{current_file_log_prefix}-Chunk{part_num}", 
#                                                                   [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} 
#                                                                    for k,r in results_dict.items()])
#                                 primary_parsed_info_chunk = next((loc for loc in parsed_locs if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)

#                                 if primary_parsed_info_chunk and primary_parsed_info_chunk.get("success"):
#                                     chunk_meta_list.append({"part_number": part_num, "size": len(chunk_data), "send_locations": parsed_locs})
#                                 else:
#                                     all_chunks_ok = False
#                                     updated_file_meta["reason_telegram"] = f"Failed TG chunk {part_num}: {primary_parsed_info_chunk.get('error') if primary_parsed_info_chunk else 'Primary send failed'}"
#                                     break
#                                 part_num +=1
                        
#                         if all_chunks_ok:
#                             updated_file_meta["telegram_send_status"] = "success_chunked_bg"
#                             updated_file_meta["telegram_chunks"] = chunk_meta_list
#                         else:
#                             updated_file_meta["telegram_send_status"] = "failed_chunking_bg"
#                             batch_tg_success = False


#                     else: # Single file
#                         # ... Single file logic using tg_send_executor, _send_single_file_task, _parse_send_results ...
#                         # ... Adapt similarly to not yield SSE and update updated_file_meta ...
#                         # Example sketch:
#                         futures_dict_single: Dict[Future, str] = {
#                             tg_send_executor.submit(_send_single_file_task, local_temp_file_for_tg, original_filename, str(chat_id), access_id): str(chat_id)
#                             for chat_id in TELEGRAM_CHAT_IDS
#                         }
#                         results_dict_single: Dict[str, ApiResult] = {}
#                         primary_ok_single = False
#                         for future_s in as_completed(futures_dict_single):
#                             chat_id_res_s = futures_dict_single[future_s]
#                             try:
#                                 _, api_res_s = future_s.result()
#                                 results_dict_single[chat_id_res_s] = api_res_s
#                                 if chat_id_res_s == str(PRIMARY_TELEGRAM_CHAT_ID) and api_res_s[0]:
#                                     primary_ok_single = True
#                             except Exception as e_fs:
#                                 results_dict_single[chat_id_res_s] = (False, f"Task exc: {e_fs}", None)

#                         parsed_locs_single = _parse_send_results(f"{current_file_log_prefix}-Single",
#                                                                  [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} 
#                                                                   for k,r in results_dict_single.items()])
#                         primary_parsed_info_single = next((loc for loc in parsed_locs_single if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)

#                         if primary_parsed_info_single and primary_parsed_info_single.get("success"):
#                             updated_file_meta["telegram_send_status"] = "success_single_bg"
#                             updated_file_meta["telegram_send_locations"] = parsed_locs_single
#                         else:
#                             updated_file_meta["telegram_send_status"] = "failed_single_bg"
#                             updated_file_meta["reason_telegram"] = f"Primary TG send failed: {primary_parsed_info_single.get('error') if primary_parsed_info_single else 'Primary send failed'}"
#                             batch_tg_success = False

#                 # After TG attempt for this file
#                 if updated_file_meta["telegram_send_status"].startswith("success"):
#                     logging.info(f"{current_file_log_prefix} Successfully sent to Telegram. Deleting from GDrive.")
#                     delete_from_gdrive(gdrive_file_id) # Ignore result for now, or log error
#                 else:
#                     logging.warning(f"{current_file_log_prefix} Failed Telegram send. File remains in GDrive (ID: {gdrive_file_id}).")
#                     batch_tg_success = False # Mark overall batch as having issues if any file fails

#             except Exception as e_file_processing:
#                 logging.error(f"{current_file_log_prefix} Error processing for Telegram: {e_file_processing}", exc_info=True)
#                 updated_file_meta["telegram_send_status"] = "error_processing_bg"
#                 updated_file_meta["reason_telegram"] = str(e_file_processing)
#                 batch_tg_success = False
#             finally:
#                 if local_temp_file_for_tg and os.path.exists(local_temp_file_for_tg):
#                     _safe_remove_file(local_temp_file_for_tg, current_file_log_prefix, "temp for TG send")
            
#             processed_files_for_db.append(updated_file_meta)

#         # All files in the batch processed for Telegram
#         db_record["files_in_batch"] = processed_files_for_db
#         if batch_tg_success:
#             db_record["storage_location"] = "telegram"
#             db_record["status_overall"] = "telegram_complete"
#             logging.info(f"{log_prefix} All files successfully transferred to Telegram.")
#         else:
#             db_record["storage_location"] = "mixed_gdrive_telegram_error" # Or "gdrive_error" if all failed TG
#             db_record["status_overall"] = "telegram_processing_errors"
#             # Collect error reasons for last_error
#             error_summary = []
#             for f_meta in processed_files_for_db:
#                 if not f_meta.get("telegram_send_status", "").startswith("success") and \
#                    f_meta.get("telegram_send_status") != "skipped_empty_bg":
#                     error_summary.append(f"File '{f_meta.get('original_filename')}': {f_meta.get('reason_telegram', 'Unknown TG error')}")
#             db_record["last_error"] = "; ".join(error_summary) if error_summary else "Telegram processing had errors, no specific reasons."
#             logging.warning(f"{log_prefix} Some files failed Telegram transfer. Errors: {db_record['last_error']}")

#         db_record["telegram_processing_timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
#         save_file_metadata(db_record)
#         logging.info(f"{log_prefix} Final DB record updated. Status: {db_record['status_overall']}")

#     except Exception as e_bg:
#         logging.error(f"{log_prefix} Unhandled exception in background GDrive-to-Telegram transfer: {e_bg}", exc_info=True)
#         if db_record: # If record was fetched, try to mark it as error
#             db_record["status_overall"] = "error_telegram_processing_unhandled_bg"
#             db_record["last_error"] = f"Unhandled background error: {str(e_bg)}"
#             save_file_metadata(db_record)
#     finally:
#         if tg_send_executor:
#             tg_send_executor.shutdown(wait=True)
#         # Clean up the operation from upload_progress_data now that background task is done or errored
#         if access_id in upload_progress_data:
#             try:
#                 del upload_progress_data[access_id]
#                 logging.info(f"{log_prefix} Cleaned up transient progress data for {access_id} after background task.")
#             except KeyError:
#                 pass # Already gone
#         logging.info(f"{log_prefix} Background GDrive-to-Telegram transfer finished.")

def run_gdrive_to_telegram_transfer(access_id: str):
    """
    Handles the GDrive to Telegram transfer in the background.
    This function is submitted to the background_executor.
    It does NOT yield SSE events directly to a client.
    """
    log_prefix = f"[BG-TG-{access_id}]"
    logging.info(f"{log_prefix} Background GDrive-to-Telegram transfer started.")
    
    tg_send_executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'BgTgSend_{access_id[:4]}')
    
    db_record = None
    try:
        db_record, db_error = find_metadata_by_access_id(access_id)
        if db_error or not db_record:
            logging.error(f"{log_prefix} Failed to fetch DB record: {db_error or 'Not found'}")
            return

        if db_record.get("status_overall") != "gdrive_complete_pending_telegram":
            logging.warning(f"{log_prefix} Record not in 'gdrive_complete_pending_telegram' state. Current: {db_record.get('status_overall')}. Aborting background task.")
            return

        db_record["status_overall"] = "telegram_processing_background"
        save_file_metadata(db_record) 

        files_to_process = db_record.get("files_in_batch", [])
        processed_files_for_db = []
        batch_tg_success = True
        
        for file_detail in files_to_process:
            original_filename = file_detail.get("original_filename")
            gdrive_file_id = file_detail.get("gdrive_file_id")

            if not original_filename or not gdrive_file_id:
                logging.error(f"{log_prefix} Skipping file due to missing data: {file_detail}")
                failed_entry = file_detail.copy()
                failed_entry["telegram_send_status"] = "skipped_bad_data_bg"
                processed_files_for_db.append(failed_entry)
                batch_tg_success = False
                continue

            current_file_log_prefix = f"{log_prefix} File '{original_filename}'"
            updated_file_meta = file_detail.copy() 
            local_temp_file_for_tg: Optional[str] = None

            try:
                logging.info(f"{current_file_log_prefix} Downloading from GDrive (ID: {gdrive_file_id}) for Telegram.")
                gdrive_stream, dl_err = download_from_gdrive(gdrive_file_id)
                if dl_err or not gdrive_stream:
                    raise Exception(f"GDrive download failed for TG: {dl_err or 'No stream'}")

                file_suffix_tg = os.path.splitext(original_filename)[1] if '.' in original_filename else ".tmp"
                with tempfile.NamedTemporaryFile(delete=False, dir=UPLOADS_TEMP_DIR, suffix=file_suffix_tg) as temp_f:
                    local_temp_file_for_tg = temp_f.name
                    shutil.copyfileobj(gdrive_stream, temp_f)
                gdrive_stream.close()
                logging.info(f"{current_file_log_prefix} Downloaded to temp: {local_temp_file_for_tg}")

                file_size_for_tg = os.path.getsize(local_temp_file_for_tg)
                if file_size_for_tg == 0:
                    updated_file_meta["telegram_send_status"] = "skipped_empty_bg"
                    logging.warning(f"{current_file_log_prefix} File is empty. Skipping Telegram send.")
                else:
                    if file_size_for_tg > TELEGRAM_MAX_CHUNK_SIZE_BYTES:
                        updated_file_meta["is_split_for_telegram"] = True
                        all_chunks_ok = True
                        chunk_meta_list = []
                        with open(local_temp_file_for_tg, 'rb') as f_chunk:
                            part_num = 1
                            while True:
                                chunk_data = f_chunk.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
                                if not chunk_data: break
                                chunk_filename = f"{original_filename}.part{part_num}"
                                
                                futures_dict: Dict[Future, str] = {
                                    tg_send_executor.submit(_send_chunk_task, chunk_data, chunk_filename, str(chat_id), access_id, part_num): str(chat_id)
                                    for chat_id in TELEGRAM_CHAT_IDS
                                }
                                results_dict: Dict[str, ApiResult] = {}
                                for future_item in as_completed(futures_dict):
                                    chat_id_res = futures_dict[future_item]
                                    try: _, api_res = future_item.result(); results_dict[chat_id_res] = api_res
                                    except Exception as e_f: results_dict[chat_id_res] = (False, f"Task exc: {e_f}", None)
                                
                                parsed_locs = _parse_send_results(f"{current_file_log_prefix}-Chunk{part_num}", 
                                                                  [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} 
                                                                   for k,r in results_dict.items()])
                                primary_parsed_info_chunk = next((loc for loc in parsed_locs if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)

                                if primary_parsed_info_chunk and primary_parsed_info_chunk.get("success"):
                                    chunk_meta_list.append({"part_number": part_num, "size": len(chunk_data), "send_locations": parsed_locs})
                                else:
                                    all_chunks_ok = False
                                    err_reason = (primary_parsed_info_chunk.get('error') if primary_parsed_info_chunk 
                                                  else "Primary chunk send failed/missing")
                                    updated_file_meta["reason_telegram"] = f"Failed TG chunk {part_num}: {err_reason}"
                                    logging.error(f"{current_file_log_prefix} Failed TG chunk {part_num}: {err_reason}")
                                    break
                                part_num +=1
                        
                        if all_chunks_ok:
                            updated_file_meta["telegram_send_status"] = "success_chunked_bg"
                            updated_file_meta["telegram_chunks"] = chunk_meta_list
                        else:
                            updated_file_meta["telegram_send_status"] = "failed_chunking_bg"
                            batch_tg_success = False
                    else: # Single file
                        futures_dict_single: Dict[Future, str] = {
                            tg_send_executor.submit(_send_single_file_task, local_temp_file_for_tg, original_filename, str(chat_id), access_id): str(chat_id)
                            for chat_id in TELEGRAM_CHAT_IDS
                        }
                        results_dict_single: Dict[str, ApiResult] = {}
                        for future_s in as_completed(futures_dict_single):
                            chat_id_res_s = futures_dict_single[future_s]
                            try: _, api_res_s = future_s.result(); results_dict_single[chat_id_res_s] = api_res_s
                            except Exception as e_fs: results_dict_single[chat_id_res_s] = (False, f"Task exc: {e_fs}", None)

                        parsed_locs_single = _parse_send_results(f"{current_file_log_prefix}-Single",
                                                                 [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} 
                                                                  for k,r in results_dict_single.items()])
                        primary_parsed_info_single = next((loc for loc in parsed_locs_single if str(loc.get("chat_id")) == str(PRIMARY_TELEGRAM_CHAT_ID)), None)

                        if primary_parsed_info_single and primary_parsed_info_single.get("success"):
                            updated_file_meta["telegram_send_status"] = "success_single_bg"
                            updated_file_meta["telegram_send_locations"] = parsed_locs_single
                        else:
                            err_reason_s = (primary_parsed_info_single.get('error') if primary_parsed_info_single 
                                          else "Primary single send failed/missing")
                            updated_file_meta["telegram_send_status"] = "failed_single_bg"
                            updated_file_meta["reason_telegram"] = f"Primary TG send failed: {err_reason_s}"
                            logging.error(f"{current_file_log_prefix} Failed single TG send: {err_reason_s}")
                            batch_tg_success = False

                if updated_file_meta["telegram_send_status"].startswith("success"):
                    logging.info(f"{current_file_log_prefix} Successfully sent to Telegram. Deleting from GDrive.")
                    del_success, del_err = delete_from_gdrive(gdrive_file_id)
                    if not del_success: logging.warning(f"{current_file_log_prefix} Failed GDrive delete ID {gdrive_file_id}: {del_err}")
                else:
                    logging.warning(f"{current_file_log_prefix} Failed Telegram send. File remains in GDrive (ID: {gdrive_file_id}).")
                    # batch_tg_success implicitly false if not starts with "success"

            except Exception as e_file_processing:
                logging.error(f"{current_file_log_prefix} Error processing for Telegram: {e_file_processing}", exc_info=True)
                updated_file_meta["telegram_send_status"] = "error_processing_bg"
                updated_file_meta["reason_telegram"] = str(e_file_processing)
                batch_tg_success = False
            finally:
                if local_temp_file_for_tg and os.path.exists(local_temp_file_for_tg):
                    _safe_remove_file(local_temp_file_for_tg, current_file_log_prefix, "temp for TG send")
            
            processed_files_for_db.append(updated_file_meta)

        db_record["files_in_batch"] = processed_files_for_db
        if batch_tg_success:
            db_record["storage_location"] = "telegram"
            db_record["status_overall"] = "telegram_complete"
            logging.info(f"{log_prefix} All files successfully transferred to Telegram.")
        else:
            db_record["storage_location"] = "mixed_gdrive_telegram_error"
            db_record["status_overall"] = "telegram_processing_errors"
            error_summary = [f"File '{f.get('original_filename')}': {f.get('reason_telegram', 'Unknown TG error')}"
                             for f in processed_files_for_db if not f.get("telegram_send_status", "").startswith("success") and f.get("telegram_send_status") != "skipped_empty_bg"]
            db_record["last_error"] = "; ".join(error_summary) if error_summary else "Telegram processing had errors."
            logging.warning(f"{log_prefix} Some files failed Telegram transfer. Errors: {db_record['last_error']}")

        db_record["telegram_processing_timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        save_file_metadata(db_record)
        logging.info(f"{log_prefix} Final DB record updated. Status: {db_record['status_overall']}")

    except Exception as e_bg:
        logging.error(f"{log_prefix} Unhandled exception in background GDrive-to-Telegram transfer: {e_bg}", exc_info=True)
        if db_record: 
            db_record["status_overall"] = "error_telegram_processing_unhandled_bg"
            db_record["last_error"] = f"Unhandled background error: {str(e_bg)}"
            save_file_metadata(db_record)
    finally:
        if tg_send_executor:
            tg_send_executor.shutdown(wait=True)
        if access_id in upload_progress_data:
            try: del upload_progress_data[access_id]
            except KeyError: pass
        logging.info(f"{log_prefix} Background GDrive-to-Telegram transfer finished.")

# # def process_upload_and_generate_updates(upload_id: str) -> Generator[SseEvent, None, None]:
#     executor: Optional[ThreadPoolExecutor] = None
#     log_prefix = f"[{upload_id}]"
#     logging.info(f"{log_prefix} Starting processing generator (source: Google Drive).")
    
#     upload_data = upload_progress_data.get(upload_id)
#     if not upload_data:
#         logging.error(f"{log_prefix} Critical: Upload data missing for operation ID.")
#         yield _yield_sse_event('error', {'message': 'Internal error: Upload data not found.'})
#         return

#     if upload_data.get('status') != 'initiated_gdrive':
#         logging.warning(f"{log_prefix} Process called in unexpected state: {upload_data.get('status')}. Expected 'initiated_gdrive'.")
#         if upload_data.get('status') not in ['processing_telegram', 'completed', 'completed_with_errors', 'error']:
#              yield _yield_sse_event('error', {'message': f"Processing error: Invalid state '{upload_data.get('status')}'."})
#         return
#     username = upload_data['username']
#     files_from_gdrive_details = upload_data.get("files_in_gdrive_details", [])
#     batch_display_name = upload_data.get("batch_display_name", "Upload")
#     db_record_access_id: str = upload_data.get('access_id')
#     if not upload_data.get("is_batch") or not files_from_gdrive_details:
#         logging.error(f"{log_prefix} Invalid batch data or no GDrive file details found.")
#         yield _yield_sse_event('error', {'message': 'Internal error: Invalid GDrive batch data.'})
#         upload_data['status'] = 'error'; upload_data['error'] = 'Invalid GDrive batch data'
#         return
#     if not db_record_access_id: # Should have been set in initiate_upload
#         logging.error(f"{log_prefix} Critical: db_record_access_id (for MongoDB) missing from upload_data.")
#         yield _yield_sse_event('error', {'message': 'Internal error: Missing record identifier.'})
#         upload_data['status'] = 'error'; upload_data['error'] = 'Missing record identifier'
#         return
#     # try:
#     #     log_prefix = f"[{upload_id}]"
#     #     logging.info(f"{log_prefix} Starting processing generator...")
#     #     upload_data = upload_progress_data.get(upload_id)
#     #     if not upload_data:
#     #         yield _yield_sse_event('error', {'message': 'Internal error: Upload data not found.'})
#     #         return

#     #     username = upload_data['username']
#     #     batch_directory_path = upload_data.get("batch_directory_path")
#     #     original_filenames_in_batch = upload_data.get("original_filenames_in_batch", [])
#     #     batch_display_name = upload_data.get("batch_display_name", "Upload")

#     #     if not upload_data.get("is_batch") or not batch_directory_path or not os.path.isdir(batch_directory_path) or not original_filenames_in_batch:
#     #         yield _yield_sse_event('error', {'message': 'Internal error: Invalid batch data.'})
#     #         if batch_directory_path and os.path.isdir(batch_directory_path):
#     #             _safe_remove_directory(batch_directory_path, log_prefix, "invalid batch dir")
#     #         return

#     #     upload_data['status'] = 'processing_telegram'
#     #     access_id: str = upload_data.get('access_id') or uuid.uuid4().hex[:10]
#     #     upload_data['access_id'] = access_id

#     #     if len(TELEGRAM_CHAT_IDS) > 0:
#     #         executor = ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS, thread_name_prefix=f'Upload_{upload_id[:4]}')
#     #     else:
#     #         yield _yield_sse_event('error', {'message': 'Server configuration error: No destination chats.'})
#     #         return

#     #     total_original_bytes_in_batch = 0
#     #     files_to_process_details = []
#     #     for filename_in_list in original_filenames_in_batch:
#     #         file_path_in_list = os.path.join(batch_directory_path, filename_in_list)
#     #         if os.path.exists(file_path_in_list):
#     #             try:
#     #                 size = os.path.getsize(file_path_in_list)
#     #                 total_original_bytes_in_batch += size
#     #                 files_to_process_details.append({"path": file_path_in_list, "name": filename_in_list, "size": size})
#     #             except OSError as e: logging.warning(f"{log_prefix} Could not get size for {filename_in_list}, skipping. Error: {e}")
#     #         else: logging.warning(f"{log_prefix} File {filename_in_list} not found in {batch_directory_path}, skipping.")

#     #     if not files_to_process_details:
#     #         yield _yield_sse_event('error', {'message': 'No valid files found to upload in the batch.'})
#     #         _safe_remove_directory(batch_directory_path, log_prefix, "empty batch dir after file check")
#     #         return

#     #     yield _yield_sse_event('start', {'filename': batch_display_name, 'totalSize': total_original_bytes_in_batch})
#     #     yield _yield_sse_event('status', {'message': f'Uploading {len(files_to_process_details)} files...'})

#     #     overall_start_time = time.time()
#     #     bytes_sent_so_far = 0
#     #     all_files_metadata_for_db = [] 
#     #     batch_overall_success = True

#     #     for file_detail in files_to_process_details:
#     #         current_file_path = file_detail["path"]
#     #         current_filename = file_detail["name"]
#     #         current_file_size = file_detail["size"]
#     #         log_file_prefix_indiv = f"{log_prefix} File '{current_filename}'"

#     #         file_meta_entry: Dict[str, Any] = {
#     #             "original_filename": current_filename, "original_size": current_file_size,
#     #             "is_split": False, "is_compressed": current_filename.lower().endswith('.zip'),
#     #             "skipped": False, "failed": False, "reason": None,
#     #             "send_locations": [], "chunks": []          
#     #         }
            
#     #         mime_type_guess, _ = mimetypes.guess_type(current_filename)
#     #         file_meta_entry["mime_type"] = mime_type_guess if mime_type_guess else 'application/octet-stream'
            
#     #         logging.info(f"{log_file_prefix_indiv} Guessed MIME type: {file_meta_entry['mime_type']}")

#     #         if current_file_size == 0:
#     #             file_meta_entry["skipped"] = True; file_meta_entry["reason"] = "File is empty"
#     #             all_files_metadata_for_db.append(file_meta_entry)
#     #             current_batch_progress = _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch)
#     #             yield _yield_sse_event('progress', current_batch_progress)
#     #             continue 
            
#     #         try:
#     #             if current_file_size > TELEGRAM_MAX_CHUNK_SIZE_BYTES: # CHUNKING
#     #                 file_meta_entry["is_split"] = True
#     #                 part_number = 1; bytes_processed_for_this_file_chunking = 0
#     #                 all_chunks_sent_successfully_for_this_file = True
#     #                 with open(current_file_path, 'rb') as f_in:
#     #                     while True:
#     #                         chunk_data = f_in.read(TELEGRAM_MAX_CHUNK_SIZE_BYTES)
#     #                         if not chunk_data: break 
#     #                         chunk_filename = f"{current_filename}.part{part_number}"
#     #                         log_chunk_prefix = f"{log_file_prefix_indiv} Chunk {part_number}"
                            
#     #                         chunk_specific_futures: Dict[Future, str] = {}
#     #                         chunk_specific_results: Dict[str, ApiResult] = {}
#     #                         primary_send_success_for_this_chunk = False
#     #                         primary_send_message_for_this_chunk = "Primary chunk send failed."

#     #                         if executor:
#     #                             for chat_id_str_loop in TELEGRAM_CHAT_IDS:
#     #                                 fut = executor.submit(_send_chunk_task, chunk_data, chunk_filename, str(chat_id_str_loop), upload_id, part_number)
#     #                                 chunk_specific_futures[fut] = str(chat_id_str_loop)
#     #                         else: 
#     #                             _, res_tuple_no_exec_chunk = _send_chunk_task(chunk_data, chunk_filename, str(TELEGRAM_CHAT_IDS[0]), upload_id, part_number)
#     #                             chunk_specific_results[str(TELEGRAM_CHAT_IDS[0])] = res_tuple_no_exec_chunk
#     #                             primary_send_success_for_this_chunk, primary_send_message_for_this_chunk = res_tuple_no_exec_chunk[0], res_tuple_no_exec_chunk[1]
                            
#     #                         if chunk_specific_futures: 
#     #                             primary_chunk_fut = next((f for f, cid in chunk_specific_futures.items() if cid == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
#     #                             if primary_chunk_fut:
#     #                                 cid_res_chunk, res_chunk = primary_chunk_fut.result()
#     #                                 chunk_specific_results[cid_res_chunk] = res_chunk
#     #                                 primary_send_success_for_this_chunk, primary_send_message_for_this_chunk = res_chunk[0], res_chunk[1]
#     #                             for fut_completed_chunk in as_completed(chunk_specific_futures):
#     #                                 cid_res_c, res_c = fut_completed_chunk.result()
#     #                                 if cid_res_c not in chunk_specific_results: chunk_specific_results[cid_res_c] = res_c
                            
#     #                         parsed_locations_for_this_chunk = _parse_send_results(log_chunk_prefix, 
#     #                             [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k, r in chunk_specific_results.items()])

#     #                         if primary_send_success_for_this_chunk:
#     #                             file_meta_entry["chunks"].append({"part_number": part_number, "size": len(chunk_data), "send_locations": parsed_locations_for_this_chunk})
#     #                             bytes_sent_so_far += len(chunk_data)
#     #                             bytes_processed_for_this_file_chunking += len(chunk_data)
#     #                             yield _yield_sse_event('progress', _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch))
#     #                             yield _yield_sse_event('status', {'message': f'Uploaded chunk {part_number} for {current_filename}'})
#     #                         else:
#     #                             batch_overall_success = False; all_chunks_sent_successfully_for_this_file = False
#     #                             file_meta_entry["failed"] = True; file_meta_entry["reason"] = f"Failed chunk {part_number}: {primary_send_message_for_this_chunk}"
#     #                             break 
#     #                         part_number += 1
#     #                 if all_chunks_sent_successfully_for_this_file:
#     #                     file_meta_entry["compressed_total_size"] = bytes_processed_for_this_file_chunking
#     #             else: # SINGLE FILE (NON-CHUNKED)
#     #                 single_file_futures: Dict[Future, str] = {}
#     #                 single_file_results: Dict[str, ApiResult] = {}
#     #                 primary_send_success_for_single_file = False
#     #                 primary_send_message_single_file = "Primary send (single) failed."

#     #                 if executor:
#     #                     for chat_id_str_single in TELEGRAM_CHAT_IDS:
#     #                         fut_single = executor.submit(_send_single_file_task, current_file_path, current_filename, str(chat_id_str_single), upload_id)
#     #                         single_file_futures[fut_single] = str(chat_id_str_single)
#     #                 else:
#     #                     _, res_tuple_single_no_exec = _send_single_file_task(current_file_path, current_filename, str(TELEGRAM_CHAT_IDS[0]), upload_id)
#     #                     single_file_results[str(TELEGRAM_CHAT_IDS[0])] = res_tuple_single_no_exec
#     #                     primary_send_success_for_single_file, primary_send_message_single_file = res_tuple_single_no_exec[0], res_tuple_single_no_exec[1]

#     #                 if single_file_futures:
#     #                     primary_fut_single = next((f for f, cid in single_file_futures.items() if cid == str(PRIMARY_TELEGRAM_CHAT_ID)), None)
#     #                     if primary_fut_single:
#     #                         cid_res_s, res_s = primary_fut_single.result()
#     #                         single_file_results[cid_res_s] = res_s
#     #                         primary_send_success_for_single_file, primary_send_message_single_file = res_s[0], res_s[1]
#     #                     for fut_completed_s in as_completed(single_file_futures):
#     #                         cid_res_s_comp, res_s_comp = fut_completed_s.result()
#     #                         if cid_res_s_comp not in single_file_results: single_file_results[cid_res_s_comp] = res_s_comp
                    
#     #                 parsed_locations_single_file = _parse_send_results(log_file_prefix_indiv, 
#     #                     [{"chat_id": k, "success": r[0], "message": r[1], "tg_response": r[2]} for k,r in single_file_results.items()])

#     #                 if primary_send_success_for_single_file:
#     #                     bytes_sent_so_far += current_file_size
#     #                     file_meta_entry["send_locations"] = parsed_locations_single_file
#     #                 else:
#     #                     batch_overall_success = False; file_meta_entry["failed"] = True
#     #                     file_meta_entry["reason"] = f"Primary send failed (single): {primary_send_message_single_file}"
#     #                     file_meta_entry["send_locations"] = parsed_locations_single_file
#     #         except Exception as file_processing_exception:
#     #             file_meta_entry["failed"] = True; file_meta_entry["reason"] = f"Internal error: {str(file_processing_exception)}"
#     #             batch_overall_success = False
            
#     #         all_files_metadata_for_db.append(file_meta_entry)
#     #         yield _yield_sse_event('progress', _calculate_progress(overall_start_time, bytes_sent_so_far, total_original_bytes_in_batch))
#     #         yield _yield_sse_event('status', {'message': f'Processed {len(all_files_metadata_for_db)} of {len(files_to_process_details)} files...'})
        
#     #     total_batch_duration = time.time() - overall_start_time
#     #     if not all_files_metadata_for_db:
#     #         yield _yield_sse_event('error', {'message': 'Internal server error: Failed to record upload details.'})
#     #         upload_data['status'] = 'error'; upload_data['error'] = "No metadata generated"
#     #         return

#     #     db_batch_record = {
#     #         "access_id": access_id, "username": username,
#     #         "is_anonymous": upload_data.get('is_anonymous', False), "anonymous_id": upload_data.get('anonymous_id'),
#     #         "upload_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "is_batch": True,
#     #         "batch_display_name": batch_display_name, "files_in_batch": all_files_metadata_for_db,
#     #         "total_original_size": total_original_bytes_in_batch,
#     #         "total_upload_duration_seconds": round(total_batch_duration, 2),
#     #     }
#     #     if db_batch_record["anonymous_id"] is None: del db_batch_record["anonymous_id"]
        
#     #     save_success, save_msg = save_file_metadata(db_batch_record)
#     #     if not save_success:
#     #         upload_data['status'] = 'completed_metadata_error'; upload_data['error'] = f"DB save fail: {save_msg}"
#     #         yield _yield_sse_event('error', {'message': f"Upload processed, but failed to save details: {save_msg}"})
#     #         return
        
#     #     is_single_effective_file = not db_batch_record.get('is_batch', True) 
#     #     final_browser_url_filename_for_sse = ""
#     #     if is_single_effective_file:
#     #         # Link directly to the preview page.
#     #         # The filename for the queryParam should be the actual file's name.
#     #         actual_filename = all_files_metadata_for_db[0].get('original_filename', 'file')
#     #         browser_url = f"{request.host_url.rstrip('/')}/preview/{access_id}?filename={actual_filename}"
#     #         final_browser_url_filename_for_sse = actual_filename
#     #         logging.info(f"{log_prefix} Generated PREVIEW link for single file: {browser_url}")
#     #     else: # It's a true multi-file batch
#     #         # Link to the batch listing page
#     #         browser_url = f"{request.host_url.rstrip('/')}/batch-view/{access_id}"
#     #         final_browser_url_filename_for_sse = batch_display_name # For batch, filename in SSE is batch_display_name
#     #         logging.info(f"{log_prefix} Generated BATCH LISTING link for multi-file batch: {browser_url}")

#     #     # browser_url = f"{request.host_url.rstrip('/')}/browse/{access_id}" # Adjust browse route if needed
#     #     complete_message = f'Batch upload ' + ('completed with errors.' if not batch_overall_success else 'complete!')
#     #     complete_payload = {'message': complete_message, 'download_url': browser_url, 'filename': final_browser_url_filename_for_sse,'access_id': access_id, 'is_batch': not is_single_effective_file,
#     #                         'batch_access_id': access_id, }
#     #     upload_data['status'] = 'completed_with_errors' if not batch_overall_success else 'completed'
#     #     yield _yield_sse_event('complete', complete_payload)

#     # except Exception as e:
#     #     error_msg_final = f"Critical upload processing error: {str(e) or type(e).__name__}"
#     #     yield _yield_sse_event('error', {'message': error_msg_final})
#     #     if upload_id in upload_progress_data:
#     #         upload_progress_data[upload_id]['status'] = 'error'
#     #         upload_progress_data[upload_id]['error'] = error_msg_final
#     # finally:
#     #     if executor: executor.shutdown(wait=True)
#     #     if batch_directory_path and os.path.exists(batch_directory_path):
#     #          _safe_remove_directory(batch_directory_path, log_prefix, "batch temp dir in finally")
#     #     logging.info(f"{log_prefix} Upload processing generator finished. Final Status: {upload_progress_data.get(upload_id, {}).get('status', 'unknown')}")