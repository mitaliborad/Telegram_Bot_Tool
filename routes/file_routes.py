# file_routes.py
import logging
import io
import zipfile
from flask import Blueprint, request, make_response, jsonify, Response, url_for, render_template
from flask_jwt_extended import jwt_required, get_jwt_identity
import mimetypes # Ensure this is imported
from telegram_api import download_telegram_file_content
from .utils import _find_best_telegram_file_id, _find_filename_in_zip, get_preview_type
from dateutil import parser as dateutil_parser
from bson import ObjectId
from datetime import datetime, timedelta, timezone
import database
from database import (
    find_user_by_id,
    find_metadata_by_username,
    find_metadata_by_access_id,
    delete_metadata_by_access_id,
    get_archived_files_collection,
    # archive_file_record_by_access_id
)
from config import app , PRIMARY_TELEGRAM_CHAT_ID
from typing import Dict, Any, Tuple, Optional, List, Generator

# file_bp = Blueprint('file', __name__, template_folder='../templates/file')
file_bp = Blueprint('file_api', __name__)

@file_bp.route('/files/<username>', methods=['GET', 'OPTIONS'])
@jwt_required()
def list_user_files(username: str) -> Response:
    if request.method == 'OPTIONS':
        return make_response(), 200
    log_prefix = f"[ListFiles-{username}]"
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))

    if error or not user_doc:
        return jsonify({"error": "User not found or token invalid."}), 401
    jwt_username = user_doc.get('username')
    if not jwt_username: return jsonify({"error": "User identity error."}), 500
    if jwt_username != username:
         return jsonify({"error": "Forbidden: You can only list your own files."}), 403

    user_files, error_msg = find_metadata_by_username(username)
    if error_msg: return jsonify({"error": "Server error retrieving file list."}), 500

    serializable_files = []
    for file_record in (user_files or []):
        if '_id' in file_record and isinstance(file_record['_id'], ObjectId):
            file_record['_id'] = str(file_record['_id'])
        serializable_files.append(file_record)
    return jsonify(serializable_files)


@file_bp.route('/browse-page/<username>', methods=['GET'])
@jwt_required()
def list_user_files_page(username: str):
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
    if error or not user_doc or user_doc.get('username') != username:
        return jsonify({"error": "Unauthorized to view these files"}), 403

    user_files_data, db_error = find_metadata_by_username(username)
    if db_error:
        return jsonify({"error": "Could not retrieve files"}), 500

    return render_template('file/browse_files.html', username=username, files=user_files_data or [])

@file_bp.route('/delete-file/<username>/<path:access_id_from_path>', methods=['DELETE', 'OPTIONS'])
@jwt_required()
def delete_file_record(username: str, access_id_from_path: str) -> Response:
    if request.method == 'OPTIONS':
        return make_response(), 200
    log_prefix = f"[ArchiveFile-{access_id_from_path}]"
    try:
        current_user_jwt_identity = get_jwt_identity()
        user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
        if error or not user_doc: return jsonify({"error": "User not found or token invalid."}), 401

        requesting_username = user_doc.get('username')
        if not requesting_username: return jsonify({"error": "User identity error."}), 500

        record_to_archive, find_err = find_metadata_by_access_id(access_id_from_path)
        if find_err:
            logging.error(f"{log_prefix} Error finding record to archive: {find_err}")
            return jsonify({"error": "Server error checking record for archiving."}), 500
        if not record_to_archive:
            logging.warning(f"{log_prefix} Record ID '{access_id_from_path}' not found to archive.")
            return jsonify({"error": f"Record ID '{access_id_from_path}' not found."}), 404

        record_owner_username = record_to_archive.get('username')
        if record_owner_username != requesting_username:
            logging.warning(f"{log_prefix} User '{requesting_username}' attempt to archive record of '{record_owner_username}'.")
            return jsonify({"error": "Permission denied to archive this record."}), 403

        archived_record = record_to_archive.copy()
        archived_record["archived_timestamp"] = datetime.now(timezone.utc)
        archived_record["archived_by_username"] = requesting_username
        if '_id' in archived_record:
            del archived_record['_id']

        archived_collection, error_get_archived_coll = get_archived_files_collection()
        if error_get_archived_coll or archived_collection is None:
            logging.error(f"{log_prefix} Failed to get archived_files collection: {error_get_archived_coll}")
            return jsonify({"error": "Server error: Could not access archive."}), 500

        try:
            insert_result = archived_collection.insert_one(archived_record)
            if not insert_result.inserted_id:
                logging.error(f"{log_prefix} Failed to insert record into archive. No inserted_id.")
                return jsonify({"error": "Server error: Failed to archive record."}), 500
            logging.info(f"{log_prefix} Record inserted into archive with new ID: {insert_result.inserted_id}")
        except Exception as db_insert_e: # Changed variable name 'e' to 'db_insert_e'
            logging.error(f"{log_prefix} Exception inserting record into archive: {db_insert_e}", exc_info=True)
            return jsonify({"error": "Server error: Exception during archiving."}), 500

        deleted_count, db_error_msg = delete_metadata_by_access_id(access_id_from_path)
        if db_error_msg:
            logging.critical(f"{log_prefix} CRITICAL: Record archived (ID: {insert_result.inserted_id}) BUT failed to delete original from user_files (access_id: {access_id_from_path}). Error: {db_error_msg}")
            return jsonify({"error": "Server error: Record archived but failed to remove original. Please contact support."}), 500
        if deleted_count == 0:
            logging.warning(f"{log_prefix} Record archived (ID: {insert_result.inserted_id}) but original (access_id: {access_id_from_path}) was not found for deletion in user_files. Might have been deleted concurrently.")

        display_name = record_to_archive.get('batch_display_name', record_to_archive.get('original_filename', access_id_from_path)) # Changed filename_to_preview to original_filename
        logging.info(f"{log_prefix} Record '{display_name}' (access_id: {access_id_from_path}) successfully archived.")
        return jsonify({"message": f"Record '{display_name}' archived successfully."}), 200

    except Exception as e:
        logging.error(f"{log_prefix} UNEXPECTED EXCEPTION in delete_file_record: {e}", exc_info=True)
        return jsonify({"error": "An unexpected server error occurred during the archive process."}), 500

@file_bp.route('/delete-batch/<access_id>', methods=['DELETE'])
@jwt_required()
def delete_batch_record(access_id: str) -> Response:
    log_prefix = f"[ArchiveBatch-{access_id}]"
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
    if error or not user_doc: return jsonify({"error": "User not found/token invalid."}), 401

    requesting_username = user_doc.get('username')
    if not requesting_username: return jsonify({"error": "User identity error."}), 500

    record_to_archive, find_err = find_metadata_by_access_id(access_id)
    if find_err:
        logging.error(f"{log_prefix} Error finding batch to archive: {find_err}")
        return jsonify({"error": "Server error checking batch for archiving."}), 500
    if not record_to_archive:
        logging.warning(f"{log_prefix} Batch record '{access_id}' not found to archive.")
        return jsonify({"error": f"Batch record '{access_id}' not found."}), 404
    if not record_to_archive.get('is_batch', False):
        logging.warning(f"{log_prefix} Record '{access_id}' is not a batch, cannot archive as batch.")
        return jsonify({"error": f"Record '{access_id}' is not a batch."}), 400

    record_owner = record_to_archive.get('username')
    if record_owner != requesting_username:
        logging.warning(f"{log_prefix} User '{requesting_username}' attempt to archive batch of '{record_owner}'.")
        return jsonify({"error": "Permission denied to archive this batch."}), 403

    archived_record = record_to_archive.copy()
    archived_record["archived_timestamp"] = datetime.now(timezone.utc)
    archived_record["archived_by_username"] = requesting_username
    if '_id' in archived_record:
        del archived_record['_id']

    archived_collection, error_get_archived_coll = get_archived_files_collection()
    if error_get_archived_coll or archived_collection is None:
        logging.error(f"{log_prefix} Failed to get archived_files collection: {error_get_archived_coll}")
        return jsonify({"error": "Server error: Could not access archive."}), 500

    try:
        insert_result = archived_collection.insert_one(archived_record)
        if not insert_result.inserted_id:
            logging.error(f"{log_prefix} Failed to insert batch into archive. No inserted_id.")
            return jsonify({"error": "Server error: Failed to archive batch."}), 500
        logging.info(f"{log_prefix} Batch record inserted into archive with new ID: {insert_result.inserted_id}")
    except Exception as e:
        logging.error(f"{log_prefix} Exception inserting batch into archive: {e}", exc_info=True)
        return jsonify({"error": "Server error: Exception during batch archiving."}), 500

    deleted_count, db_error_msg = delete_metadata_by_access_id(access_id)
    if db_error_msg:
        logging.critical(f"{log_prefix} CRITICAL: Batch archived (ID: {insert_result.inserted_id}) BUT failed to delete original from user_files (access_id: {access_id}). Error: {db_error_msg}")
        return jsonify({"error": "Server error: Batch archived but failed to remove original. Please contact support."}), 500
    if deleted_count == 0:
        logging.warning(f"{log_prefix} Batch archived (ID: {insert_result.inserted_id}) but original (access_id: {access_id}) not found for deletion in user_files.")

    display_name = record_to_archive.get('batch_display_name', access_id)
    logging.info(f"{log_prefix} Batch '{display_name}' (access_id: {access_id}) successfully archived.")
    return jsonify({"message": f"Batch '{display_name}' archived successfully."}), 200

@file_bp.route('/api/file-details/<access_id>', methods=['GET'])
def api_get_single_file_details(access_id: str):
    file_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg: return jsonify({"error": "Server error."}), 500
    if not file_info: return jsonify({"error": f"File ID '{access_id}' not found."}), 404

    if file_info.get('is_batch'): 
        return jsonify({"error": "This ID is for a batch. Use /api/batch-details."}), 400
    
    # --- MODIFIED PART FOR original_size ---
    size_from_batch_item = None
    final_original_size = None # Default to None if not found

    files_in_batch_list = file_info.get("files_in_batch")
    if files_in_batch_list and isinstance(files_in_batch_list, list) and len(files_in_batch_list) > 0:
        first_file_item = files_in_batch_list[0]
        if 'original_size' in first_file_item: # Check if key exists
            size_from_batch_item = first_file_item['original_size']

    if size_from_batch_item is not None:
        final_original_size = size_from_batch_item
    elif 'original_size' in file_info: # Check top-level if not in files_in_batch item
        final_original_size = file_info['original_size']
    # If not found in either, final_original_size remains None
    # --- END OF MODIFICATION ---

    response_data = {
        "access_id": access_id,
        "filename_to_preview": file_info.get("files_in_batch", [{}])[0].get("original_filename") or \
                               file_info.get('original_filename', 'Unknown'),
        "original_size": final_original_size, # Use the determined size
        "upload_timestamp": file_info.get('upload_timestamp'),
        "username": file_info.get('username', 'N/A'),
        "is_compressed": file_info.get("files_in_batch", [{}])[0].get('is_compressed', False),
        "is_split": file_info.get("files_in_batch", [{}])[0].get('is_split', False),
    }
    return jsonify(response_data)



# @file_bp.route('/api/file-details/<access_id>', methods=['GET'])
# def api_get_single_file_details(access_id: str):
#     file_info, error_msg = find_metadata_by_access_id(access_id)
#     if error_msg: return jsonify({"error": "Server error."}), 500
#     if not file_info: return jsonify({"error": f"File ID '{access_id}' not found."}), 404

#     if file_info.get('is_batch'): 
#         return jsonify({"error": "This ID is for a batch. Use /api/batch-details."}), 400
    
#     response_data = {
#         "access_id": access_id,
#         "filename_to_preview": file_info.get("files_in_batch", [{}])[0].get("original_filename") or \
#                                file_info.get('original_filename', 'Unknown'),
#         "original_size": file_info.get("files_in_batch", [{}])[0].get("original_size") or \
#                          file_info.get('original_size', 0),
#         "upload_timestamp": file_info.get('upload_timestamp'),
#         "username": file_info.get('username', 'N/A'),
#         "is_compressed": file_info.get("files_in_batch", [{}])[0].get('is_compressed', False),
#         "is_split": file_info.get("files_in_batch", [{}])[0].get('is_split', False),
#     }
#     return jsonify(response_data)

# @file_bp.route('/batch-details/<access_id>', methods=['GET'])
# def api_get_batch_details(access_id: str):
#     log_prefix = f"[APIBatchDetails-{access_id}]"
#     batch_info, error_msg = find_metadata_by_access_id(access_id)
#     if error_msg:
#         logging.error(f"{log_prefix} Server error fetching metadata: {error_msg}")
#         return jsonify({"error": "Server error."}), 500
#     if not batch_info:
#         logging.warning(f"{log_prefix} Record not found.")
#         return jsonify({"error": f"Record ID '{access_id}' not found."}), 404

#     files_in_record = batch_info.get('files_in_batch', [])
#     if not files_in_record:
#         logging.warning(f"{log_prefix} Record '{access_id}' has no files in 'files_in_batch'. is_batch flag: {batch_info.get('is_batch')}")
#         return jsonify({"error": "Record contains no file information."}), 400

#     response_data = {
#         "batch_name": batch_info.get('batch_display_name', f"Files for {access_id}"),
#         "username": batch_info.get('username', 'N/A'),
#         "upload_date": batch_info.get('upload_timestamp'),
#         "total_size": batch_info.get('total_original_size', 0),
#         "files": files_in_record, 
#         "access_id": access_id,
#         "is_anonymous": batch_info.get('is_anonymous', False),
#         "upload_timestamp_raw": batch_info.get('upload_timestamp'),
#         "is_batch": batch_info.get('is_batch', False) 
#     }
    
#     processed_files = []
#     for f_item in response_data["files"]:
#         processed_f_item = f_item.copy()
#         processed_f_item.setdefault('original_filename', "Unknown File")
#         processed_f_item.setdefault('filename_to_preview', processed_f_item['original_filename'])
#         processed_f_item.setdefault('original_size', 0)
#         processed_f_item.setdefault('skipped', False)
#         processed_f_item.setdefault('failed', False)
#         processed_f_item.setdefault('reason', None)
#         processed_files.append(processed_f_item)
#     response_data["files"] = processed_files
    
#     logging.info(f"{log_prefix} Successfully retrieved batch details for access_id: {access_id}")
#     return jsonify(response_data)

@file_bp.route('/batch-details/<access_id>', methods=['GET'])
def api_get_batch_details(access_id: str):
    log_prefix = f"[APIBatchDetails-{access_id}]"
    batch_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg:
        logging.error(f"{log_prefix} Server error fetching metadata: {error_msg}")
        return jsonify({"error": "Server error."}), 500
    if not batch_info:
        logging.warning(f"{log_prefix} Record not found.")
        return jsonify({"error": f"Record ID '{access_id}' not found."}), 404

    files_in_record = batch_info.get('files_in_batch', [])
    if not files_in_record:
        logging.warning(f"{log_prefix} Record '{access_id}' has no files in 'files_in_batch'. is_batch flag: {batch_info.get('is_batch')}")
        return jsonify({"error": "Record contains no file information."}), 400

    response_data = {
        "batch_name": batch_info.get('batch_display_name', f"Files for {access_id}"),
        "username": batch_info.get('username', 'N/A'),
        "upload_date": batch_info.get('upload_timestamp'),
        "total_size": batch_info.get('total_original_size', 0),
        "files": files_in_record, 
        "access_id": access_id,
        "is_anonymous": batch_info.get('is_anonymous', False),
        "upload_timestamp_raw": batch_info.get('upload_timestamp'),
        "is_batch": batch_info.get('is_batch', False) 
    }
    
    processed_files = []
    for f_item in response_data["files"]: # f_item is a dict from batch_info['files_in_batch']
        processed_f_item = f_item.copy()
        processed_f_item.setdefault('original_filename', "Unknown File")
        processed_f_item.setdefault('filename_to_preview', processed_f_item['original_filename'])
        
        # --- MODIFIED PART FOR original_size ---
        if 'original_size' not in f_item: 
            # If 'original_size' key is missing in the item from the database
            processed_f_item['original_size'] = None 
        else:
            # If 'original_size' key exists, use its value (could be 0 or any other number)
            processed_f_item['original_size'] = f_item['original_size']
        # --- END OF MODIFICATION ---
            
        processed_f_item.setdefault('skipped', False)
        processed_f_item.setdefault('failed', False)
        processed_f_item.setdefault('reason', None)
        processed_files.append(processed_f_item)
    response_data["files"] = processed_files
    
    logging.info(f"{log_prefix} Successfully retrieved batch details for access_id: {access_id}")
    return jsonify(response_data)

# @file_bp.route('/preview-details/<access_id>', methods=['GET', 'OPTIONS'])
# def get_preview_details(access_id: str):
#     log_prefix = f"[PreviewDetails-{access_id}]"
#     filename_to_preview_query = request.args.get('filename')
#     if filename_to_preview_query:
#         log_prefix += f"-File-{filename_to_preview_query[:20]}"
#     logging.info(f"{log_prefix} Request received.")

#     top_level_record, error_msg_db = find_metadata_by_access_id(access_id)

#     if error_msg_db: 
#         logging.error(f"{log_prefix} Database error: {error_msg_db}")
#         return jsonify({"error": "Server error while fetching file information."}), 500
#     if not top_level_record:
#         logging.warning(f"{log_prefix} File or batch not found (access_id: {access_id}).")
#         return jsonify({"error": "File or batch not found."}), 404

#     target_file_info: Optional[Dict[str, Any]] = None
#     effective_filename_for_preview: Optional[str] = None

#     files_in_record_arr = top_level_record.get('files_in_batch', [])

#     if not files_in_record_arr: 
#         logging.error(f"{log_prefix} Record {access_id} has no 'files_in_batch' array or it's empty.")
#         return jsonify({"error": "Record contains no file information."}), 400

#     if top_level_record.get('is_batch', False): 
#         if not filename_to_preview_query:
#             logging.warning(f"{log_prefix} Accessing preview for a batch record without specifying a file.")
#             return jsonify({"error": "Please select a file from the batch to preview."}), 400
        
#         target_file_info = next((f for f in files_in_record_arr if f.get('original_filename') == filename_to_preview_query), None)
#         if not target_file_info:
#             logging.warning(f"{log_prefix} File '{filename_to_preview_query}' not found in batch {access_id}.")
#             return jsonify({"error": f"File '{filename_to_preview_query}' not found in batch."}), 404
#         effective_filename_for_preview = filename_to_preview_query
#     else: 
#         if len(files_in_record_arr) != 1:
#             logging.error(f"{log_prefix} Record {access_id} marked as non-batch but files_in_batch count is not 1.")
#             return jsonify({"error": "Inconsistent single file record."}), 500
#         target_file_info = files_in_record_arr[0]
#         effective_filename_for_preview = target_file_info.get('original_filename')
#         if filename_to_preview_query and filename_to_preview_query != effective_filename_for_preview:
#             logging.warning(f"{log_prefix} Query filename '{filename_to_preview_query}' does not match single file record's filename '{effective_filename_for_preview}'.")


#     if not target_file_info or not effective_filename_for_preview:
#         logging.error(f"{log_prefix} Could not determine target file info or effective filename for preview.")
#         return jsonify({"error": "Could not determine file to preview from the record."}), 500
    
#     mime_type = target_file_info.get('mime_type')
#     if not mime_type:
#         mime_type, _ = mimetypes.guess_type(effective_filename_for_preview)
#         mime_type = mime_type or 'application/octet-stream' 
#         logging.info(f"{log_prefix} Guessed MIME type for '{effective_filename_for_preview}': {mime_type}")

#     preview_type_str = get_preview_type(mime_type, effective_filename_for_preview)
#     response_data = {
#         "access_id": access_id,
#         "filename": effective_filename_for_preview, 
#         "size": target_file_info.get('original_size', 0),
#         "mime_type": mime_type,
#         "preview_type": preview_type_str,
#         "is_anonymous": top_level_record.get('is_anonymous', False),
#         "upload_timestamp": top_level_record.get('upload_timestamp'),
#         "preview_content_url": None,
#         "preview_data": None,
#     }

#     if response_data["is_anonymous"] and response_data["upload_timestamp"]:
#         logging.info(f"{log_prefix} Anonymous upload. Checking expiration. Timestamp: '{response_data['upload_timestamp']}'")
#         try:
#             upload_datetime = dateutil_parser.isoparse(response_data["upload_timestamp"])
#             if upload_datetime.tzinfo is None or upload_datetime.tzinfo.utcoffset(upload_datetime) is None:
#                 upload_datetime = upload_datetime.replace(tzinfo=timezone.utc)
#             expiration_days = app.config.get('ANONYMOUS_LINK_EXPIRATION_DAYS', 5)
#             expiration_limit = timedelta(days=expiration_days) 
#             if datetime.now(timezone.utc) > (upload_datetime + expiration_limit):
#                 logging.info(f"{log_prefix} Anonymous download link EXPIRED.")
#                 response_data["preview_type"] = "expired"
#                 return jsonify(response_data), 410
#             else:
#                 logging.info(f"{log_prefix} Anonymous download link still valid.")
#         except ValueError as e_parse:
#             logging.error(f"{log_prefix} Error parsing upload_timestamp '{response_data['upload_timestamp']}': {e_parse}", exc_info=True)
#             return jsonify({"error": "Error processing file metadata (invalid timestamp format)."}), 500
#         except Exception as e_exp: 
#             logging.error(f"{log_prefix} Unexpected error during expiration check: {e_exp}", exc_info=True)
#             return jsonify({"error": "Server error during file validation."}), 500

#     if response_data["preview_type"] not in ['unsupported', 'expired', 'directory_listing']: # directory_listing doesn't need content URL
#         response_data['preview_content_url'] = url_for(
#             'file_api.serve_raw_file_content', 
#             access_id=access_id, 
#             filename=effective_filename_for_preview, 
#             _external=False
#         )
#         logging.info(f"{log_prefix} Preview content URL set to: {response_data['preview_content_url']}")

#     logging.info(f"{log_prefix} Successfully prepared preview details: {response_data}")
#     return jsonify(response_data)


@file_bp.route('/preview-details/<access_id>', methods=['GET', 'OPTIONS'])
def get_preview_details(access_id: str):
    log_prefix = f"[PreviewDetails-{access_id}]"
    filename_to_preview_query = request.args.get('filename')
    if filename_to_preview_query:
        log_prefix += f"-File-{filename_to_preview_query[:20]}"
    logging.info(f"{log_prefix} Request received.")

    top_level_record, error_msg_db = find_metadata_by_access_id(access_id)

    if error_msg_db: 
        logging.error(f"{log_prefix} Database error: {error_msg_db}")
        return jsonify({"error": "Server error while fetching file information."}), 500
    if not top_level_record:
        logging.warning(f"{log_prefix} File or batch not found (access_id: {access_id}).")
        return jsonify({"error": "File or batch not found."}), 404

    target_file_info: Optional[Dict[str, Any]] = None
    effective_filename_for_preview: Optional[str] = None

    files_in_record_arr = top_level_record.get('files_in_batch', [])

    if not files_in_record_arr: 
        logging.error(f"{log_prefix} Record {access_id} has no 'files_in_batch' array or it's empty.")
        return jsonify({"error": "Record contains no file information."}), 400

    if top_level_record.get('is_batch', False): 
        if not filename_to_preview_query:
            logging.warning(f"{log_prefix} Accessing preview for a batch record without specifying a file.")
            return jsonify({"error": "Please select a file from the batch to preview."}), 400
        
        target_file_info = next((f for f in files_in_record_arr if f.get('original_filename') == filename_to_preview_query), None)
        if not target_file_info:
            logging.warning(f"{log_prefix} File '{filename_to_preview_query}' not found in batch {access_id}.")
            return jsonify({"error": f"File '{filename_to_preview_query}' not found in batch."}), 404
        effective_filename_for_preview = filename_to_preview_query
    else: 
        if len(files_in_record_arr) != 1:
            logging.error(f"{log_prefix} Record {access_id} marked as non-batch but files_in_batch count is not 1.")
            return jsonify({"error": "Inconsistent single file record."}), 500
        target_file_info = files_in_record_arr[0]
        effective_filename_for_preview = target_file_info.get('original_filename')
        if filename_to_preview_query and filename_to_preview_query != effective_filename_for_preview:
            logging.warning(f"{log_prefix} Query filename '{filename_to_preview_query}' does not match single file record's filename '{effective_filename_for_preview}'.")


    if not target_file_info or not effective_filename_for_preview:
        logging.error(f"{log_prefix} Could not determine target file info or effective filename for preview.")
        return jsonify({"error": "Could not determine file to preview from the record."}), 500
    
    mime_type = target_file_info.get('mime_type')
    if not mime_type:
        mime_type, _ = mimetypes.guess_type(effective_filename_for_preview)
        mime_type = mime_type or 'application/octet-stream' 
        logging.info(f"{log_prefix} Guessed MIME type for '{effective_filename_for_preview}': {mime_type}")

    preview_type_str = get_preview_type(mime_type, effective_filename_for_preview)
    file_size = None # Default to None
    if 'original_size' in target_file_info: # Check if key exists
        file_size = target_file_info['original_size']
    response_data = {
        "access_id": access_id,
        "filename": effective_filename_for_preview, 
        "size": file_size,
        "mime_type": mime_type,
        "preview_type": preview_type_str,
        "is_anonymous": top_level_record.get('is_anonymous', False),
        "upload_timestamp": top_level_record.get('upload_timestamp'),
        "preview_content_url": None,
        "preview_data": None,
    }

    if response_data["is_anonymous"] and response_data["upload_timestamp"]:
        logging.info(f"{log_prefix} Anonymous upload. Checking expiration. Timestamp: '{response_data['upload_timestamp']}'")
        try:
            upload_datetime = dateutil_parser.isoparse(response_data["upload_timestamp"])
            if upload_datetime.tzinfo is None or upload_datetime.tzinfo.utcoffset(upload_datetime) is None:
                upload_datetime = upload_datetime.replace(tzinfo=timezone.utc)
            expiration_days = app.config.get('ANONYMOUS_LINK_EXPIRATION_DAYS', 5)
            expiration_limit = timedelta(days=expiration_days) 
            if datetime.now(timezone.utc) > (upload_datetime + expiration_limit):
                logging.info(f"{log_prefix} Anonymous download link EXPIRED.")
                response_data["preview_type"] = "expired"
                return jsonify(response_data), 410
            else:
                logging.info(f"{log_prefix} Anonymous download link still valid.")
        except ValueError as e_parse:
            logging.error(f"{log_prefix} Error parsing upload_timestamp '{response_data['upload_timestamp']}': {e_parse}", exc_info=True)
            return jsonify({"error": "Error processing file metadata (invalid timestamp format)."}), 500
        except Exception as e_exp: 
            logging.error(f"{log_prefix} Unexpected error during expiration check: {e_exp}", exc_info=True)
            return jsonify({"error": "Server error during file validation."}), 500

    if response_data["preview_type"] not in ['unsupported', 'expired', 'directory_listing']: # directory_listing doesn't need content URL
        response_data['preview_content_url'] = url_for(
            'file_api.serve_raw_file_content', 
            access_id=access_id, 
            filename=effective_filename_for_preview, 
            _external=False
        )
        logging.info(f"{log_prefix} Preview content URL set to: {response_data['preview_content_url']}")

    logging.info(f"{log_prefix} Successfully prepared preview details: {response_data}")
    return jsonify(response_data)


def _get_final_file_content_for_preview(access_id: str, top_level_record: Dict[str, Any], specific_filename_from_query: Optional[str] = None) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    log_prefix_helper = f"[GetContentHelper-{access_id}]"
    if specific_filename_from_query:
        log_prefix_helper += f"-File-{specific_filename_from_query[:20]}"

    logging.info(f"{log_prefix_helper} Attempting to get final content.")
    actual_file_to_process_meta: Optional[Dict[str, Any]] = None
    effective_filename_for_processing: Optional[str] = None
    files_in_record_arr = top_level_record.get('files_in_batch', [])
    if not files_in_record_arr:
        return None, "Record contains no file information for content retrieval.", None

    if top_level_record.get('is_batch', False):
        if not specific_filename_from_query:
            if len(files_in_record_arr) == 1: 
                actual_file_to_process_meta = files_in_record_arr[0]
                effective_filename_for_processing = actual_file_to_process_meta.get('original_filename')
            else:
                return None, "Multi-file batch: specific filename required to get content.", None
        else: 
            actual_file_to_process_meta = next((f for f in files_in_record_arr if f.get('original_filename') == specific_filename_from_query), None)
            if not actual_file_to_process_meta:
                return None, f"File '{specific_filename_from_query}' not found in batch.", None
            effective_filename_for_processing = specific_filename_from_query
    else: 
        actual_file_to_process_meta = files_in_record_arr[0] 
        effective_filename_for_processing = actual_file_to_process_meta.get('original_filename')
        if specific_filename_from_query and specific_filename_from_query != effective_filename_for_processing:
            logging.warning(f"{log_prefix_helper} Query filename '{specific_filename_from_query}' for non-batch record differs from actual filename '{effective_filename_for_processing}'. Using actual.")
    if not actual_file_to_process_meta or not effective_filename_for_processing:
        return None, "Could not determine target file metadata or filename for content.", None
    
    logging.info(f"{log_prefix_helper} Determined target file for content: '{effective_filename_for_processing}'")
    is_split = actual_file_to_process_meta.get('is_split', False) 
    raw_downloaded_content: Optional[bytes] = None

    if is_split:
        logging.info(f"{log_prefix_helper} File '{effective_filename_for_processing}' is split. Reassembling...")
        chunks_meta = actual_file_to_process_meta.get('chunks') 
        if not chunks_meta:
            return None, "Split file metadata missing chunk information.", effective_filename_for_processing
        
        chunks_meta.sort(key=lambda c: int(c.get('part_number', 0)))
        reassembled_buffer = io.BytesIO()
        for i, chunk_info in enumerate(chunks_meta):
            part_num = chunk_info.get("part_number", i + 1)
            chunk_tg_file_id, _ = _find_best_telegram_file_id(chunk_info.get("send_locations", []), PRIMARY_TELEGRAM_CHAT_ID)
            if not chunk_tg_file_id:
                return None, f"Missing Telegram ID for chunk {part_num}.", effective_filename_for_processing
            
            content_part, err_part = download_telegram_file_content(chunk_tg_file_id)
            if err_part or not content_part:
                return None, f"Error downloading/empty content for chunk {part_num}: {err_part}", effective_filename_for_processing
            reassembled_buffer.write(content_part)
        
        raw_downloaded_content = reassembled_buffer.getvalue()
        reassembled_buffer.close()
    else: 
        send_locations = actual_file_to_process_meta.get('send_locations') or actual_file_to_process_meta.get('telegram_send_locations')
        tg_file_id, _ = _find_best_telegram_file_id(send_locations, PRIMARY_TELEGRAM_CHAT_ID)
        if not tg_file_id:
            return None, "Missing Telegram ID for non-split file.", effective_filename_for_processing
        raw_downloaded_content, err_download = download_telegram_file_content(tg_file_id)
        if err_download or not raw_downloaded_content:
            return None, f"Error downloading/empty content for non-split file: {err_download}", effective_filename_for_processing
    final_content_to_serve: bytes = raw_downloaded_content
    filename_for_mime_type_final: str = effective_filename_for_processing

    is_compressed_by_uploader = actual_file_to_process_meta.get('is_compressed_for_telegram', actual_file_to_process_meta.get('is_compressed', False))
    if is_compressed_by_uploader and not effective_filename_for_processing.lower().endswith('.zip'):
        logging.info(f"{log_prefix_helper} Decompressing '{effective_filename_for_processing}' (was compressed by uploader).")
        try:
            with zipfile.ZipFile(io.BytesIO(raw_downloaded_content), 'r') as zf:
                entry_name_to_extract = _find_filename_in_zip(zf, effective_filename_for_processing, log_prefix_helper)
                final_content_to_serve = zf.read(entry_name_to_extract)
        except Exception as e_zip:
            return None, f"Failed to decompress file '{effective_filename_for_processing}': {e_zip}", effective_filename_for_processing
    
    return final_content_to_serve, None, filename_for_mime_type_final

@file_bp.route('/file-content/<access_id>', methods=['GET'])
def serve_raw_file_content(access_id: str):
    log_prefix = f"[ServeRawContent-{access_id}]"
    filename_from_query = request.args.get('filename') 
    if filename_from_query:
        log_prefix += f"-File-{filename_from_query[:20]}"
    logging.info(f"{log_prefix} Request received to serve raw content.")

    top_level_record, error_msg_db = find_metadata_by_access_id(access_id)
    if error_msg_db or not top_level_record:
        return make_response(f"Error fetching record: {error_msg_db or 'Not found'}", 500 if error_msg_db else 404)
    if top_level_record.get("is_anonymous") and top_level_record.get("upload_timestamp"):
        logging.info(f"{log_prefix} Anonymous upload. Checking expiration. Timestamp: '{top_level_record['upload_timestamp']}'")
        try:
            upload_datetime = dateutil_parser.isoparse(top_level_record["upload_timestamp"])
            if upload_datetime.tzinfo is None or upload_datetime.tzinfo.utcoffset(upload_datetime) is None:
                upload_datetime = upload_datetime.replace(tzinfo=timezone.utc)
            expiration_days = app.config.get('ANONYMOUS_LINK_EXPIRATION_DAYS', 5)
            expiration_limit = timedelta(days=expiration_days) 
            if datetime.now(timezone.utc) > (upload_datetime + expiration_limit):
                logging.info(f"{log_prefix} Anonymous content link EXPIRED.")
                return make_response("File link has expired.", 410) 
        except Exception as e_exp:
            logging.error(f"{log_prefix} Error during expiration check for content: {e_exp}", exc_info=True)
            return make_response("Error validating file expiration.", 500)


    content_bytes, error_msg_content, filename_for_mime = _get_final_file_content_for_preview(
        access_id, 
        top_level_record, 
        filename_from_query 
    )

    if error_msg_content:
        logging.error(f"{log_prefix} Error getting final file content: {error_msg_content}")
        return make_response(error_msg_content, 500 if "Telegram ID" not in error_msg_content else 404) # Be more specific on 404 if ID missing
    if not content_bytes:
        logging.error(f"{log_prefix} Final content bytes are None or empty unexpectedly.")
        return make_response("Failed to retrieve file content. It might be missing or corrupted.", 500)
    if not filename_for_mime:
        filename_for_mime = filename_from_query or "unknown.dat"
        logging.warning(f"{log_prefix} Filename for MIME type determination is missing, using fallback: {filename_for_mime}")
    final_mime_type, _ = mimetypes.guess_type(filename_for_mime)
    final_mime_type = final_mime_type or 'application/octet-stream'
    
    logging.info(f"{log_prefix} Serving content for '{filename_for_mime}' with MIME type: {final_mime_type}. Size: {len(content_bytes)}")
    return Response(content_bytes, mimetype=final_mime_type)