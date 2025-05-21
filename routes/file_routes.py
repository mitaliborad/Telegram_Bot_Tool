# file_routes.py
import logging
from flask import Blueprint, request, make_response, jsonify, Response, url_for, render_template
from flask_jwt_extended import jwt_required, get_jwt_identity
from bson import ObjectId
from datetime import datetime, timezone
import database
from database import (
    find_user_by_id,             
    find_metadata_by_username,   
    find_metadata_by_access_id,  
    delete_metadata_by_access_id, 
    get_archived_files_collection, 
    archive_file_record_by_access_id
)
from config import app 

file_bp = Blueprint('file', __name__, template_folder='../templates/file')

@file_bp.route('/files/<username>', methods=['GET', 'OPTIONS']) 
@jwt_required()
def list_user_files(username: str) -> Response:
    if request.method == 'OPTIONS':
        return make_response(), 204 # CORS preflight

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


@file_bp.route('/browse-page/<username>') 
@jwt_required() 
def list_user_files_page(username: str):
    # Similar authorization check as list_user_files API
    current_user_jwt_identity = get_jwt_identity() # Or current_user from Flask-Login
    user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
    if error or not user_doc or user_doc.get('username') != username:
        # Handle error, perhaps flash a message and redirect
        return jsonify({"error": "Unauthorized to view these files"}), 403 # Or redirect

    # Fetch files for rendering
    user_files_data, db_error = find_metadata_by_username(username)
    if db_error:
        # Handle error
        return jsonify({"error": "Could not retrieve files"}), 500
    
    # Assuming you have a template, e.g., 'browse_files.html' in 'templates' or 'templates/file'
    return render_template('file/browse_files.html', username=username, files=user_files_data or [])

@file_bp.route('/delete-file/<username>/<path:access_id_from_path>', methods=['DELETE'])
@jwt_required()
def delete_file_record(username: str, access_id_from_path: str) -> Response:
    log_prefix = f"[ArchiveFile-{access_id_from_path}]" 
    try:
        current_user_jwt_identity = get_jwt_identity()
        user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
        if error or not user_doc: return jsonify({"error": "User not found or token invalid."}), 401

        requesting_username = user_doc.get('username')
        if not requesting_username: return jsonify({"error": "User identity error."}), 500

        # 1. Find the record in the main 'user_files' collection
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

        # 2. Prepare the record for the 'archived_files' collection
        archived_record = record_to_archive.copy() # Make a copy
        archived_record["archived_timestamp"] = datetime.now(timezone.utc)
        archived_record["archived_by_username"] = requesting_username
        if '_id' in archived_record: # MongoDB will generate a new _id on insert
            del archived_record['_id']

        # 3. Insert into 'archived_files' collection
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
        except Exception as db_insert_e:
            logging.error(f"{log_prefix} Exception inserting record into archive: {e}", exc_info=True)
            return jsonify({"error": "Server error: Exception during archiving."}), 500

        # 4. Delete from the original 'user_files' collection
        deleted_count, db_error_msg = delete_metadata_by_access_id(access_id_from_path)
        if db_error_msg:
            logging.critical(f"{log_prefix} CRITICAL: Record archived (ID: {insert_result.inserted_id}) BUT failed to delete original from user_files (access_id: {access_id_from_path}). Error: {db_error_msg}")
            return jsonify({"error": "Server error: Record archived but failed to remove original. Please contact support."}), 500
        if deleted_count == 0:
            logging.warning(f"{log_prefix} Record archived (ID: {insert_result.inserted_id}) but original (access_id: {access_id_from_path}) was not found for deletion in user_files. Might have been deleted concurrently.")

        display_name = record_to_archive.get('batch_display_name', record_to_archive.get('original_filename', access_id_from_path))
        logging.info(f"{log_prefix} Record '{display_name}' (access_id: {access_id_from_path}) successfully archived.")
        return jsonify({"message": f"Record '{display_name}' archived successfully."}), 200
    
    except Exception as e: # General catch-all for unexpected errors in the route
        logging.error(f"{log_prefix} UNEXPECTED EXCEPTION in delete_file_record: {e}", exc_info=True)
        return jsonify({"error": "An unexpected server error occurred during the archive process."}), 500
    
@file_bp.route('/delete-batch/<access_id>', methods=['DELETE'])
@jwt_required()
def delete_batch_record(access_id: str) -> Response:
    log_prefix = f"[ArchiveBatch-{access_id}]" # Changed log prefix
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
    if error or not user_doc: return jsonify({"error": "User not found/token invalid."}), 401

    requesting_username = user_doc.get('username')
    if not requesting_username: return jsonify({"error": "User identity error."}), 500

    # 1. Find the batch record in 'user_files'
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

    # 2. Prepare the record for 'archived_files'
    archived_record = record_to_archive.copy()
    archived_record["archived_timestamp"] = datetime.now(timezone.utc)
    archived_record["archived_by_username"] = requesting_username
    if '_id' in archived_record:
        del archived_record['_id']

    # 3. Insert into 'archived_files'
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

    # 4. Delete from 'user_files'
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
    if file_info.get('is_batch'): return jsonify({"error": "This ID is for a batch."}), 400
    
    response_data = {
        "access_id": access_id,
        "original_filename": file_info.get('original_filename', 'Unknown'),
        "original_size": file_info.get('original_size', 0),
        "upload_timestamp": file_info.get('upload_timestamp'),
        "username": file_info.get('username', 'N/A'),
        "is_compressed": file_info.get('is_compressed', False), 
        "is_split": file_info.get('is_split', False),         
    }
    return jsonify(response_data)

@file_bp.route('/api/batch-details/<access_id>', methods=['GET'])
def api_get_batch_details(access_id: str):
    batch_info, error_msg = find_metadata_by_access_id(access_id)
    if error_msg: return jsonify({"error": "Server error."}), 500
    if not batch_info: return jsonify({"error": f"Batch ID '{access_id}' not found."}), 404
    if not batch_info.get('is_batch'): return jsonify({"error": "This ID is not for a batch."}), 400

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
        processed_f_item.setdefault('original_filename', "Unknown File")
        processed_f_item.setdefault('original_size', 0)
        processed_f_item.setdefault('skipped', False)
        processed_f_item.setdefault('failed', False)
        processed_f_item.setdefault('reason', None)
        processed_files.append(processed_f_item)
    response_data["files"] = processed_files
    return jsonify(response_data)

