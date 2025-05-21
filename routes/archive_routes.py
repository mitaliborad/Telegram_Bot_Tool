import logging
from flask import Blueprint, request, make_response, jsonify, Response
from flask_jwt_extended import jwt_required, get_jwt_identity
from bson import ObjectId
from datetime import datetime, timezone 

import database 
from database import (
    find_user_by_id,                          
    find_archived_metadata_by_username,
    find_archived_metadata_by_access_id,
    save_file_metadata,                       
    delete_archived_metadata_by_access_id 

)

archive_bp = Blueprint('archive', __name__, url_prefix='/archive') 

@archive_bp.route('/list-files/<username>', methods=['GET', 'OPTIONS'])
@jwt_required()
def list_user_archived_files(username: str) -> Response:
    if request.method == 'OPTIONS':
        return make_response(), 204 # CORS preflight

    log_prefix = f"[ListArchivedFiles-{username}]"
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))

    if error or not user_doc:
        logging.warning(f"{log_prefix} User not found or token invalid for JWT ID: {current_user_jwt_identity}")
        return jsonify({"error": "User not found or token invalid."}), 401

    jwt_username = user_doc.get('username')
    if not jwt_username:
        logging.error(f"{log_prefix} User identity error for JWT ID: {current_user_jwt_identity}")
        return jsonify({"error": "User identity error."}), 500

    if jwt_username != username:
        logging.warning(f"{log_prefix} Forbidden attempt by '{jwt_username}' to access archived files of '{username}'.")
        return jsonify({"error": "Forbidden: You can only list your own archived files."}), 403

    archived_files, error_msg = find_archived_metadata_by_username(username)

    if error_msg:
        logging.error(f"{log_prefix} Server error retrieving archived file list: {error_msg}")
        return jsonify({"error": "Server error retrieving archived file list."}), 500

    return jsonify(archived_files or []) 


@archive_bp.route('/restore-file/<access_id>', methods=['POST', 'OPTIONS'])
@jwt_required()
def restore_archived_file(access_id: str) -> Response:
    if request.method == 'OPTIONS':
        return make_response(), 204 # CORS preflight

    log_prefix = f"[RestoreFile-{access_id}]"
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))

    if error or not user_doc:
        logging.warning(f"{log_prefix} User not found or token invalid for JWT ID: {current_user_jwt_identity}")
        return jsonify({"error": "User not found or token invalid."}), 401

    requesting_username = user_doc.get('username')
    if not requesting_username:
        logging.error(f"{log_prefix} User identity error for JWT ID: {current_user_jwt_identity}")
        return jsonify({"error": "User identity error."}), 500

    # 1. Find the record in the 'archived_files' collection
    archived_record, find_err = find_archived_metadata_by_access_id(access_id)
    if find_err:
        logging.error(f"{log_prefix} Error finding record to restore: {find_err}")
        return jsonify({"error": "Server error checking record for restoration."}), 500
    if not archived_record:
        logging.warning(f"{log_prefix} Archived record ID '{access_id}' not found to restore.")
        return jsonify({"error": f"Archived record ID '{access_id}' not found."}), 404

    # 2. Verify ownership
    archived_record_owner = archived_record.get('username')
    if archived_record_owner != requesting_username:
        logging.warning(f"{log_prefix} User '{requesting_username}' attempt to restore record of '{archived_record_owner}'.")
        return jsonify({"error": "Permission denied to restore this record."}), 403

    # 3. Prepare the record for 'user_files' collection (remove archive-specific fields)
    record_to_restore = archived_record.copy()
    if "archived_timestamp" in record_to_restore:
        del record_to_restore["archived_timestamp"]
    if "archived_by_username" in record_to_restore:
        del record_to_restore["archived_by_username"]
    if '_id' in record_to_restore: # The _id from archived_files collection
        del record_to_restore['_id']
    # Note: 'access_id' is preserved, which is correct.

    # 4. Insert (or upsert) into 'user_files' collection
    # Using save_file_metadata which handles upsert based on access_id
    success, message = save_file_metadata(record_to_restore)
    if not success:
        logging.error(f"{log_prefix} Failed to save restored record to user_files: {message}")
        return jsonify({"error": f"Server error: Failed to restore record. {message}"}), 500
    logging.info(f"{log_prefix} Record (access_id: {access_id}) successfully saved back to user_files.")

    # 5. Delete from the 'archived_files' collection
    deleted_count, del_arc_err = delete_archived_metadata_by_access_id(access_id)
    if del_arc_err:
        # CRITICAL: Record restored to user_files, but failed to delete from archive.
        # This could lead to it appearing in both lists.
        logging.critical(f"{log_prefix} CRITICAL: Record restored to user_files BUT failed to delete from archive (access_id: {access_id}). Error: {del_arc_err}")
        return jsonify({"error": "Server error: Record restored but failed to remove from archive. Please contact support."}), 500
    if deleted_count == 0:
        logging.warning(f"{log_prefix} Record restored, but original (access_id: {access_id}) not found for deletion in archive. Might have been deleted concurrently.")
        # This is less critical if the main goal (restoring) was met.

    display_name = record_to_restore.get('batch_display_name', record_to_restore.get('original_filename', access_id))
    logging.info(f"{log_prefix} Record '{display_name}' (access_id: {access_id}) successfully restored.")
    return jsonify({"message": f"Record '{display_name}' restored successfully."}), 200
