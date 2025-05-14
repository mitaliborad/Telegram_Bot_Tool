# file_routes.py
import logging
from flask import Blueprint, request, make_response, jsonify, Response, url_for, render_template # Added render_template, url_for
from flask_jwt_extended import jwt_required, get_jwt_identity
from bson import ObjectId

import database
from database import (
    find_metadata_by_username, find_metadata_by_access_id, 
    delete_metadata_by_access_id # Changed from delete_metadata_by_filename
)
from config import app # Import app for direct routes if any, or config constants

file_bp = Blueprint('file', __name__, template_folder='../templates/file')

@file_bp.route('/files/<username>', methods=['GET', 'OPTIONS']) # Added OPTIONS
@jwt_required()
def list_user_files(username: str) -> Response:
    if request.method == 'OPTIONS':
        return make_response(), 204 # CORS preflight

    log_prefix = f"[ListFiles-{username}]"
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))

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

# This is an example if you want a server-rendered page for listing files.
# If your frontend handles this, this route might not be needed.
@file_bp.route('/browse-page/<username>') # Example name, adjust as needed
@jwt_required() # Or use Flask-Login's @login_required if it's for server-side sessions
def list_user_files_page(username: str):
    # Similar authorization check as list_user_files API
    current_user_jwt_identity = get_jwt_identity() # Or current_user from Flask-Login
    user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))
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
    log_prefix = f"[DeleteFile-{access_id_from_path}]"
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))
    if error or not user_doc: return jsonify({"error": "User not found or token invalid."}), 401
    requesting_username = user_doc.get('username')
    if not requesting_username: return jsonify({"error": "User identity error."}), 500

    record_to_delete, find_err = find_metadata_by_access_id(access_id_from_path)
    if find_err: return jsonify({"error": "Server error checking record."}), 500
    if not record_to_delete: return jsonify({"error": f"Record ID '{access_id_from_path}' not found."}), 404

    record_owner_username = record_to_delete.get('username')
    # Add anonymous check if needed: record_to_delete.get('is_anonymous', False) and record_to_delete.get('anonymous_id')
    if record_owner_username != requesting_username:
        return jsonify({"error": "Permission denied to delete this record."}), 403 

    deleted_count, db_error_msg = delete_metadata_by_access_id(access_id_from_path)
    if db_error_msg: return jsonify({"error": "Server error during deletion."}), 500
    if deleted_count == 0:
        return jsonify({"error": f"Failed to delete record '{access_id_from_path}'. May have been already deleted."}), 404
    
    display_name = record_to_delete.get('batch_display_name', record_to_delete.get('original_filename', access_id_from_path))
    return jsonify({"message": f"Record '{display_name}' deleted successfully."}), 200


@file_bp.route('/delete-batch/<access_id>', methods=['DELETE']) # Kept for explicitness, could merge with above
@jwt_required() 
def delete_batch_record(access_id: str) -> Response:
    # This is largely identical to delete_file_record now that both use access_id
    # You can call delete_file_record or repeat the logic. For clarity, repeating slightly modified.
    log_prefix = f"[DeleteBatch-{access_id}]"
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = database.find_user_by_id(ObjectId(current_user_jwt_identity))
    if error or not user_doc: return jsonify({"error": "User not found/token invalid."}), 401
    requesting_username = user_doc.get('username')
    if not requesting_username: return jsonify({"error": "User identity error."}), 500

    record_to_delete, find_err = find_metadata_by_access_id(access_id)
    if find_err: return jsonify({"error": "Server error checking record."}), 500
    if not record_to_delete: return jsonify({"error": f"Batch record '{access_id}' not found."}), 404
    if not record_to_delete.get('is_batch', False): # Ensure it's a batch
        return jsonify({"error": f"Record '{access_id}' is not a batch."}), 400


    record_owner = record_to_delete.get('username')
    if record_owner != requesting_username:
        return jsonify({"error": "Permission denied to delete this batch."}), 403

    deleted_count, db_error_msg = delete_metadata_by_access_id(access_id)
    if db_error_msg: return jsonify({"error": "Server error during batch deletion."}), 500
    if deleted_count == 0: return jsonify({"error": f"Failed to delete batch '{access_id}'."}), 500 # Or 404 if might be already gone
    
    display_name = record_to_delete.get('batch_display_name', access_id)
    return jsonify({"message": f"Batch '{display_name}' deleted successfully."}), 200


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