# # file_routes.py
# import logging
# import io
# import zipfile
# from flask import Blueprint, request, make_response, jsonify, Response, url_for, render_template
# from flask_jwt_extended import jwt_required, get_jwt_identity
# import mimetypes
# from telegram_api import download_telegram_file_content
# from .utils import _find_best_telegram_file_id, _find_filename_in_zip, get_preview_type
# from dateutil import parser as dateutil_parser
# from bson import ObjectId
# from datetime import datetime, timedelta, timezone
# import database
# from database import (
#     find_user_by_id,             
#     find_metadata_by_username,   
#     find_metadata_by_access_id,  
#     delete_metadata_by_access_id, 
#     get_archived_files_collection, 
#     archive_file_record_by_access_id
# )
# from config import app , PRIMARY_TELEGRAM_CHAT_ID
# from typing import Dict, Any, Tuple, Optional, List, Generator

# file_bp = Blueprint('file', __name__, template_folder='../templates/file')

# @file_bp.route('/files/<username>', methods=['GET', 'OPTIONS']) 
# @jwt_required()
# def list_user_files(username: str) -> Response:
#     if request.method == 'OPTIONS':
#         return make_response(), 204 # CORS preflight

#     log_prefix = f"[ListFiles-{username}]"
#     current_user_jwt_identity = get_jwt_identity()
#     user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))

#     if error or not user_doc:
#         return jsonify({"error": "User not found or token invalid."}), 401
#     jwt_username = user_doc.get('username')
#     if not jwt_username: return jsonify({"error": "User identity error."}), 500
#     if jwt_username != username:
#          return jsonify({"error": "Forbidden: You can only list your own files."}), 403

#     user_files, error_msg = find_metadata_by_username(username)
#     if error_msg: return jsonify({"error": "Server error retrieving file list."}), 500
    
#     serializable_files = []
#     for file_record in (user_files or []):
#         if '_id' in file_record and isinstance(file_record['_id'], ObjectId):
#             file_record['_id'] = str(file_record['_id'])
#         serializable_files.append(file_record)
#     return jsonify(serializable_files)


# @file_bp.route('/browse-page/<username>') 
# @jwt_required() 
# def list_user_files_page(username: str):
#     # Similar authorization check as list_user_files API
#     current_user_jwt_identity = get_jwt_identity() # Or current_user from Flask-Login
#     user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
#     if error or not user_doc or user_doc.get('username') != username:
#         # Handle error, perhaps flash a message and redirect
#         return jsonify({"error": "Unauthorized to view these files"}), 403 # Or redirect

#     # Fetch files for rendering
#     user_files_data, db_error = find_metadata_by_username(username)
#     if db_error:
#         # Handle error
#         return jsonify({"error": "Could not retrieve files"}), 500
    
#     # Assuming you have a template, e.g., 'browse_files.html' in 'templates' or 'templates/file'
#     return render_template('file/browse_files.html', username=username, files=user_files_data or [])

# @file_bp.route('/delete-file/<username>/<path:access_id_from_path>', methods=['DELETE'])
# @jwt_required()
# def delete_file_record(username: str, access_id_from_path: str) -> Response:
#     log_prefix = f"[ArchiveFile-{access_id_from_path}]" 
#     try:
#         current_user_jwt_identity = get_jwt_identity()
#         user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
#         if error or not user_doc: return jsonify({"error": "User not found or token invalid."}), 401

#         requesting_username = user_doc.get('username')
#         if not requesting_username: return jsonify({"error": "User identity error."}), 500

#         # 1. Find the record in the main 'user_files' collection
#         record_to_archive, find_err = find_metadata_by_access_id(access_id_from_path)
#         if find_err:
#             logging.error(f"{log_prefix} Error finding record to archive: {find_err}")
#             return jsonify({"error": "Server error checking record for archiving."}), 500
#         if not record_to_archive:
#             logging.warning(f"{log_prefix} Record ID '{access_id_from_path}' not found to archive.")
#             return jsonify({"error": f"Record ID '{access_id_from_path}' not found."}), 404

#         record_owner_username = record_to_archive.get('username')
#         if record_owner_username != requesting_username:
#             logging.warning(f"{log_prefix} User '{requesting_username}' attempt to archive record of '{record_owner_username}'.")
#             return jsonify({"error": "Permission denied to archive this record."}), 403

#         # 2. Prepare the record for the 'archived_files' collection
#         archived_record = record_to_archive.copy() # Make a copy
#         archived_record["archived_timestamp"] = datetime.now(timezone.utc)
#         archived_record["archived_by_username"] = requesting_username
#         if '_id' in archived_record: # MongoDB will generate a new _id on insert
#             del archived_record['_id']

#         # 3. Insert into 'archived_files' collection
#         archived_collection, error_get_archived_coll = get_archived_files_collection()
#         if error_get_archived_coll or archived_collection is None:
#             logging.error(f"{log_prefix} Failed to get archived_files collection: {error_get_archived_coll}")
#             return jsonify({"error": "Server error: Could not access archive."}), 500

#         try:
#             insert_result = archived_collection.insert_one(archived_record)
#             if not insert_result.inserted_id:
#                 logging.error(f"{log_prefix} Failed to insert record into archive. No inserted_id.")
#                 return jsonify({"error": "Server error: Failed to archive record."}), 500
#             logging.info(f"{log_prefix} Record inserted into archive with new ID: {insert_result.inserted_id}")
#         except Exception as db_insert_e:
#             logging.error(f"{log_prefix} Exception inserting record into archive: {e}", exc_info=True)
#             return jsonify({"error": "Server error: Exception during archiving."}), 500

#         # 4. Delete from the original 'user_files' collection
#         deleted_count, db_error_msg = delete_metadata_by_access_id(access_id_from_path)
#         if db_error_msg:
#             logging.critical(f"{log_prefix} CRITICAL: Record archived (ID: {insert_result.inserted_id}) BUT failed to delete original from user_files (access_id: {access_id_from_path}). Error: {db_error_msg}")
#             return jsonify({"error": "Server error: Record archived but failed to remove original. Please contact support."}), 500
#         if deleted_count == 0:
#             logging.warning(f"{log_prefix} Record archived (ID: {insert_result.inserted_id}) but original (access_id: {access_id_from_path}) was not found for deletion in user_files. Might have been deleted concurrently.")

#         display_name = record_to_archive.get('batch_display_name', record_to_archive.get('filename_to_preview', access_id_from_path))
#         logging.info(f"{log_prefix} Record '{display_name}' (access_id: {access_id_from_path}) successfully archived.")
#         return jsonify({"message": f"Record '{display_name}' archived successfully."}), 200
    
#     except Exception as e: # General catch-all for unexpected errors in the route
#         logging.error(f"{log_prefix} UNEXPECTED EXCEPTION in delete_file_record: {e}", exc_info=True)
#         return jsonify({"error": "An unexpected server error occurred during the archive process."}), 500
    
# @file_bp.route('/delete-batch/<access_id>', methods=['DELETE'])
# @jwt_required()
# def delete_batch_record(access_id: str) -> Response:
#     log_prefix = f"[ArchiveBatch-{access_id}]" # Changed log prefix
#     current_user_jwt_identity = get_jwt_identity()
#     user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
#     if error or not user_doc: return jsonify({"error": "User not found/token invalid."}), 401

#     requesting_username = user_doc.get('username')
#     if not requesting_username: return jsonify({"error": "User identity error."}), 500

#     # 1. Find the batch record in 'user_files'
#     record_to_archive, find_err = find_metadata_by_access_id(access_id)
#     if find_err:
#         logging.error(f"{log_prefix} Error finding batch to archive: {find_err}")
#         return jsonify({"error": "Server error checking batch for archiving."}), 500
#     if not record_to_archive:
#         logging.warning(f"{log_prefix} Batch record '{access_id}' not found to archive.")
#         return jsonify({"error": f"Batch record '{access_id}' not found."}), 404
#     if not record_to_archive.get('is_batch', False):
#         logging.warning(f"{log_prefix} Record '{access_id}' is not a batch, cannot archive as batch.")
#         return jsonify({"error": f"Record '{access_id}' is not a batch."}), 400

#     record_owner = record_to_archive.get('username')
#     if record_owner != requesting_username:
#         logging.warning(f"{log_prefix} User '{requesting_username}' attempt to archive batch of '{record_owner}'.")
#         return jsonify({"error": "Permission denied to archive this batch."}), 403

#     # 2. Prepare the record for 'archived_files'
#     archived_record = record_to_archive.copy()
#     archived_record["archived_timestamp"] = datetime.now(timezone.utc)
#     archived_record["archived_by_username"] = requesting_username
#     if '_id' in archived_record:
#         del archived_record['_id']

#     # 3. Insert into 'archived_files'
#     archived_collection, error_get_archived_coll = get_archived_files_collection()
#     if error_get_archived_coll or archived_collection is None:
#         logging.error(f"{log_prefix} Failed to get archived_files collection: {error_get_archived_coll}")
#         return jsonify({"error": "Server error: Could not access archive."}), 500

#     try:
#         insert_result = archived_collection.insert_one(archived_record)
#         if not insert_result.inserted_id:
#             logging.error(f"{log_prefix} Failed to insert batch into archive. No inserted_id.")
#             return jsonify({"error": "Server error: Failed to archive batch."}), 500
#         logging.info(f"{log_prefix} Batch record inserted into archive with new ID: {insert_result.inserted_id}")
#     except Exception as e:
#         logging.error(f"{log_prefix} Exception inserting batch into archive: {e}", exc_info=True)
#         return jsonify({"error": "Server error: Exception during batch archiving."}), 500

#     # 4. Delete from 'user_files'
#     deleted_count, db_error_msg = delete_metadata_by_access_id(access_id)
#     if db_error_msg:
#         logging.critical(f"{log_prefix} CRITICAL: Batch archived (ID: {insert_result.inserted_id}) BUT failed to delete original from user_files (access_id: {access_id}). Error: {db_error_msg}")
#         return jsonify({"error": "Server error: Batch archived but failed to remove original. Please contact support."}), 500
#     if deleted_count == 0:
#         logging.warning(f"{log_prefix} Batch archived (ID: {insert_result.inserted_id}) but original (access_id: {access_id}) not found for deletion in user_files.")

#     display_name = record_to_archive.get('batch_display_name', access_id)
#     logging.info(f"{log_prefix} Batch '{display_name}' (access_id: {access_id}) successfully archived.")
#     return jsonify({"message": f"Batch '{display_name}' archived successfully."}), 200

# @file_bp.route('/api/file-details/<access_id>', methods=['GET'])
# def api_get_single_file_details(access_id: str):
#     file_info, error_msg = find_metadata_by_access_id(access_id)
#     if error_msg: return jsonify({"error": "Server error."}), 500
#     if not file_info: return jsonify({"error": f"File ID '{access_id}' not found."}), 404
#     if file_info.get('is_batch'): return jsonify({"error": "This ID is for a batch."}), 400
    
#     response_data = {
#         "access_id": access_id,
#         "filename_to_preview": file_info.get('filename_to_preview', 'Unknown'),
#         "original_size": file_info.get('original_size', 0),
#         "upload_timestamp": file_info.get('upload_timestamp'),
#         "username": file_info.get('username', 'N/A'),
#         "is_compressed": file_info.get('is_compressed', False), 
#         "is_split": file_info.get('is_split', False),         
#     }
#     return jsonify(response_data)

# @file_bp.route('/api/batch-details/<access_id>', methods=['GET'])
# def api_get_batch_details(access_id: str):
#     batch_info, error_msg = find_metadata_by_access_id(access_id)
#     if error_msg: return jsonify({"error": "Server error."}), 500
#     if not batch_info: return jsonify({"error": f"Batch ID '{access_id}' not found."}), 404
#     if not batch_info.get('is_batch'): return jsonify({"error": "This ID is not for a batch."}), 400

#     response_data = {
#         "batch_name": batch_info.get('batch_display_name', f"Batch {access_id}"),
#         "username": batch_info.get('username', 'N/A'),
#         "upload_date": batch_info.get('upload_timestamp'),
#         "total_size": batch_info.get('total_original_size', 0),
#         "files": batch_info.get('files_in_batch', []),
#         "access_id": access_id
#     }
#     processed_files = []
#     for f_item in response_data["files"]:
#         processed_f_item = f_item.copy()
#         processed_f_item.setdefault('filename_to_preview', "Unknown File")
#         processed_f_item.setdefault('original_size', 0)
#         processed_f_item.setdefault('skipped', False)
#         processed_f_item.setdefault('failed', False)
#         processed_f_item.setdefault('reason', None)
#         processed_files.append(processed_f_item)
#     response_data["files"] = processed_files
#     return jsonify(response_data)


# # @file_bp.route('/api/preview-details/<access_id>', methods=['GET'])
# # # @jwt_required(optional=True) # You can decide if this needs JWT.
# #                               # If browse links are public, this can be public.
# #                               # If only logged-in users can see previews, make it @jwt_required().
# #                               # For now, let's assume public access to the preview page itself.
# # def get_preview_details(access_id: str):
# #     """
# #     API endpoint to get details needed for rendering a file preview.
# #     """
# #     log_prefix = f"[PreviewDetails-{access_id}]"
# #     logging.info(f"{log_prefix} Request received.")

# #     file_info, error_msg_db = find_metadata_by_access_id(access_id)

# #     if error_msg_db:
# #         logging.error(f"{log_prefix} Database error: {error_msg_db}")
# #         return jsonify({"error": "Server error while fetching file information."}), 500
# #     if not file_info:
# #         logging.warning(f"{log_prefix} File not found.")
# #         return jsonify({"error": "File not found."}), 404

# #     # Ensure this endpoint is for single files, not batches, for preview.
# #     # Batch previews would be a separate, more complex feature.
# #     if file_info.get('is_batch'):
# #         logging.warning(f"{log_prefix} Attempted to get preview details for a batch.")
# #         return jsonify({"error": "Preview is not supported for batch uploads. Please select an individual file."}), 400

# #     filename_to_preview = file_info.get('filename_to_preview', 'unknown_file')

# #     # Get stored MIME type, or guess as a fallback
# #     mime_type = file_info.get('mime_type')
# #     if not mime_type:
# #         logging.warning(f"{log_prefix} MIME type not found in DB for '{filename_to_preview}'. Guessing.")
# #         mime_type, _ = mimetypes.guess_type(filename_to_preview)
# #         if not mime_type: # If guess also fails
# #             mime_type = 'application/octet-stream'
# #             logging.warning(f"{log_prefix} MIME type guess failed for '{filename_to_preview}'. Defaulting to {mime_type}.")

# #     preview_type_str = get_preview_type(mime_type, filename_to_preview)

# #     response_data = {
# #         "access_id": access_id,
# #         "filename": filename_to_preview,
# #         "size": file_info.get('original_size', 0),
# #         "mime_type": mime_type,
# #         "preview_type": preview_type_str, # Use the determined string
# #         "is_anonymous": file_info.get('is_anonymous', False),
# #         "upload_timestamp": file_info.get('upload_timestamp'), # ISO string format from DB
# #         "preview_content_url": None, # URL to fetch raw content for preview
# #         "preview_data": None,        # For potentially embedding small text content directly
# #     }

# #     # --- Expiration Check for Anonymous Uploads ---
# #     if response_data["is_anonymous"] and response_data["upload_timestamp"]:
# #         logging.info(f"{log_prefix} Anonymous upload. Checking expiration. Timestamp: '{response_data['upload_timestamp']}'")
# #         try:
# #             upload_datetime = dateutil_parser.isoparse(response_data["upload_timestamp"])
# #             # Ensure timezone awareness (MongoDB stores in UTC)
# #             if upload_datetime.tzinfo is None or upload_datetime.tzinfo.utcoffset(upload_datetime) is None:
# #                 upload_datetime = upload_datetime.replace(tzinfo=timezone.utc)

# #             # Define expiration limit (e.g., 5 days. This should ideally come from config)
# #             # For this example, let's use a fixed 5 days.
# #             # In your config.py, you could add: ANON_UPLOAD_EXPIRATION_DAYS = 5
# #             # Then import and use it: from config import ANON_UPLOAD_EXPIRATION_DAYS
# #             expiration_limit = timedelta(days=5) # Replace 5 with config value later

# #             if datetime.now(timezone.utc) > (upload_datetime + expiration_limit):
# #                 logging.info(f"{log_prefix} Anonymous download link EXPIRED. Uploaded: {upload_datetime}, Expires: {upload_datetime + expiration_limit}")
# #                 # Update preview_type to signal frontend specifically about expiration
# #                 response_data["preview_type"] = "expired"
# #                 # Return 410 Gone, so frontend can handle this status code specifically
# #                 return jsonify(response_data), 410
# #             else:
# #                 logging.info(f"{log_prefix} Anonymous download link still valid. Expires at: {upload_datetime + expiration_limit}")

# #         except ValueError as e_parse: # Catch parsing errors for the timestamp
# #             logging.error(f"{log_prefix} Error parsing upload_timestamp '{response_data['upload_timestamp']}': {e_parse}", exc_info=True)
# #             # Decide how to handle: maybe treat as non-expiring or error out
# #             # For now, let's error out to be safe, as expiration is a key feature for anon.
# #             return jsonify({"error": "Error processing file metadata (invalid timestamp format)."}), 500
# #         except Exception as e_exp: # Catch any other exception during check
# #             logging.error(f"{log_prefix} Unexpected error during expiration check: {e_exp}", exc_info=True)
# #             return jsonify({"error": "Server error during file validation."}), 500
# #     else:
# #         logging.info(f"{log_prefix} Not an anonymous upload or no timestamp, expiration check skipped.")

# #     # --- Determine how to provide preview content ---
# #     # For types that need their raw content fetched by the browser (images, videos, PDFs, or any text file)
# #     if response_data["preview_type"] in ['image', 'video', 'pdf', 'code', 'text', 'markdown', 'directory_listing', 'audio']:
# #         # We generate a URL to another backend endpoint that will serve the raw file.
# #         # We'll create this `serve_raw_file_content` endpoint in Step 4.
# #         # `_external=False` generates a relative URL, suitable for API responses consumed by frontend on same domain.
# #         response_data['preview_content_url'] = url_for('file.serve_raw_file_content', access_id=access_id, _external=False)
# #         logging.info(f"{log_prefix} Preview content URL set to: {response_data['preview_content_url']}")

# #     # OPTIONAL: For very small text-based files, you *could* fetch the content here
# #     # and embed it directly in `response_data['preview_data']`.
# #     # This avoids an extra HTTP request from the frontend.
# #     # However, it makes this endpoint more complex and potentially slower.
# #     # For now, we'll stick to `preview_content_url` for all content-based previews.
# #     # Example (if you were to implement direct data embedding):
# #     # if response_data["preview_type"] in ['code', 'text', 'markdown', 'directory_listing'] and response_data["size"] < 1024 * 50: # e.g., < 50KB
# #     #     from .download_routes import _get_actual_file_content # You'd need to make this helper accessible
# #     #     content_bytes, err_content = _get_actual_file_content(access_id, file_info) # Simplified call
# #     #     if not err_content and content_bytes:
# #     #         try:
# #     #             response_data['preview_data'] = content_bytes.decode('utf-8')
# #     #             response_data['preview_content_url'] = None # No need for URL if data is embedded
# #     #         except UnicodeDecodeError:
# #     #             logging.warning(f"{log_prefix} Could not decode content as UTF-8 for direct preview.")
# #     #             # Fallback to URL method if direct decoding fails
# #     #             if not response_data['preview_content_url']: # If not already set
# #     #                 response_data['preview_content_url'] = url_for('file.serve_raw_file_content', access_id=access_id, _external=False)
# #     #     else:
# #     #         logging.warning(f"{log_prefix} Failed to get content for direct preview: {err_content}")


# #     logging.info(f"{log_prefix} Successfully prepared preview details: {response_data}")
# #     return jsonify(response_data)

# # routes/file_routes.py
# # ... (imports and other parts of the file remain the same)

# @file_bp.route('/api/preview-details/<access_id>', methods=['GET'])
# def get_preview_details(access_id: str):
#     log_prefix = f"[PreviewDetails-{access_id}]"
#     filename_to_preview = request.args.get('filename') # Get filename from query
#     if filename_to_preview:
#         log_prefix += f"-File-{filename_to_preview[:20]}"
#     logging.info(f"{log_prefix} Request received.")

#     # top_level_record , error_msg_db = find_metadata_by_access_id(access_id)
#     top_level_record, error_msg_db = find_metadata_by_access_id(access_id)

#     if error_msg_db:
#         logging.error(f"{log_prefix} Database error: {error_msg_db}")
#         return jsonify({"error": "Server error while fetching file information."}), 500
#     if not top_level_record: # ... (handle record not found)
#         return jsonify({"error": "File or batch not found."}), 404

#     # --- MODIFIED LOGIC TO HANDLE BATCHES OF ONE ---
#     target_file_info = None
#     is_true_batch_with_multiple_files = False

#     if top_level_record.get('is_batch', False): # It's marked as a batch
#         files_in_batch = top_level_record  .get('files_in_batch', [])
#         if len(files_in_batch) == 1:
#             # It's a "batch" but only contains one file, so we can preview this single file.
#             target_file_info = files_in_batch[0]
#             logging.info(f"{log_prefix} Record is a 'batch of one'. Targeting the single file for preview.")
#         elif len(files_in_batch) > 1:
#             is_true_batch_with_multiple_files = True
#             logging.warning(f"{log_prefix} Record is a true batch with multiple files. Preview not supported for the whole batch.")
#             # For a true batch, the frontend would typically show a list of files in the batch,
#             # and clicking a file would call this endpoint again with a modified access_id
#             # or a new endpoint designed to get details for a *file within a batch*.
#             # For now, this endpoint will error for true batches.
#             return jsonify({"error": "This is a batch with multiple files. Preview is for single files. Please select a file from the batch."}), 400
#         else: # is_batch is true, but files_in_batch is empty
#             logging.warning(f"{log_prefix} Record marked as batch but has no files in files_in_batch array.")
#             return jsonify({"error": "Batch record is empty or corrupted."}), 400
#     else:
#         # It's a single file record (is_batch is false or missing, treat as false)
#         target_file_info = top_level_record
#         logging.info(f"{log_prefix} Record is a single file type. Targeting it for preview.")

#     if not target_file_info:
#         # This should ideally not be reached if the logic above is correct
#         logging.error(f"{log_prefix} Could not determine target file info from record.")
#         return jsonify({"error": "Could not determine file to preview from the record."}), 500
#     # --- END OF MODIFIED LOGIC ---

#    # original_filename = target_file_info.get('original_filename', 'unknown_file')

#     # Get stored MIME type from the target_file_info, or guess as a fallback
#     mime_type = target_file_info.get('mime_type')
#     if not mime_type:
#         logging.warning(f"{log_prefix} MIME type not found in DB for '{filename_to_preview}'. Guessing.")
#         mime_type, _ = mimetypes.guess_type(filename_to_preview)
#         if not mime_type:
#             mime_type = 'application/octet-stream'
#             logging.warning(f"{log_prefix} MIME type guess failed. Defaulting to {mime_type}.")

#     mime_type = mime_type or 'application/octet-stream'

#     preview_type_str = get_preview_type(mime_type, filename_to_preview)

#     response_data = {
#         "access_id": access_id, # The main access_id of the record fetched
#         "filename": filename_to_preview,
#         "size": target_file_info.get('original_size', 0),
#         "mime_type": mime_type,
#         "preview_type": preview_type_str,
#         # Anonymity and timestamp for expiration should come from the top-level top_level_record   ,
#         # as these apply to the whole upload record.
#         "is_anonymous": top_level_record   .get('is_anonymous', False),
#         "upload_timestamp": top_level_record   .get('upload_timestamp'),
#         "preview_content_url": None,
#         "preview_data": None,
#     }

#     # --- Expiration Check for Anonymous Uploads (uses top_level_record for timestamp) ---
#     if response_data["is_anonymous"] and response_data["upload_timestamp"]:
#         # (Expiration check logic remains the same as before, using response_data["upload_timestamp"])
#         # ...
#         logging.info(f"{log_prefix} Anonymous upload. Checking expiration. Timestamp: '{response_data['upload_timestamp']}'")
#         try:
#             upload_datetime = dateutil_parser.isoparse(response_data["upload_timestamp"])
#             if upload_datetime.tzinfo is None or upload_datetime.tzinfo.utcoffset(upload_datetime) is None:
#                 upload_datetime = upload_datetime.replace(tzinfo=timezone.utc)
#             expiration_limit = timedelta(days=5) # Replace with config value
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
#     else:
#         logging.info(f"{log_prefix} Not an anonymous upload or no timestamp, expiration check skipped.")


#     # --- Determine how to provide preview content ---
#     if response_data["preview_type"] in ['image', 'video', 'pdf', 'code', 'text', 'markdown', 'directory_listing', 'audio']:
#         # The access_id for serve_raw_file_content should be the main access_id.
#         # The serve_raw_file_content endpoint will then need to know if it's looking at a batch_of_one or a true single file.
#         # For now, let's keep it simple and assume serve_raw_file_content can also handle this.
#         response_data['preview_content_url'] = url_for('file.serve_raw_file_content', access_id=access_id, filename=filename_to_preview, _external=False)
#         logging.info(f"{log_prefix} Preview content URL set to: {response_data['preview_content_url']}")

#     logging.info(f"{log_prefix} Successfully prepared preview details for single file within record: {response_data}")
#     return jsonify(response_data)

# # @file_bp.route('/api/file-content/<access_id>')
# # # @jwt_required(optional=True) # Match the auth requirement of get_preview_details for now
# # def serve_raw_file_content(access_id: str):
#     # """
#     # Serves the raw file content for preview.
#     # (Implementation to be completed in Step 4)
#     # """
#     # log_prefix = f"[ServeRawContent-{access_id}]"
#     # logging.info(f"{log_prefix} Placeholder hit. Full implementation in next step.")
#     # # For now, just return a placeholder response or an error
#     # # This allows url_for to build the URL correctly.
#     # return jsonify({"message": f"Placeholder for serving content of {access_id}. Implement in Step 4."}), 200



# # --- Helper function MUST be defined BEFORE it's called by serve_raw_file_content ---
# def _get_final_file_content_for_preview(access_id: str, top_level_record: Dict[str, Any], specific_filename: Optional[str] = None) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
#     """
#     Fetches/reassembles/decompresses the actual file content for preview.
#     Returns: (content_bytes, error_message, filename_to_preview_for_mime)
#     The filename_to_preview_for_mime is crucial if the file was decompressed.
#     """
#     log_prefix_helper = f"[GetContentHelper-{access_id}]"
#     if specific_filename:
#         log_prefix_helper += f"-File-{specific_filename[:20]}"
#     log_prefix_helper += "]"
#     logging.info(f"{log_prefix_helper} Attempting to get final content.")
#     actual_file_to_process_meta: Optional[Dict[str, Any]] = None
#     # effective_filename_to_preview: Optional[str] = None
#     effective_original_filename: Optional[str] = None

#     # 1. Determine the actual file metadata to process (handles batch-of-one vs. single file)
#     if top_level_record.get('is_batch', False):
#         files_in_batch = top_level_record.get('files_in_batch', [])
#         target_filename_for_batch = specific_filename
#         if not target_filename_for_batch:
#             if len(files_in_batch) == 1:
#                 actual_file_to_process_meta = files_in_batch[0]
#                 effective_filename_to_preview = actual_file_to_process_meta.get('filename_to_preview')
#                 logging.info(f"{log_prefix_helper} Identified as batch-of-one. Target file: {effective_filename_to_preview}")
#             else:
#                 logging.error(f"{log_prefix_helper} Called for a multi-file batch. This function is for single file targets.")
#                 return None, "Cannot get content for a multi-file batch directly using this helper.", None
#         else: # Specific filename provided for a batch
#             actual_file_to_process_meta = next((f for f in files_in_batch if f.get('original_filename') == target_filename_for_batch), None)
#             if not actual_file_to_process_meta:
#                 logging.error(f"{log_prefix_helper} File '{target_filename_for_batch}' not found in batch record {access_id}.")
#                 return None, f"File '{target_filename_for_batch}' not found in batch.", None
#             effective_original_filename = target_filename_for_batch
#             logging.info(f"{log_prefix_helper} Targeting file '{effective_original_filename}' from batch.")
#     else:
#         actual_file_to_process_meta = top_level_record
#         effective_filename_to_preview = actual_file_to_process_meta.get('filename_to_preview')
#         logging.info(f"{log_prefix_helper} Identified as single file record. Target file: {effective_filename_to_preview}")

#     if not actual_file_to_process_meta or not effective_filename_to_preview:
#         logging.error(f"{log_prefix_helper} Could not determine target file metadata or effective filename.")
#         return None, "Could not determine target file metadata.", None

#     # 2. Get raw downloaded content (handles split or non-split)
#     is_split = actual_file_to_process_meta.get('is_split', False)
#     raw_downloaded_content: Optional[bytes] = None # Initialize

#     if is_split:
#         logging.info(f"{log_prefix_helper} File '{effective_filename_to_preview}' is split. Reassembling...")
#         chunks_meta = actual_file_to_process_meta.get('chunks')
#         if not chunks_meta:
#             logging.error(f"{log_prefix_helper} Split file metadata missing chunk information for '{effective_filename_to_preview}'.")
#             return None, "Split file metadata missing chunk information.", effective_filename_to_preview
        
#         chunks_meta.sort(key=lambda c: int(c.get('part_number', 0)))
#         reassembled_buffer = io.BytesIO()
#         for i, chunk_info in enumerate(chunks_meta):
#             part_num = chunk_info.get("part_number", i + 1)
#             # You need to ensure _find_best_telegram_file_id and download_telegram_file_content are imported/accessible
#             chunk_tg_file_id, _ = _find_best_telegram_file_id(chunk_info.get("send_locations", []), PRIMARY_TELEGRAM_CHAT_ID)
#             if not chunk_tg_file_id:
#                 logging.error(f"{log_prefix_helper} Missing Telegram ID for chunk {part_num} of '{effective_filename_to_preview}'.")
#                 return None, f"Missing Telegram ID for chunk {part_num} of '{effective_filename_to_preview}'.", effective_filename_to_preview
            
#             logging.debug(f"{log_prefix_helper} Downloading chunk {part_num} (TG ID: {chunk_tg_file_id}) for '{effective_filename_to_preview}'.")
#             content_part, err_part = download_telegram_file_content(chunk_tg_file_id) # Ensure this is imported
#             if err_part:
#                 logging.error(f"{log_prefix_helper} Error downloading chunk {part_num} for '{effective_filename_to_preview}': {err_part}")
#                 return None, f"Error downloading chunk {part_num} for '{effective_filename_to_preview}': {err_part}", effective_filename_to_preview
#             if not content_part:
#                 logging.error(f"{log_prefix_helper} Empty content for chunk {part_num} of '{effective_filename_to_preview}'.")
#                 return None, f"Empty content for chunk {part_num} of '{effective_filename_to_preview}'.", effective_filename_to_preview
#             reassembled_buffer.write(content_part)
        
#         raw_downloaded_content = reassembled_buffer.getvalue()
#         reassembled_buffer.close()
#         logging.info(f"{log_prefix_helper} Finished reassembling '{effective_filename_to_preview}'. Size: {len(raw_downloaded_content) if raw_downloaded_content else 0}")
#     else: # Not split
#         logging.info(f"{log_prefix_helper} File '{effective_filename_to_preview}' is not split. Downloading directly.")
#         send_locations = actual_file_to_process_meta.get('send_locations', [])
#         tg_file_id, _ = _find_best_telegram_file_id(send_locations, PRIMARY_TELEGRAM_CHAT_ID) # Ensure imported
#         if not tg_file_id:
#             logging.error(f"{log_prefix_helper} Missing Telegram ID for non-split file '{effective_filename_to_preview}'.")
#             return None, f"Missing Telegram ID for non-split file '{effective_filename_to_preview}'.", effective_filename_to_preview
        
#         logging.debug(f"{log_prefix_helper} Downloading non-split (TG ID: {tg_file_id}) for '{effective_filename_to_preview}'.")
#         raw_downloaded_content, err_download = download_telegram_file_content(tg_file_id) # Ensure imported
#         if err_download:
#             logging.error(f"{log_prefix_helper} Error downloading non-split file '{effective_filename_to_preview}': {err_download}")
#             return None, f"Error downloading non-split file '{effective_filename_to_preview}': {err_download}", effective_filename_to_preview
#         logging.info(f"{log_prefix_helper} Finished downloading non-split '{effective_filename_to_preview}'. Size: {len(raw_downloaded_content) if raw_downloaded_content else 0}")

#     # 3. Check if raw content was successfully obtained
#     if raw_downloaded_content is None:
#         logging.error(f"{log_prefix_helper} Failed to obtain raw downloaded content for '{effective_filename_to_preview}'.")
#         return None, f"Failed to retrieve initial content for '{effective_filename_to_preview}'.", effective_filename_to_preview

#     # 4. Handle potential uploader-side compression
#     final_content_to_serve: bytes = raw_downloaded_content # Initialize with potentially zipped content
#     filename_for_mime_type_final: str = effective_filename_to_preview # Default to original name

#     is_compressed_by_uploader = actual_file_to_process_meta.get('is_compressed', False)
#     if is_compressed_by_uploader and not effective_filename_to_preview.lower().endswith('.zip'):
#         logging.info(f"{log_prefix_helper} File '{effective_filename_to_preview}' was compressed by uploader. Attempting to decompress.")
#         try:
#             with zipfile.ZipFile(io.BytesIO(raw_downloaded_content), 'r') as zf:
#                 # _find_filename_in_zip needs to be robust or we need to be sure about the name.
#                 # Assuming effective_filename_to_preview is the name of the single file inside the zip.
#                 entry_name_to_extract = _find_filename_in_zip(zf, effective_filename_to_preview, log_prefix_helper) # Ensure imported
#                 final_content_to_serve = zf.read(entry_name_to_extract) # This is the actual content
#                 # filename_for_mime_type_final remains effective_filename_to_preview as it's the true name of the content
#                 logging.info(f"{log_prefix_helper} Decompressed '{entry_name_to_extract}' from zip. New size: {len(final_content_to_serve)}")
#         except Exception as e_zip:
#             logging.error(f"{log_prefix_helper} Failed to decompress '{effective_filename_to_preview}': {e_zip}", exc_info=True)
#             # If decompression fails, return an error, not the zipped content for a non-zip preview
#             return None, f"Failed to decompress file '{effective_filename_to_preview}' for preview.", effective_filename_to_preview
#     else:
#         logging.info(f"{log_prefix_helper} File '{effective_filename_to_preview}' not marked for uploader-side decompression, or is already a zip.")
#         # final_content_to_serve is already raw_downloaded_content
#         # filename_for_mime_type_final is already effective_filename_to_preview

#     # 5. Final check and return
#     if final_content_to_serve is None: # Should not happen if logic above is correct
#          logging.error(f"{log_prefix_helper} final_content_to_serve is None before returning for '{filename_for_mime_type_final}'. This is unexpected.")
#          return None, f"Unexpected error preparing content for '{filename_for_mime_type_final}'.", filename_for_mime_type_final

#     return final_content_to_serve, None, filename_for_mime_type_final
# # --- End of helper function ---

# @file_bp.route('/api/file-content/<access_id>')
# # @jwt_required(optional=True) # Keep auth consistent with get_preview_details
# def serve_raw_file_content(access_id: str):
#     """
#     Serves the raw file content for preview.
#     """
#     log_prefix = f"[ServeRawContent-{access_id}]"
#     filename_from_query = request.args.get('filename') # Get filename from query
#     if filename_from_query:
#         log_prefix += f"-File-{filename_from_query[:20]}"
#     logging.info(f"{log_prefix} Request received.")

#     # 1. Fetch the top-level record metadata
#     top_level_record, error_msg_db = find_metadata_by_access_id(access_id)

#     if error_msg_db:
#         logging.error(f"{log_prefix} Database error: {error_msg_db}")
#         return make_response("Server error while fetching file information.", 500)
#     if not top_level_record:
#         logging.warning(f"{log_prefix} Record not found.")
#         return make_response("File or batch not found.", 404)

#     # 2. Expiration Check for Anonymous Uploads (using top-level record's timestamp)
#     if top_level_record.get("is_anonymous") and top_level_record.get("upload_timestamp"):
#         logging.info(f"{log_prefix} Anonymous upload. Checking expiration. Timestamp: '{top_level_record['upload_timestamp']}'")
#         try:
#             upload_datetime = dateutil_parser.isoparse(top_level_record["upload_timestamp"])
#             if upload_datetime.tzinfo is None or upload_datetime.tzinfo.utcoffset(upload_datetime) is None:
#                 upload_datetime = upload_datetime.replace(tzinfo=timezone.utc)
            
#             expiration_limit = timedelta(days=5) # From config ideally
#             if datetime.now(timezone.utc) > (upload_datetime + expiration_limit):
#                 logging.info(f"{log_prefix} Anonymous content link EXPIRED.")
#                 return make_response("File link has expired.", 410) # 410 Gone
#         except Exception as e_exp:
#             logging.error(f"{log_prefix} Error during expiration check for content: {e_exp}", exc_info=True)
#             # If expiration check fails, better to deny access
#             return make_response("Error validating file expiration.", 500)
    
#     # 3. Get the actual file content using the helper
#     # The helper will internally determine if it's a batch-of-one or a true single file record.
#     content_bytes, error_msg_content, filename_for_mime_determination = _get_final_file_content_for_preview(access_id, top_level_record, filename_from_query)

#     if error_msg_content:
#         logging.error(f"{log_prefix} Error getting final file content: {error_msg_content}")
#         return make_response(error_msg_content, 500)
#     if not content_bytes: # Should be caught by error_msg_content, but as a safeguard
#         logging.error(f"{log_prefix} Final content bytes are None or empty unexpectedly.")
#         return make_response("Failed to retrieve file content.", 500)
#     if not filename_for_mime: # Should also not happen if content_bytes is valid
#         logging.error(f"{log_prefix} Filename for MIME type determination is missing.")
#         filename_for_mime = "unknown.dat" # Fallback

#     # 4. Determine MIME type for the response
#     # Prefer stored MIME if available from the specific file_info (in case of batch-of-one)
#     # or top_level_record (for true single file).
#     # The _get_final_file_content_for_preview already used the correct original filename if it decompressed.
#     stored_mime_type: Optional[str] = None
    
#     if top_level_record.get('is_batch', False) and len(top_level_record.get('files_in_batch', [])) == 1:
#         final_mime_type = top_level_record['files_in_batch'][0].get('mime_type')
#     else: # True single file record
#         final_mime_type = top_level_record.get('mime_type')

#     final_mime_type = stored_mime_type
#     if not final_mime_type: # Fallback to guessing based on the (potentially decompressed) filename
#         final_mime_type, _ = mimetypes.guess_type(filename_for_mime)
#         if not final_mime_type:
#             final_mime_type = 'application/octet-stream' # Ultimate fallback
    
#     logging.info(f"{log_prefix} Serving content for '{filename_for_mime}' with MIME type: {final_mime_type}. Size: {len(content_bytes)}")

#     # 5. Serve the content
#     return Response(content_bytes, mimetype=final_mime_type)

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
    archive_file_record_by_access_id
)
from config import app , PRIMARY_TELEGRAM_CHAT_ID
from typing import Dict, Any, Tuple, Optional, List, Generator

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
    current_user_jwt_identity = get_jwt_identity()
    user_doc, error = find_user_by_id(ObjectId(current_user_jwt_identity))
    if error or not user_doc or user_doc.get('username') != username:
        return jsonify({"error": "Unauthorized to view these files"}), 403

    user_files_data, db_error = find_metadata_by_username(username)
    if db_error:
        return jsonify({"error": "Could not retrieve files"}), 500

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
    if file_info.get('is_batch'): return jsonify({"error": "This ID is for a batch."}), 400

    response_data = {
        "access_id": access_id,
        "filename_to_preview": file_info.get('filename_to_preview', file_info.get('original_filename', 'Unknown')), # Added original_filename fallback
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
        "access_id": access_id,
        "is_anonymous": batch_info.get('is_anonymous', False), # Pass this
        "upload_timestamp_raw": batch_info.get('upload_timestamp'), # Pass this raw for expiration logic
    }
    processed_files = []
    for f_item in response_data["files"]:
        processed_f_item = f_item.copy()
        processed_f_item.setdefault('filename_to_preview', f_item.get('original_filename', "Unknown File")) # Use original_filename as fallback
        processed_f_item.setdefault('original_size', 0)
        processed_f_item.setdefault('skipped', False)
        processed_f_item.setdefault('failed', False)
        processed_f_item.setdefault('reason', None)
        processed_files.append(processed_f_item)
    response_data["files"] = processed_files
    return jsonify(response_data)

@file_bp.route('/api/preview-details/<access_id>', methods=['GET'])
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

    if top_level_record.get('is_batch', False):
        files_in_batch = top_level_record.get('files_in_batch', [])
        if filename_to_preview_query: # Specific file from batch requested
            target_file_info = next((f for f in files_in_batch if f.get('original_filename') == filename_to_preview_query), None)
            if target_file_info:
                effective_filename_for_preview = filename_to_preview_query
            else:
                logging.warning(f"{log_prefix} File '{filename_to_preview_query}' not found in batch {access_id}.")
                return jsonify({"error": f"File '{filename_to_preview_query}' not found in batch."}), 404
        elif len(files_in_batch) == 1: # Batch of one
            target_file_info = files_in_batch[0]
            effective_filename_for_preview = target_file_info.get('original_filename') or target_file_info.get('filename_to_preview') # Prefer original_filename
        else: # True multi-file batch but no specific file requested
            logging.warning(f"{log_prefix} Attempted to get preview details for a multi-file batch without specifying a file.")
            return jsonify({"error": "This is a batch. Please select an individual file from the batch for preview."}), 400
    else: # Single file record
        target_file_info = top_level_record
        effective_filename_for_preview = target_file_info.get('filename_to_preview') or target_file_info.get('original_filename')

    if not target_file_info or not effective_filename_for_preview:
        logging.error(f"{log_prefix} Could not determine target file info or filename for preview. Target: {target_file_info}, EffectiveFN: {effective_filename_for_preview}")
        return jsonify({"error": "Could not determine file to preview from the record."}), 500

    mime_type = target_file_info.get('mime_type')
    if not mime_type:
        mime_type, _ = mimetypes.guess_type(effective_filename_for_preview)
        if not mime_type: mime_type = 'application/octet-stream'
        logging.info(f"{log_prefix} Guessed MIME type for '{effective_filename_for_preview}': {mime_type}")


    preview_type_str = get_preview_type(mime_type, effective_filename_for_preview)

    response_data = {
        "access_id": access_id,
        "filename": effective_filename_for_preview,
        "size": target_file_info.get('original_size', 0),
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
            expiration_limit = timedelta(days=app.config.get('ANONYMOUS_LINK_EXPIRATION_DAYS', 5))
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
    else:
        logging.info(f"{log_prefix} Not an anonymous upload or no timestamp, expiration check skipped.")

    if response_data["preview_type"] not in ['unsupported', 'expired']:
        # Pass the effective_filename_for_preview as the 'filename' query param for serve_raw_file_content
        response_data['preview_content_url'] = url_for('file.serve_raw_file_content', access_id=access_id, filename=effective_filename_for_preview, _external=False)
        logging.info(f"{log_prefix} Preview content URL set to: {response_data['preview_content_url']}")

    logging.info(f"{log_prefix} Successfully prepared preview details: {response_data}")
    return jsonify(response_data)


def _get_final_file_content_for_preview(access_id: str, top_level_record: Dict[str, Any], specific_filename_from_query: Optional[str] = None) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    """
    Fetches/reassembles/decompresses the actual file content for preview.
    Returns: (content_bytes, error_message, filename_used_for_mime_type_determination)
    """
    log_prefix_helper = f"[GetContentHelper-{access_id}]"
    if specific_filename_from_query:
        log_prefix_helper += f"-File-{specific_filename_from_query[:20]}"

    logging.info(f"{log_prefix_helper} Attempting to get final content.")
    actual_file_to_process_meta: Optional[Dict[str, Any]] = None
    effective_filename_for_processing: Optional[str] = None

    if top_level_record.get('is_batch', False):
        files_in_batch = top_level_record.get('files_in_batch', [])
        if not specific_filename_from_query: # No specific file requested from batch
            if len(files_in_batch) == 1:
                actual_file_to_process_meta = files_in_batch[0]
                effective_filename_for_processing = actual_file_to_process_meta.get('original_filename')
                logging.info(f"{log_prefix_helper} Identified as batch-of-one. Target file: {effective_filename_for_processing}")
            else:
                return None, "Cannot get content for a multi-file batch directly. A specific file must be indicated.", None
        else: # Specific filename provided for a batch
            actual_file_to_process_meta = next((f for f in files_in_batch if f.get('original_filename') == specific_filename_from_query), None)
            if not actual_file_to_process_meta:
                return None, f"File '{specific_filename_from_query}' not found in batch.", None
            effective_filename_for_processing = specific_filename_from_query
            logging.info(f"{log_prefix_helper} Targeting file '{effective_filename_for_processing}' from batch.")
    else: # Not a batch, it's a single file record
        actual_file_to_process_meta = top_level_record
        effective_filename_for_processing = actual_file_to_process_meta.get('original_filename') or actual_file_to_process_meta.get('filename_to_preview')
        logging.info(f"{log_prefix_helper} Identified as single file record. Target file: {effective_filename_for_processing}")

    if not actual_file_to_process_meta or not effective_filename_for_processing:
        logging.error(f"{log_prefix_helper} Could not determine target file metadata or effective filename for processing.")
        return None, "Could not determine target file metadata or filename.", None

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
            
            logging.debug(f"{log_prefix_helper} Downloading chunk {part_num} (TG ID: {chunk_tg_file_id}) for '{effective_filename_for_processing}'.")
            content_part, err_part = download_telegram_file_content(chunk_tg_file_id)
            if err_part:
                return None, f"Error downloading chunk {part_num}: {err_part}", effective_filename_for_processing
            if not content_part:
                return None, f"Empty content for chunk {part_num}.", effective_filename_for_processing
            reassembled_buffer.write(content_part)
        
        raw_downloaded_content = reassembled_buffer.getvalue()
        reassembled_buffer.close()
        logging.info(f"{log_prefix_helper} Finished reassembling '{effective_filename_for_processing}'. Size: {len(raw_downloaded_content) if raw_downloaded_content else 0}")
    else:
        logging.info(f"{log_prefix_helper} File '{effective_filename_for_processing}' is not split. Downloading directly.")
        send_locations = actual_file_to_process_meta.get('send_locations', [])
        tg_file_id, _ = _find_best_telegram_file_id(send_locations, PRIMARY_TELEGRAM_CHAT_ID)
        if not tg_file_id:
            return None, "Missing Telegram ID for non-split file.", effective_filename_for_processing
        
        logging.debug(f"{log_prefix_helper} Downloading non-split (TG ID: {tg_file_id}) for '{effective_filename_for_processing}'.")
        raw_downloaded_content, err_download = download_telegram_file_content(tg_file_id)
        if err_download:
            return None, f"Error downloading non-split file: {err_download}", effective_filename_for_processing
        logging.info(f"{log_prefix_helper} Finished downloading non-split '{effective_filename_for_processing}'. Size: {len(raw_downloaded_content) if raw_downloaded_content else 0}")

    if raw_downloaded_content is None:
        return None, "Failed to retrieve initial content.", effective_filename_for_processing

    final_content_to_serve: bytes = raw_downloaded_content
    filename_for_mime_type_final: str = effective_filename_for_processing

    is_compressed_by_uploader = actual_file_to_process_meta.get('is_compressed', False)
    if is_compressed_by_uploader and not effective_filename_for_processing.lower().endswith('.zip'):
        logging.info(f"{log_prefix_helper} File '{effective_filename_for_processing}' was compressed by uploader. Attempting to decompress.")
        try:
            with zipfile.ZipFile(io.BytesIO(raw_downloaded_content), 'r') as zf:
                entry_name_to_extract = _find_filename_in_zip(zf, effective_filename_for_processing, log_prefix_helper)
                final_content_to_serve = zf.read(entry_name_to_extract)
                logging.info(f"{log_prefix_helper} Decompressed '{entry_name_to_extract}' from zip. New size: {len(final_content_to_serve)}")
        except Exception as e_zip:
            logging.error(f"{log_prefix_helper} Failed to decompress '{effective_filename_for_processing}': {e_zip}", exc_info=True)
            return None, "Failed to decompress file for preview.", effective_filename_for_processing
    else:
        logging.info(f"{log_prefix_helper} File '{effective_filename_for_processing}' not marked for uploader-side decompression, or is already a zip.")

    if final_content_to_serve is None:
         return None, "Unexpected error preparing content.", filename_for_mime_type_final

    return final_content_to_serve, None, filename_for_mime_type_final

@file_bp.route('/api/file-content/<access_id>')
def serve_raw_file_content(access_id: str):
    log_prefix = f"[ServeRawContent-{access_id}]"
    filename_from_query = request.args.get('filename')
    if filename_from_query:
        log_prefix += f"-File-{filename_from_query[:20]}"
    logging.info(f"{log_prefix} Request received.")

    top_level_record, error_msg_db = find_metadata_by_access_id(access_id)

    if error_msg_db:
        logging.error(f"{log_prefix} Database error: {error_msg_db}")
        return make_response("Server error while fetching file information.", 500)
    if not top_level_record:
        logging.warning(f"{log_prefix} Record not found.")
        return make_response("File or batch not found.", 404)

    if top_level_record.get("is_anonymous") and top_level_record.get("upload_timestamp"):
        logging.info(f"{log_prefix} Anonymous upload. Checking expiration. Timestamp: '{top_level_record['upload_timestamp']}'")
        try:
            upload_datetime = dateutil_parser.isoparse(top_level_record["upload_timestamp"])
            if upload_datetime.tzinfo is None or upload_datetime.tzinfo.utcoffset(upload_datetime) is None:
                upload_datetime = upload_datetime.replace(tzinfo=timezone.utc)
            
            expiration_limit = timedelta(days=app.config.get('ANONYMOUS_LINK_EXPIRATION_DAYS', 5))
            if datetime.now(timezone.utc) > (upload_datetime + expiration_limit):
                logging.info(f"{log_prefix} Anonymous content link EXPIRED.")
                return make_response("File link has expired.", 410)
        except Exception as e_exp:
            logging.error(f"{log_prefix} Error during expiration check for content: {e_exp}", exc_info=True)
            return make_response("Error validating file expiration.", 500)
    
    content_bytes, error_msg_content, filename_for_mime = _get_final_file_content_for_preview(access_id, top_level_record, filename_from_query)

    if error_msg_content:
        logging.error(f"{log_prefix} Error getting final file content: {error_msg_content}")
        return make_response(error_msg_content, 500)
    if not content_bytes:
        logging.error(f"{log_prefix} Final content bytes are None or empty unexpectedly.")
        return make_response("Failed to retrieve file content.", 500)
    if not filename_for_mime:
        logging.error(f"{log_prefix} Filename for MIME type determination is missing.")
        filename_for_mime = "unknown.dat" 

    final_mime_type, _ = mimetypes.guess_type(filename_for_mime)
    if not final_mime_type:
        final_mime_type = 'application/octet-stream'
    
    logging.info(f"{log_prefix} Serving content for '{filename_for_mime}' with MIME type: {final_mime_type}. Size: {len(content_bytes)}")
    return Response(content_bytes, mimetype=final_mime_type)