# database/archive_models.py
import logging
from typing import Optional, Dict, Any, List, Tuple
from bson import ObjectId
from pymongo.errors import PyMongoError, OperationFailure
from datetime import datetime, timezone
from .connection import get_archived_files_collection

# --- Archived File Metadata (archived_files collection) Functions ---

def find_archived_metadata_by_username(username: str) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Finds all archived metadata records for a given username.
    """
    collection, error = get_archived_files_collection()
    if error or collection is None:
        logging.error(f"Failed to get archived_files collection for user '{username}': {error}")
        return None, f"Database error: {error}"
    try:
        records_cursor = collection.find({"username": username}).sort("archived_timestamp", -1)
        records_list = list(records_cursor)
        for record in records_list: # Convert ObjectId to str
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
        logging.info(f"Found {len(records_list)} archived metadata records for username: {username}")
        return records_list, ""
    except OperationFailure as of: error_msg = f"DB op failed finding archived metadata for '{username}': {of}"; logging.exception(error_msg); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error finding archived metadata for '{username}': {e}"; logging.exception(error_msg); return None, error_msg

def find_archived_metadata_by_access_id(access_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Finds a single archived metadata record by its unique access_id.
    """
    collection, error = get_archived_files_collection()
    if error or collection is None:
        logging.error(f"Failed to get archived_files collection for access_id '{access_id}': {error}")
        return None, f"Database error: {error}"
    try:
        record = collection.find_one({"access_id": access_id})
        if record:
            if '_id' in record and isinstance(record['_id'], ObjectId): # Convert ObjectId to str
                record['_id'] = str(record['_id'])
            logging.info(f"Found archived metadata record for access_id: {access_id}")
            return record, ""
        else:
            logging.info(f"No archived metadata record found for access_id: {access_id}")
            return None, "Archived file record not found."
    except OperationFailure as of: error_msg = f"DB op failed finding archived metadata by access_id '{access_id}': {of}"; logging.exception(error_msg); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error finding archived metadata by access_id '{access_id}': {e}"; logging.exception(error_msg); return None, error_msg

def delete_archived_metadata_by_access_id(access_id: str) -> Tuple[int, str]:
    """
    Deletes a single archived metadata record matching the unique access_id from the 'archived_files' collection.
    """
    collection, error = get_archived_files_collection()
    if error or collection is None:
        logging.error(f"Failed to get archived_files coll for deleting access_id '{access_id}': {error}")
        return 0, f"Database error: {error}"
    try:
        result = collection.delete_one({"access_id": access_id})
        deleted_count = result.deleted_count
        if deleted_count == 1:
            logging.info(f"Deleted archived metadata record for access_id '{access_id}'.")
        elif deleted_count == 0:
            logging.warning(f"No archived metadata record found to delete for access_id '{access_id}'.")
        return deleted_count, ""
    except PyMongoError as e: error_msg = f"PyMongoError deleting archived metadata by access_id '{access_id}': {e}"; logging.exception(error_msg); return 0, error_msg
    except Exception as e: error_msg = f"Unexpected error deleting archived metadata by access_id '{access_id}': {e}"; logging.exception(error_msg); return 0, error_msg

def get_all_archived_files(search_query: Optional[str] = None) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """
    Retrieves all documents from the 'archived_files' collection,
    optionally filtered by a search query.
    """
    collection, error = get_archived_files_collection()
    if error or collection is None:
        logging.error(f"Failed to get archived_files collection for get_all_archived_files: {error}")
        return None, f"Database error: {error}"
    query_filter = {}
    if search_query and search_query.strip():
        search_term = search_query.strip()
        query_filter["$or"] = [ # Assuming 're' is not needed for basic $regex
            {"access_id": {"$regex": search_term, "$options": "i"}},
            {"original_filename": {"$regex": search_term, "$options": "i"}},
            {"batch_display_name": {"$regex": search_term, "$options": "i"}},
            {"username": {"$regex": search_term, "$options": "i"}},
            {"archived_by_username": {"$regex": search_term, "$options": "i"}}
        ]
        logging.info(f"Searching archived_files with query: '{search_term}' using filter: {query_filter}")
    else:
        logging.info("Fetching all archived_files (no search query / empty search query).")
    try:
        records_cursor = collection.find(query_filter).sort("archived_timestamp", -1)
        records_list = list(records_cursor)
        for record in records_list: # Convert ObjectId to str
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
        logging.info(f"Retrieved {len(records_list)} archived file record(s).")
        return records_list, ""
    except PyMongoError as e: error_msg = f"PyMongoError fetching all archived files: {e}"; logging.error(error_msg, exc_info=True); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error fetching all archived files: {e}"; logging.error(error_msg, exc_info=True); return None, error_msg

logging.info("Archive models module (database/archive_models.py) initialized.")