# database/common_operations.py
import logging
from typing import Tuple
from datetime import datetime, timezone # Needed for setting archived_timestamp
from pymongo.errors import PyMongoError

# Import collection getter functions from connection.py
from .connection import get_metadata_collection, get_archived_files_collection

def archive_file_record_by_access_id(access_id: str, admin_username: str) -> Tuple[bool, str]:
    """
    Archives a file record by copying it from 'user_files' to 'archived_files'
    and then deleting it from 'user_files'.
    """
    user_files_coll, error1 = get_metadata_collection() # This is 'user_files'
    archived_coll, error2 = get_archived_files_collection()

    if error1 or user_files_coll is None:
        # Log the specific error from get_metadata_collection
        logging.error(f"Failed to get user_files collection for archiving: {error1}")
        return False, f"Error accessing user_files collection: {error1}"
    if error2 or archived_coll is None:
        # Log the specific error from get_archived_files_collection
        logging.error(f"Failed to get archived_files collection for archiving: {error2}")
        return False, f"Error accessing archived_files collection: {error2}"

    try:
        # 1. Find the record in the main 'user_files' collection
        record_to_archive = user_files_coll.find_one({"access_id": access_id})
        if not record_to_archive:
            logging.warning(f"Record ID '{access_id}' not found in active files to archive.")
            return False, f"Record ID '{access_id}' not found in active files."

        # 2. Prepare the record for the 'archived_files' collection
        # Create a copy to avoid modifying the original dict if it's cached or reused
        archived_record_doc = record_to_archive.copy()
        archived_record_doc["archived_timestamp"] = datetime.now(timezone.utc)
        archived_record_doc["archived_by_username"] = admin_username
        # MongoDB will generate a new _id on insert into archived_files.
        # If the original _id from user_files is present in archived_record_doc,
        # it's usually fine as insert_one won't try to use it if it's named '_id'.
        # If you want to be explicit, you can remove it:
        if '_id' in archived_record_doc:
            del archived_record_doc['_id']

        # 3. Insert into 'archived_files' collection
        insert_result = archived_coll.insert_one(archived_record_doc)
        if not insert_result.inserted_id:
            # This is an unexpected scenario if insert_one completes without error but has no inserted_id
            logging.error(f"Failed to insert record {access_id} into archive (no inserted_id returned from MongoDB).")
            return False, "Failed to insert record into archive (no inserted_id)."
        logging.info(f"Record {access_id} copied to archive with new _id {insert_result.inserted_id} by {admin_username}.")

        # 4. Delete from the original 'user_files' collection
        delete_result = user_files_coll.delete_one({"access_id": access_id})
        if delete_result.deleted_count == 0:
            # This is a critical state: copied but not deleted from original.
            logging.critical(f"CRITICAL: Record {access_id} archived (new archive _id: {insert_result.inserted_id}) "
                             f"BUT failed to delete from user_files. Manual cleanup needed.")
            # It's debatable whether to return True or False here.
            # The record IS archived, but the operation is not clean.
            # For now, returning False to indicate the overall operation had a critical issue.
            return False, "Record archived but failed to remove original. Please contact support immediately."

        logging.info(f"Record {access_id} successfully deleted from user_files after archiving.")
        return True, "Record archived successfully."

    except PyMongoError as e:
        logging.error(f"PyMongoError during archiving record {access_id}: {e}", exc_info=True)
        return False, f"Database error during archive: {e}"
    except Exception as e:
        logging.error(f"Unexpected error archiving record {access_id}: {e}", exc_info=True)
        return False, f"Unexpected server error during archive: {e}"

logging.info("Common database operations module (database/common_operations.py) initialized.")