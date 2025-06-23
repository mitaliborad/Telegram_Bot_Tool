# database/common_user_operations.py
import logging
from typing import Tuple, Optional, Dict, Any
from datetime import datetime, timezone
from bson import ObjectId 
from pymongo.errors import PyMongoError

from .connection import get_userinfo_collection, get_archived_users_collection
from .user_models import find_user_by_email, find_user_by_username 

def archive_user_account(user_id_to_archive_str: str, admin_username: str) -> Tuple[bool, str]:
    """
    Archives a user account by copying it from 'userinfo' to 'archived_users'
    and then deleting it from 'userinfo'.

    Args:
        user_id_to_archive_str: The string representation of the user's _id to archive.
        admin_username: The username of the admin performing the action.

    Returns:
        A tuple (success: bool, message: str).
    """
    userinfo_coll, error_userinfo = get_userinfo_collection()
    archived_users_coll, error_archived = get_archived_users_collection()

    if error_userinfo or userinfo_coll is None:
        logging.error(f"Failed to get userinfo collection for archiving user '{user_id_to_archive_str}': {error_userinfo}")
        return False, f"Database error (userinfo): {error_userinfo}"
    if error_archived or archived_users_coll is None:
        logging.error(f"Failed to get archived_users collection for archiving user '{user_id_to_archive_str}': {error_archived}")
        return False, f"Database error (archived_users): {error_archived}"

    try:
        user_oid = ObjectId(user_id_to_archive_str)
    except Exception as e:
        logging.error(f"Invalid ObjectId format for user_id_to_archive: '{user_id_to_archive_str}': {e}")
        return False, "Invalid user ID format."

    try:
        # 1. Find the user record in the 'userinfo' collection
        user_to_archive_doc = userinfo_coll.find_one({"_id": user_oid})
        if not user_to_archive_doc:
            logging.warning(f"User ID '{user_oid}' not found in active users to archive.")
            return False, f"User ID '{user_id_to_archive_str}' not found in active users."

        # 2. Prepare the record for the 'archived_users' collection
        archived_user_data = user_to_archive_doc.copy() # Make a copy
        archived_user_data["original_user_id"] = str(user_to_archive_doc["_id"]) # Store original _id as string
        archived_user_data["original_username"] = user_to_archive_doc.get("username")
        archived_user_data["original_email"] = user_to_archive_doc.get("email")
        archived_user_data["archived_at"] = datetime.now(timezone.utc)
        archived_user_data["archived_by"] = admin_username

        if '_id' in archived_user_data:
            del archived_user_data['_id']

        # 3. Insert into 'archived_users' collection
        insert_result = archived_users_coll.insert_one(archived_user_data)
        if not insert_result.inserted_id:
            logging.error(f"Failed to insert user {user_oid} into archive (no inserted_id from MongoDB).")
            return False, "Failed to insert user into archive (no inserted_id)."
        
        archive_record_id = insert_result.inserted_id # This is the _id of the new document in archived_users
        logging.info(f"User {user_oid} (Username: {archived_user_data.get('original_username')}) "
                     f"copied to archived_users with new archive record _id {archive_record_id} by {admin_username}.")

        # 4. Delete from the original 'userinfo' collection
        delete_result = userinfo_coll.delete_one({"_id": user_oid})
        if delete_result.deleted_count == 0:
            logging.critical(f"CRITICAL: User {user_oid} archived (archive_record_id: {archive_record_id}) "
                             f"BUT failed to delete from userinfo. Manual cleanup needed.")
            return False, "User archived but failed to remove original. Please contact support."

        logging.info(f"User {user_oid} successfully deleted from userinfo after archiving.")
        return True, "User account archived successfully."

    except PyMongoError as e:
        logging.error(f"PyMongoError during archiving user {user_id_to_archive_str}: {e}", exc_info=True)
        return False, f"Database error during user archive: {e}"
    except Exception as e:
        logging.error(f"Unexpected error archiving user {user_id_to_archive_str}: {e}", exc_info=True)
        return False, f"Unexpected server error during user archive: {e}"

def restore_user_account(original_user_id_str: str, admin_username: str) -> Tuple[bool, str]:
    """
    Restores a user account from 'archived_users' back to 'userinfo'.
    The 'original_user_id_str' is the original _id of the user when they were in 'userinfo'.

    Args:
        original_user_id_str: The string representation of the user's original _id.
        admin_username: The username of the admin performing the action.

    Returns:
        A tuple (success: bool, message: str).
    """
    userinfo_coll, error_userinfo = get_userinfo_collection()
    archived_users_coll, error_archived = get_archived_users_collection()

    if error_userinfo or userinfo_coll is None:
        logging.error(f"Failed to get userinfo collection for restoring user (orig_id: {original_user_id_str}): {error_userinfo}")
        return False, f"Database error (userinfo): {error_userinfo}"
    if error_archived or archived_users_coll is None:
        logging.error(f"Failed to get archived_users collection for restoring user (orig_id: {original_user_id_str}): {error_archived}")
        return False, f"Database error (archived_users): {error_archived}"

    try:
        # 1. Find the user record in the 'archived_users' collection using their original_user_id
        archived_user_doc = archived_users_coll.find_one({"original_user_id": original_user_id_str})
        if not archived_user_doc:
            logging.warning(f"Archived user with original_user_id '{original_user_id_str}' not found to restore.")
            return False, f"Archived user with original ID '{original_user_id_str}' not found."

        archive_record_actual_id = archived_user_doc["_id"]

        # 2. Prepare the record for 'userinfo' collection
        user_to_restore_data = archived_user_doc.copy()
        try:
            user_to_restore_data["_id"] = ObjectId(original_user_id_str)
        except Exception as e:
            logging.error(f"Could not convert original_user_id '{original_user_id_str}' back to ObjectId for restore: {e}")
            return False, "Corrupted original user ID in archive record."
            
        restored_username = user_to_restore_data.get("original_username")
        restored_email = user_to_restore_data.get("original_email")
        fields_to_remove_from_restored_doc = [
            "original_user_id", "original_username", "original_email",
            "archived_at", "archived_by"
        ]
        for field in fields_to_remove_from_restored_doc:
            if field in user_to_restore_data:
                del user_to_restore_data[field]
        if restored_username: user_to_restore_data['username'] = restored_username
        if restored_email: user_to_restore_data['email'] = restored_email


        # 3. CONFLICT CHECK: Before inserting into userinfo, check for username/email conflicts
        if restored_username:
            conflicting_user_by_name, _ = find_user_by_username(restored_username)
            if conflicting_user_by_name and conflicting_user_by_name["_id"] != user_to_restore_data["_id"]:
                msg = f"Cannot restore: Username '{restored_username}' is already in use by another active user."
                logging.warning(f"{msg} (Restoring user original_id: {original_user_id_str})")
                return False, msg
        
        if restored_email:
            conflicting_user_by_email, _ = find_user_by_email(restored_email)
            if conflicting_user_by_email and conflicting_user_by_email["_id"] != user_to_restore_data["_id"]:
                msg = f"Cannot restore: Email '{restored_email}' is already in use by another active user."
                logging.warning(f"{msg} (Restoring user original_id: {original_user_id_str})")
                return False, msg

        # 4. Insert into 'userinfo' collection (using the original _id)
        insert_result_userinfo = userinfo_coll.insert_one(user_to_restore_data)
        if not insert_result_userinfo.inserted_id or insert_result_userinfo.inserted_id != user_to_restore_data["_id"]:
            logging.error(f"Failed to insert user (orig_id: {original_user_id_str}) into userinfo "
                          f"(no/mismatched inserted_id from MongoDB). Expected: {user_to_restore_data['_id']}")
            return False, "Failed to insert user into active users (unexpected DB response)."
        
        logging.info(f"User (orig_id: {original_user_id_str}, Username: {restored_username}) "
                     f"restored to userinfo by {admin_username}.")

        # 5. Delete from 'archived_users' collection using the archive record's _id
        delete_result_archive = archived_users_coll.delete_one({"_id": archive_record_actual_id})
        if delete_result_archive.deleted_count == 0:
            logging.critical(f"CRITICAL: User (orig_id: {original_user_id_str}) restored to userinfo "
                             f"BUT failed to delete from archived_users (archive_record_id: {archive_record_actual_id}). "
                             f"Manual cleanup needed from archived_users.")
            return False, "User restored but failed to remove from archive. Please contact support."

        logging.info(f"Archived record (archive_record_id: {archive_record_actual_id}) "
                     f"for user (orig_id: {original_user_id_str}) successfully deleted from archived_users after restoration.")
        return True, "User account restored successfully."

    except PyMongoError as e:
        if e.code == 11000: 
             logging.error(f"PyMongoError (Duplicate Key) during restore user (orig_id: {original_user_id_str}): {e}", exc_info=True)
             return False, "Failed to restore user: A user with this ID, username, or email might already exist."
        logging.error(f"PyMongoError during restoring user (orig_id: {original_user_id_str}): {e}", exc_info=True)
        return False, f"Database error during user restore: {e}"
    except Exception as e:
        logging.error(f"Unexpected error restoring user (orig_id: {original_user_id_str}): {e}", exc_info=True)
        return False, f"Unexpected server error during user restore: {e}"

logging.info("Common user database operations module (database/common_user_operations.py) initialized.")