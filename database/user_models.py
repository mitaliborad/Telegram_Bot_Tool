# database/user_models.py
import logging
import re
from typing import Optional, Dict, Any, List, Tuple
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from bson import ObjectId
from pymongo.errors import PyMongoError, OperationFailure

# Import the function to get the userinfo collection from connection.py
from .connection import get_userinfo_collection

# User class definition (directly from your original database.py)
class User(UserMixin):
    """Represents a user for Flask-Login."""
    def __init__(self, user_data: Dict[str, Any]):
        if not user_data:
            raise ValueError("Cannot initialize User with empty data.")
        self.id = str(user_data.get('_id'))
        self.username = user_data.get('username')
        self.email = user_data.get('email')
        self.password_hash = user_data.get('password_hash')
        self.role = user_data.get('role', 'Free User')
        if not self.id or not self.username or not self.email or not self.password_hash:
            logging.error(f"User data missing essential fields during User object creation: {user_data}")
            raise ValueError("User data from database is missing required fields (_id, username, email, password_hash).")

    def get_id(self): # Already provided by UserMixin if self.id is set, but explicit is fine.
        return self.id

    def check_password(self, password_to_check: str) -> bool:
        if not self.password_hash:
            logging.error(f"User {self.username} has no password hash stored.")
            return False
        return check_password_hash(self.password_hash, password_to_check)

    @property
    def is_admin(self) -> bool:
        return self.role == "Admin"

# --- User Data Access Functions ---

def get_all_users(search_query: Optional[str] = None) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    collection, error = get_userinfo_collection()
    if error or collection is None:
        logging.error(f"Failed to get userinfo collection for get_all_users: {error}")
        return None, f"Database error: {error}"
    query_filter = {}
    if search_query and search_query.strip():
        search_term = search_query.strip()
        escaped_query = re.escape(search_term)
        regex_pattern = re.compile(escaped_query, re.IGNORECASE)
        query_filter["$or"] = [
            {"username": {"$regex": regex_pattern}},
            {"email": {"$regex": regex_pattern}}
        ]
        logging.info(f"Searching users with query: '{search_term}' using filter: {query_filter}")
    else:
        logging.info("Fetching all users (no search query / empty search query).")
    try:
        users_cursor = collection.find(query_filter).sort("username", 1)
        users_list = list(users_cursor)
        for user in users_list:
            if '_id' in user and isinstance(user['_id'], ObjectId):
                user['_id'] = str(user['_id'])
            if 'password_hash' in user: # Remove password hash for safety when listing users
                del user['password_hash']
            user['role'] = user.get('role', 'Free User')
        return users_list, ""
    except PyMongoError as e: error_msg = f"PyMongoError fetching users: {e}"; logging.error(error_msg, exc_info=True); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error fetching users: {e}"; logging.error(error_msg, exc_info=True); return None, error_msg

def find_user_by_id(user_id: ObjectId) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    collection, error = get_userinfo_collection()
    if error or collection is None:
        logging.error(f"DB error in find_user_by_id: {error}")
        return None, str(error or "Collection not available")
    if not isinstance(user_id, ObjectId):
        logging.error(f"Invalid type passed to find_user_by_id: {type(user_id)}")
        return None, "Invalid user ID format provided."
    try:
        user_doc = collection.find_one({"_id": user_id})
        # Do NOT delete password_hash here. This function is used for authentication.
        return (user_doc, None) if user_doc else (None, None)
    except PyMongoError as e: logging.error(f"Database error finding user by ID {user_id}: {e}", exc_info=True); return None, f"Database error finding user: {e}"
    except Exception as e: logging.error(f"Error finding user by ID {user_id}: {e}", exc_info=True); return None, f"Error finding user: {e}"

def find_user_by_id_str(user_id_str: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        user_oid = ObjectId(user_id_str)
        # Calls the find_user_by_id function which now correctly keeps password_hash
        return find_user_by_id(user_oid)
    except Exception as e:
        logging.error(f"Invalid ObjectId format in find_user_by_id_str for '{user_id_str}': {e}")
        return None, f"Invalid user ID format: {user_id_str}"

def find_user_by_email(email: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    collection, error = get_userinfo_collection()
    if error or collection is None: # Added check for collection
        logging.error(f"DB error in find_user_by_email: {error}")
        return None, str(error or "Collection not available for find_user_by_email")
    try:
        user_doc = collection.find_one({"email": email.lower()})
        # Do NOT delete password_hash here. This function is used for authentication.
        return (user_doc, None) if user_doc else (None, None)
    except PyMongoError as e: logging.error(f"Database error finding user by email {email}: {e}", exc_info=True); return None, f"Database error finding user: {e}"
    except Exception as e: logging.error(f"Error finding user by email {email}: {e}", exc_info=True); return None, f"Error finding user: {e}"

def find_user_by_username(username: str) -> Tuple[Optional[Dict[str, Any]], str]:
    collection, error = get_userinfo_collection()
    if error or collection is None:
        logging.error(f"Failed to get userinfo collection for username check: {error}")
        return None, f"Failed to get userinfo collection: {error}"
    try:
        user = collection.find_one({"username": username})
        # Do NOT delete password_hash here if this might be used in an auth flow.
        # If purely for checking existence or non-sensitive display, can delete. Assume auth potential.
        return (user, "") if user else (None, "")
    except PyMongoError as e: error_msg = f"PyMongoError finding user by username '{username}': {e}"; logging.exception(error_msg); return None, error_msg
    except Exception as e: error_msg = f"Unexpected error finding user by username '{username}': {e}"; logging.exception(error_msg); return None, error_msg

def save_user(user_data: Dict[str, Any]) -> Tuple[bool, str]:
    collection, error = get_userinfo_collection()
    if error or collection is None:
        return False, f"Failed to get userinfo collection: {error}"
    if "email" not in user_data or "password_hash" not in user_data:
        return False, "User data is missing required email or password_hash fields."
    try:
        user_data["email"] = user_data["email"].lower() # Ensure email is stored lowercase
        user_data.setdefault('role', 'Free User') # Ensure 'role' has a default if not provided
        # user_data.setdefault('created_at', datetime.now(timezone.utc)) # Usually handled in auth_routes

        result = collection.insert_one(user_data)
        if result.inserted_id:
            logging.info(f"Successfully inserted new user with ID: {result.inserted_id}")
            return True, f"User created successfully (ID: {result.inserted_id})."
        else:
            logging.warning("User insert operation completed but reported no inserted ID.")
            return False, "User insert operation finished unexpectedly."
    except OperationFailure as of:
        error_msg = f"Database operation failed saving user: {of}"
        logging.exception(error_msg)
        if "E11000" in str(of): # Check for duplicate key error
            if 'email_1' in str(of): return False, "Email address already exists."
            if 'username_1' in str(of): return False, "Username already exists."
            return False, "A user with this email or username already exists."
        return False, error_msg
    except Exception as e: error_msg = f"Unexpected error saving user: {e}"; logging.exception(error_msg); return False, error_msg

def update_user_password(user_id: ObjectId, new_password: str) -> Tuple[bool, str]:
    collection, error = get_userinfo_collection()
    if error or collection is None:
        logging.error(f"Failed to get userinfo collection for password update: {error}")
        return False, "Database error (collection unavailable)."
    if not isinstance(user_id, ObjectId):
        logging.error(f"Invalid type passed to update_user_password: {type(user_id)}")
        return False, "Invalid user ID format provided."
    try:
        hashed_pw = generate_password_hash(new_password, method='pbkdf2:sha256')
        result = collection.update_one({"_id": user_id}, {"$set": {"password_hash": hashed_pw}})
        if result.matched_count == 0: return False, "User not found."
        # modified_count can be 0 if password is the same, still consider it a success.
        logging.info(f"Successfully updated password for user ID {user_id}.")
        return True, "Password updated successfully."
    except PyMongoError as e: logging.error(f"Database error updating password for user ID {user_id}: {e}", exc_info=True); return False, "Database error during password update."
    except Exception as e: logging.error(f"Unexpected error updating password for user ID {user_id}: {e}", exc_info=True); return False, "Server error during password update."

# Note on delete_user_by_id:
# This function performs a PERMANENT deletion of a user from the 'userinfo' (active users) collection.
# With the introduction of the user archiving system, the admin UI's "Delete" button
# for users will typically call `database.archive_user_account` instead of this function directly.
# This function remains for low-level permanent deletion if explicitly needed.
def delete_user_by_id(user_id_str: str) -> Tuple[int, str]:
    collection, error = get_userinfo_collection()
    if error or collection is None:
        logging.error(f"Failed to get userinfo collection for delete_user_by_id: {error}")
        return 0, f"Database error: {error}"
    try:
        user_oid = ObjectId(user_id_str)
    except Exception as e:
        logging.error(f"Invalid ObjectId format for user_id_str '{user_id_str}': {e}")
        return 0, f"Invalid user ID format: {user_id_str}"
    try:
        result = collection.delete_one({"_id": user_oid})
        deleted_count = result.deleted_count
        if deleted_count == 1: logging.info(f"Successfully deleted user with ID: {user_oid} from userinfo collection.")
        elif deleted_count == 0: logging.warning(f"No user found to delete with ID: {user_oid} in userinfo collection. Already deleted?")
        return deleted_count, ""
    except PyMongoError as e: error_msg = f"PyMongoError deleting user {user_oid} from userinfo: {e}"; logging.error(error_msg, exc_info=True); return 0, error_msg
    except Exception as e: error_msg = f"Unexpected error deleting user {user_oid} from userinfo: {e}"; logging.error(error_msg, exc_info=True); return 0, error_msg

def update_user_admin_status(user_id_str: str, is_admin_new_status: bool) -> Tuple[bool, str]:
    """
    Updates the 'role' of a user to 'Admin' or 'Free User'.
    """
    collection, error = get_userinfo_collection()
    if error or collection is None:
        logging.error(f"Failed to get userinfo collection for updating admin status: {error}")
        return False, f"Database error: {error}"
    try:
        user_oid = ObjectId(user_id_str)
    except Exception as e:
        logging.error(f"Invalid ObjectId format for user_id_str '{user_id_str}' in admin update: {e}")
        return False, f"Invalid user ID format: {user_id_str}"

    new_role = "Admin" if is_admin_new_status else "Free User"
    try:
        result = collection.update_one({"_id": user_oid}, {"$set": {"role": new_role}})
        if result.matched_count == 0:
            return False, "User not found."
        action = "promoted to Admin" if is_admin_new_status else "role set to Free User"
        logging.info(f"User ID {user_oid} role updated to {new_role}. Modified: {result.modified_count}")
        return True, f"User {action}."
    except PyMongoError as e: error_msg = f"PyMongoError updating role for user {user_oid}: {e}"; logging.error(error_msg, exc_info=True); return False, error_msg
    except Exception as e: error_msg = f"Unexpected error updating role for user {user_oid}: {e}"; logging.error(error_msg, exc_info=True); return False, error_msg

def update_user_details(user_id_str: str, update_data: Dict[str, Any]) -> Tuple[bool, str]:
    collection, error = get_userinfo_collection()
    if error or collection is None: return False, f"Database error: {error}"
    if not update_data: return False, "No update data provided."
    try:
        user_oid = ObjectId(user_id_str)
    except Exception as e: return False, f"Invalid user ID format: {user_id_str}"

    allowed_to_update = {'username', 'email', 'role'}
    update_payload = {k: v for k, v in update_data.items() if k in allowed_to_update}

    if not update_payload: return False, "No valid fields provided for update."

    if 'email' in update_payload:
        new_email_lower = update_payload['email'].lower()
        existing_user_with_email, _ = find_user_by_email_excluding_id(new_email_lower, user_oid)
        if existing_user_with_email:
            return False, f"Email '{new_email_lower}' is already taken by another user."
        update_payload['email'] = new_email_lower

    if 'username' in update_payload:
        new_username = update_payload['username']
        existing_user_with_username, _ = find_user_by_username_excluding_id(new_username, user_oid)
        if existing_user_with_username:
            return False, f"Username '{new_username}' is already taken by another user."

    try:
        result = collection.update_one({"_id": user_oid}, {"$set": update_payload})
        if result.matched_count == 0: return False, "User not found."
        logging.info(f"User {user_oid} details updated. Payload: {update_payload}. Modified: {result.modified_count}")
        return True, "User details updated successfully."
    except PyMongoError as e:
        if hasattr(e, 'code') and e.code == 11000: # Duplicate key error
            if 'email_1' in str(e).lower(): return False, "Email address is already in use."
            if 'username_1' in str(e).lower(): return False, "Username is already in use."
            return False, f"Database constraint violation: A unique field value is already taken."
        return False, f"Database error updating user details: {str(e)}"
    except Exception as e: return False, f"Unexpected error updating user details: {str(e)}"

def find_user_by_email_excluding_id(email: str, exclude_user_id: ObjectId) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    collection, error = get_userinfo_collection()
    if error or collection is None: return None, str(error or "Collection not available")
    try:
        user_doc = collection.find_one({"email": email.lower(), "_id": {"$ne": exclude_user_id}})
        return user_doc, None
    except Exception as e: logging.error(f"Error in find_user_by_email_excluding_id: {e}"); return None, str(e)

def find_user_by_username_excluding_id(username: str, exclude_user_id: ObjectId) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    collection, error = get_userinfo_collection()
    if error or collection is None: return None, str(error or "Collection not available")
    try:
        user_doc = collection.find_one({"username": username, "_id": {"$ne": exclude_user_id}})
        return user_doc, None
    except Exception as e: logging.error(f"Error in find_user_by_username_excluding_id: {e}"); return None, str(e)

logging.info("User models module (database/user_models.py) initialized.")