# database/__init__.py

# --- Re-export core connection and collection access functions ---
from .connection import (
    get_db,
    close_db_connection,
    get_userinfo_collection,
    get_metadata_collection,
    get_archived_files_collection,
    get_archived_users_collection
)

# --- Re-export User model and key user-related functions ---
from .user_models import (
    User,
     get_all_users,
     find_user_by_id,
     find_user_by_id_str,
     find_user_by_email,
     find_user_by_username,
     save_user,
     update_user_password,
     delete_user_by_id, # Note: UI "Delete" will now mostly use archive_user_account
     update_user_admin_status,
     update_user_details,
     find_user_by_email_excluding_id,
     find_user_by_username_excluding_id
)

# --- Re-export Active File model functions ---
from .file_models import (
    save_file_metadata,
    find_metadata_by_username,
    find_metadata_by_access_id,
    delete_metadata_by_filename,
    find_metadata_by_email,
    delete_metadata_by_access_id,
    get_all_file_metadata
)

# --- Re-export Archived File model functions ---
from .archive_models import (
    find_archived_metadata_by_username,
    find_archived_metadata_by_access_id,
    delete_archived_metadata_by_access_id,
    get_all_archived_files
)

# --- Re-export Archived User model functions ---
from .archived_user_models import ( # Corrected filename
    get_all_archived_users,
    find_archived_user_by_original_id,
    delete_archived_user_permanently
)

# --- Re-export Common Cross-Collection Operations (Files) ---
from .common_operations import (
    archive_file_record_by_access_id
)

# --- Re-export Common User Operations ---
from .common_user_operations import (
    archive_user_account,
    restore_user_account
)

import logging
logging.info("Database package (database/__init__.py) fully initialized and re-exporting components.")