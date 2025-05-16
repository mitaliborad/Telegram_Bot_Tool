# routes/admin/views.py
import logging
from flask_admin.babel import gettext
from flask_admin.base import BaseView, expose
from flask_login import current_user # We'll use this for authentication later
from flask import redirect, url_for, request, flash
import json

# Import your database functions to fetch user data
import database # Assuming database.py is in the parent directory or accessible via PYTHONPATH

class UserView(BaseView):
    def is_accessible(self):

        # if not current_user.is_authenticated:
        #     return False 
        # if not current_user.is_admin: # Check the is_admin property
        #     logging.warning(f"User '{current_user.email}' attempted to access admin view without admin role.")
        #     return False 
        return True

    def inaccessible_callback(self, name, **kwargs):
        # if not current_user.is_authenticated:
        #     flash(gettext('Please log in to access this page.'), 'warning')
        #     return redirect(url_for('auth.login', next=request.url))
        # else:

        #     flash(gettext('You do not have permission to access this page.'), 'danger')
            return redirect(url_for('file.list_user_files_page', username=current_user.username))

    @expose('/') 
    def index(self):

        users_list = []
        error_message = None
        try:

            all_users_data, db_error = database.get_all_users() 
            if db_error:
                error_message = f"Error fetching users: {db_error}"
                logging.error(f"[AdminUserView] {error_message}")
            elif all_users_data:
                users_list = all_users_data
            else:
                error_message = "No users found." # Or handle as an empty list
                logging.info("[AdminUserView] No users found in the database.")

        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminUserView] {error_message}", exc_info=True)
        return self.render('admin/user_list.html', users=users_list, error_message=error_message)


class FileMetadataView(BaseView):
    def is_accessible(self):
        # if not current_user.is_authenticated:
        #     return False
        # if not getattr(current_user, 'is_admin', False):
        #     logging.warning(f"User '{current_user.email}' attempted to access FileMetadataView without admin role.")
        #     return False
        return True

    def inaccessible_callback(self, name, **kwargs):
        # if not current_user.is_authenticated:
        #     flash(gettext('Please log in to access this page.'), 'warning')
        #     return redirect(url_for('auth.login', next=request.url))
        # else:
        #     flash(gettext('You do not have permission to access this page.'), 'danger')
            return redirect(url_for('file.list_user_files_page', username=current_user.username))

    @expose('/')
    def index(self):
        records_list = []
        error_message = None
        try:
            # We need a function in database.py to get ALL file/batch metadata
            all_records_data, db_error = database.get_all_file_metadata() # We will define this function
            if db_error:
                error_message = f"Error fetching file metadata: {db_error}"
                logging.error(f"[AdminFileMetadataView] {error_message}")
            elif all_records_data:
                records_list = all_records_data
            else:
                logging.info("[AdminFileMetadataView] No file metadata records found.")

        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminFileMetadataView] {error_message}", exc_info=True)

        return self.render('admin/file_metadata_list.html', records=records_list, error_message=error_message)
    
    @expose('/details/<access_id>')
    def details_view(self, access_id):
        record = None
        error_message = None
        try:
            record_data, db_error = database.find_metadata_by_access_id(access_id)
            if db_error:
                error_message = f"Error fetching record details for {access_id}: {db_error}"
                logging.error(f"[AdminFileMetadataViewDetails] {error_message}")
            elif record_data:
                record = record_data
            else:
                error_message = f"Record with Access ID '{access_id}' not found."
                logging.warning(f"[AdminFileMetadataViewDetails] {error_message}")
                # Optional: abort(404) if record not found, Flask-Admin will show its 404 page
                # return abort(404) # Or handle with a message on the details template

        except Exception as e:
            error_message = f"An unexpected error occurred while fetching details for {access_id}: {str(e)}"
            logging.error(f"[AdminFileMetadataViewDetails] {error_message}", exc_info=True)

        if not record and not error_message: # Should be caught above, but as a safeguard
             error_message = f"Record with Access ID '{access_id}' not found."

        # We'll create 'admin/file_metadata_details.html' next
        return self.render('admin/file_metadata_details.html',
                           record=record,
                           error_message=error_message,
                           json=json)
        
    @expose('/delete/<access_id>', methods=('POST',)) # Only allow POST for delete
    def delete_view(self, access_id):
        # In a real app with CSRF protection, you'd validate the CSRF token here.
        # Flask-Admin forms usually handle this if you use its ModelView with forms.
        # Since this is a custom link/button, we're keeping it simple for now.

        try:
            # Verify the record exists before attempting to delete (optional, but good practice)
            record_to_delete, find_err = database.find_metadata_by_access_id(access_id)
            if find_err:
                flash(gettext('Error finding record to delete: %(error)s', error=find_err), 'danger')
                return redirect(url_for('.index'))
            if not record_to_delete:
                flash(gettext('Record with Access ID %(access_id)s not found. Already deleted?', access_id=access_id), 'warning')
                return redirect(url_for('.index'))

            # Perform the deletion
            deleted_count, db_error_msg = database.delete_metadata_by_access_id(access_id)

            if db_error_msg:
                flash(gettext('Error deleting record: %(error)s', error=db_error_msg), 'danger')
                logging.error(f"[AdminFileMetadataDelete] DB error deleting {access_id}: {db_error_msg}")
            elif deleted_count > 0:
                display_name = record_to_delete.get('batch_display_name', record_to_delete.get('original_filename', access_id))
                flash(gettext('Record "%(name)s" (%(id)s) deleted successfully.', name=display_name, id=access_id), 'success')
                logging.info(f"[AdminFileMetadataDelete] Record {access_id} deleted by admin.")
            else:
                # Should have been caught by the find_metadata_by_access_id check above
                flash(gettext('Failed to delete record %(access_id)s. It might have been already deleted.', access_id=access_id), 'warning')
                logging.warning(f"[AdminFileMetadataDelete] Record {access_id} not found for deletion or no change.")

        except Exception as e:
            flash(gettext('An unexpected error occurred during deletion: %(error)s', error=str(e)), 'danger')
            logging.error(f"[AdminFileMetadataDelete] Unexpected error deleting {access_id}: {str(e)}", exc_info=True)

        return redirect(url_for('.index'))
    
    @expose('/delete/<user_id_str>', methods=('POST',)) # Only POST for delete
    def delete_user_view(self, user_id_str):
        # Security: In a real app, double-check admin privileges here again,
        # even if is_accessible passed, as an extra layer.
        # For now, matching the open access of other views.

        try:
            # Optional: Fetch user to get username for flash message before deleting
            user_to_delete_doc, find_err = database.find_user_by_id_str(user_id_str) # We might need this new helper
            if find_err :
                flash(gettext('Error finding user for deletion details: %(error)s', error=find_err), 'danger')
                return redirect(url_for('.index'))
            if not user_to_delete_doc:
                flash(gettext('User with ID %(id)s not found. Already deleted?', id=user_id_str), 'warning')
                return redirect(url_for('.index'))

            username_for_flash = user_to_delete_doc.get('username', user_id_str)

            # Perform the deletion using the new database function
            deleted_count, db_error_msg = database.delete_user_by_id(user_id_str)

            if db_error_msg:
                flash(gettext('Error deleting user: %(error)s', error=db_error_msg), 'danger')
                logging.error(f"[AdminUserDelete] DB error deleting user {user_id_str}: {db_error_msg}")
            elif deleted_count > 0:
                flash(gettext('User "%(name)s" (ID: %(id)s) deleted successfully.', name=username_for_flash, id=user_id_str), 'success')
                logging.info(f"[AdminUserDelete] User {user_id_str} deleted by admin.")
            else:
                flash(gettext('Failed to delete user %(id)s. It might have been already deleted.', id=user_id_str), 'warning')
                logging.warning(f"[AdminUserDelete] User {user_id_str} not found for deletion or no change.")

        except Exception as e:
            flash(gettext('An unexpected error occurred during user deletion: %(error)s', error=str(e)), 'danger')
            logging.error(f"[AdminUserDelete] Unexpected error deleting user {user_id_str}: {str(e)}", exc_info=True)

        return redirect(url_for('.index'))
