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
