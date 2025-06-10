import logging
from flask_admin import AdminIndexView, expose
from flask_login import current_user
from flask import redirect, url_for, request, flash
from database import get_all_users, get_all_file_metadata
from .auth_routes import logout_user

class MyAdminIndexView(AdminIndexView):
    def is_accessible(self):
        # --- TEMPORARILY MODIFIED FOR DEVELOPMENT ---
        logging.warning("MyAdminIndexView is_accessible is temporarily returning True. REMOVE FOR PRODUCTION.")
        return current_user.is_authenticated and getattr(current_user, 'is_admin', False)
        

    def inaccessible_callback(self, name, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access the admin dashboard.', 'info')
            return redirect(url_for('admin_auth.login', next=request.url))
        else: # Authenticated but not an admin
            flash('You do not have admin privileges to access the dashboard.', 'danger')
            # Redirect to logout then login, or a safe non-admin page if you have one.
            # Forcing logout then login can be a clear way to handle this.
            logout_user() # Make sure logout_user is imported if used directly here, or redirect to logout route
            return redirect(url_for('admin_auth.login')) 

    @expose('/')
    def index(self):
        # Fetch some summary data (examples)
        user_count = 0
        file_record_count = 0
        error_msg = None

        premium_user_count = 0
        free_user_count = 0
        # We don't typically count "anonymous users" as distinct entities in the user table,
        # so we'll focus on files by anonymous uploads.

        premium_user_file_count = 0
        free_user_file_count = 0
        anonymous_file_count = 0

        premium_usernames = set()
        free_usernames = set()

        try:
            all_users_data, users_err = get_all_users()
            if users_err:
                logging.error(f"Error fetching users for dashboard: {users_err}")
                error_msg = users_err
            elif all_users_data is not None:
                user_count = len(all_users_data)
                for user in all_users_data:
                    role = user.get('role', 'Free User') # Default to Free User if role is missing
                    username = user.get('username')
                    if role == "Premium User":
                        premium_user_count += 1
                        if username: premium_usernames.add(username)
                    elif role == "Free User": # Catches "Free User" and any other non-Admin, non-Premium roles
                        free_user_count += 1
                        if username: free_usernames.add(username)
                    # Admins are counted in total_users but not separately for "Free" or "Premium" categories here.
                    # If you need a separate admin_user_count, you can add it.

            all_files_data, files_err = get_all_file_metadata()
            if files_err:
                logging.error(f"Error fetching file metadata for dashboard: {files_err}")
                error_msg = f"{error_msg if error_msg else ''}; {files_err}".strip('; ')
            elif all_files_data is not None:
                file_record_count = len(all_files_data)
                for record in all_files_data:
                    record_username = record.get('username')
                    is_anonymous_upload = record.get('is_anonymous', False)

                    if is_anonymous_upload:
                        anonymous_file_count += 1
                    elif record_username in premium_usernames:
                        premium_user_file_count += 1
                    elif record_username in free_usernames:
                        # This also catches files by users who might have a role other than 'Premium User' or 'Free User'
                        # but are not anonymous, and their username was collected in `free_usernames` list.
                        # If Admin files should be separate, adjust user categorization.
                        free_user_file_count += 1
                    # else:
                        # Optionally, count files that don't fall into these categories (e.g., by Admins, or orphaned)
                        # logging.debug(f"File {record.get('access_id')} by user {record_username} not categorized for premium/free/anonymous.")
                        pass


        except Exception as e:
            logging.error(f"Error fetching dashboard data: {e}", exc_info=True)
            error_msg = str(e)

        return self.render('admin/dashboard_index.html',
                           user_count=user_count,
                           file_record_count=file_record_count,
                           premium_user_count=premium_user_count,
                           free_user_count=free_user_count,
                           premium_user_file_count=premium_user_file_count,
                           free_user_file_count=free_user_file_count,
                           anonymous_file_count=anonymous_file_count,
                           error_message=error_msg)