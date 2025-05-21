# routes/admin/dashboard_view.py
import logging
from flask_admin import AdminIndexView, expose
from flask_login import current_user # Will be used when security is back
from flask import redirect, url_for, request
from database import get_all_users, get_all_file_metadata

class MyAdminIndexView(AdminIndexView):
    def is_accessible(self):
        # --- TEMPORARILY MODIFIED FOR DEVELOPMENT ---
        logging.warning("MyAdminIndexView is_accessible is temporarily returning True. REMOVE FOR PRODUCTION.")
        return True
        # --- END OF TEMPORARY MODIFICATION ---

        # Real check (when security is back on):
        # if not current_user.is_authenticated or not getattr(current_user, 'is_admin', False):
        #     return False
        # return True

    def inaccessible_callback(self, name, **kwargs):
        logging.info(f"inaccessible_callback for MyAdminIndexView. Redirecting to login.")
        return redirect(url_for('auth.login', next=request.url)) 

    @expose('/')
    def index(self):
        # Fetch some summary data (examples)
        user_count = 0
        file_record_count = 0
        error_msg = None

        try:
            users, err = get_all_users() # Fetches all, then counts
            if not err and users is not None: # Check users is not None
                user_count = len(users)
            elif err:
                logging.error(f"Error fetching users for dashboard: {err}")
                error_msg = err

            files, err_files = get_all_file_metadata() # Fetches all, then counts
            if not err_files and files is not None: # Check files is not None
                file_record_count = len(files)
            elif err_files:
                logging.error(f"Error fetching file metadata for dashboard: {err_files}")
                error_msg = error_msg + "; " + err_files if error_msg else err_files

        except Exception as e:
            logging.error(f"Error fetching dashboard data: {e}", exc_info=True)
            error_msg = str(e)

        # self.render() is available from AdminIndexView
        # We need to create 'admin/dashboard_index.html'
        return self.render('admin/dashboard_index.html',
                           user_count=user_count,
                           file_record_count=file_record_count,
                           error_message=error_msg)