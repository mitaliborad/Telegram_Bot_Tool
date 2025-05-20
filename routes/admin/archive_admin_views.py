# routes/admin/archive_admin_views.py
import logging
from flask_admin.babel import gettext
from flask_admin.base import BaseView, expose
from flask_login import current_user # For security later
from flask import redirect, url_for, request, flash
from math import ceil
import database # Your database module
# json might be needed if you add a details view for archived files later
# import json

class ArchivedFileView(BaseView):
    def is_accessible(self):
        # --- TEMPORARILY MODIFIED FOR DEVELOPMENT ---
        logging.warning("ArchivedFileView is_accessible is temporarily returning True. REMOVE FOR PRODUCTION.")
        return True
        # --- END OF TEMPORARY MODIFICATION ---

        # Real check (when security is back on):
        # if not current_user.is_authenticated or not getattr(current_user, 'is_admin', False):
        #     return False
        # return True

    def inaccessible_callback(self, name, **kwargs):
        logging.info(f"inaccessible_callback for ArchivedFileView. Current is_accessible: True")
        if hasattr(current_user, 'is_authenticated') and not current_user.is_authenticated: # Check if current_user has this attr
            flash(gettext('Please log in to access this page.'), 'warning')
            return redirect(url_for('auth.login', next=request.url))
        else:
            flash(gettext('You do not have permission to access this page.'), 'danger')
            if hasattr(current_user, 'username') and current_user.username:
                 return redirect(url_for('file.list_user_files_page', username=current_user.username))
            else:
                 return redirect(url_for('auth.login')) # Fallback

    @expose('/')
    def index(self):
        search_query = request.args.get('q', '').strip()
        page = request.args.get('page', 1, type=int)
        per_page = 20 # Number of archived items per page
        total_records = 0
        total_pages = 1
        archived_records_page = []
        error_message = None

        try:
            all_archived_data, db_error = database.get_all_archived_files(search_query=search_query)

            if db_error:
                error_message = f"Error fetching archived files: {db_error}"
                logging.error(f"[AdminArchivedFileView] {error_message}")
            elif all_archived_data:
                total_records = len(all_archived_data)
                total_pages = ceil(total_records / per_page) if per_page > 0 else 1
                if total_pages == 0 and total_records > 0: total_pages = 1

                start_index = (page - 1) * per_page
                end_index = start_index + per_page
                archived_records_page = all_archived_data[start_index:end_index]

                if not archived_records_page and page > 1:
                    pass # Show empty list if page is too high
            else:
                if not search_query:
                    logging.info("[AdminArchivedFileView] No archived files found.")
                else:
                    logging.info(f"[AdminArchivedFileView] No archived files found matching search: '{search_query}'.")

        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminArchivedFileView] Unexpected error: {error_message}", exc_info=True)

        endpoint_args = {}
        if search_query:
            endpoint_args['q'] = search_query

        # We will create 'admin/archived_file_list.html' in the next step
        return self.render('admin/archived_file_list.html',
                           records=archived_records_page,
                           error_message=error_message,
                           current_page=page,
                           total_pages=total_pages,
                           per_page=per_page,
                           total_records=total_records,
                           search_query=search_query,
                           endpoint_args=endpoint_args)

    # We will add restore_view and permanently_delete_view methods here later