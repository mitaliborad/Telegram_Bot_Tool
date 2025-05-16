# routes/admin/user_admin_views.py
import logging
from flask_admin.babel import gettext
from flask_admin.base import BaseView, expose
# from flask_login import current_user # Not strictly needed while security is off
from flask import redirect, url_for, request, flash # Keep flash for feedback
from math import ceil
import re
import database

class UserView(BaseView):
    def is_accessible(self):
        logging.warning("UserView is_accessible is temporarily returning True. REMOVE FOR PRODUCTION.")
        return True

    def inaccessible_callback(self, name, **kwargs):
        logging.info(f"inaccessible_callback called for {name} in UserView, but is_accessible is True.")
        flash(gettext('Access Denied (inaccessible_callback). This should not happen with current settings.'), 'warning')
        # When security is on, this should redirect based on current_user state
        return redirect(url_for('auth.login')) # Fallback redirect

    @expose('/')
    def index(self):
        search_query = request.args.get('q', '').strip()
        page = request.args.get('page', 1, type=int)
        per_page = 20
        total_users = 0
        total_pages = 1
        users_list_page = []
        error_message = None

        try:
            all_users_data, db_error = database.get_all_users(search_query=search_query)

            if db_error:
                error_message = f"Error fetching users: {db_error}"
            elif all_users_data:
                total_users = len(all_users_data)
                total_pages = ceil(total_users / per_page) if per_page > 0 else 1
                if total_pages == 0 and total_users > 0: total_pages = 1

                start_index = (page - 1) * per_page
                end_index = start_index + per_page
                users_list_page = all_users_data[start_index:end_index]

                if not users_list_page and page > 1:
                    pass
            else:
                # If search_query is present and no users, it's not an error, just no results
                if not search_query:
                    logging.info("[AdminUserView] No users found in the database (no search).")
                else:
                    logging.info(f"[AdminUserView] No users found matching search: '{search_query}'.")


        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminUserView] Unexpected error: {error_message}", exc_info=True)

        endpoint_args = {}
        if search_query:
            endpoint_args['q'] = search_query

        return self.render('admin/user_list.html',
                           users=users_list_page,
                           error_message=error_message,
                           current_page=page,
                           total_pages=total_pages,
                           per_page=per_page,
                           total_users=total_users,
                           search_query=search_query,
                           endpoint_args=endpoint_args)

    @expose('/delete/<user_id_str>', methods=('POST',))
    def delete_user_view(self, user_id_str):
        try:
            user_to_delete_doc, find_err = database.find_user_by_id_str(user_id_str)
            if find_err :
                flash(gettext('Error finding user for deletion details: %(error)s', error=find_err), 'danger')
                return redirect(url_for('.index'))
            if not user_to_delete_doc:
                flash(gettext('User with ID %(id)s not found. Already deleted?', id=user_id_str), 'warning')
                return redirect(url_for('.index'))

            username_for_flash = user_to_delete_doc.get('username', user_id_str)
            deleted_count, db_error_msg = database.delete_user_by_id(user_id_str)

            if db_error_msg:
                flash(gettext('Error deleting user: %(error)s', error=db_error_msg), 'danger')
            elif deleted_count > 0:
                flash(gettext('User "%(name)s" (ID: %(id)s) deleted successfully.', name=username_for_flash, id=user_id_str), 'success')
            else:
                flash(gettext('Failed to delete user %(id)s. It might have been already deleted.', id=user_id_str), 'warning')
        except Exception as e:
            flash(gettext('An unexpected error occurred during user deletion: %(error)s', error=str(e)), 'danger')
        return redirect(url_for('.index'))