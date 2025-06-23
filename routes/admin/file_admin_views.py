# # routes/admin/file_admin_views.py
# import logging
# from flask_admin.babel import gettext
# from flask_admin.base import BaseView, expose
# from flask import redirect, url_for, request, flash, abort 
# import json
# from flask_login import current_user
# from math import ceil
# from database import (
#     get_all_file_metadata,
#     find_metadata_by_access_id,
#     archive_file_record_by_access_id, # For the delete/archive action
#     find_archived_metadata_by_access_id # Used in delete_view to get name after archive
# )

# class FileMetadataView(BaseView):
#     def is_accessible(self):
#         logging.warning("FileMetadataView is_accessible is temporarily returning True. REMOVE FOR PRODUCTION.")
#         return current_user.is_authenticated and getattr(current_user, 'is_admin', False)

#     def inaccessible_callback(self, name, **kwargs):
#         logging.info(f"inaccessible_callback called for {name} in FileMetadataView, but is_accessible is True.")
#         if not current_user.is_authenticated:
#             flash('Please log in to access this page.', 'info')
#             return redirect(url_for('admin_auth.login', next=request.url))
#         else:
#             flash('You do not have permission to access this page.', 'danger')
#             return redirect(url_for('admin_auth.login'))

#     @expose('/')
#     def index(self):
#         search_query = request.args.get('q', '').strip() 
#         user_type_filter = request.args.get('user_type', None)
#         page = request.args.get('page', 1, type=int)
#         per_page = 20
#         total_records = 0
#         total_pages = 1
#         records_list_page = []
#         error_message = None

#         try:
#             all_records_data, db_error = get_all_file_metadata(search_query=search_query, user_type_filter=user_type_filter) 

#             if db_error:
#                 error_message = f"Error fetching file metadata: {db_error}"
#             elif all_records_data is not None: # Check for None explicitly
#                 total_records = len(all_records_data)
#                 total_pages = ceil(total_records / per_page) if per_page > 0 else 1
#                 if total_pages == 0 and total_records > 0: total_pages = 1


#                 start_index = (page - 1) * per_page
#                 end_index = start_index + per_page
#                 records_list_page = all_records_data[start_index:end_index]

#                 if not records_list_page and page > 1 and total_records > 0:
#                     logging.info(f"[AdminFileMetadataView] Requested page {page} is out of range for current filters.")
#             else: # all_records_data is None (error occurred) or empty list (no match)
#                 log_msg_parts = []
#                 if search_query: log_msg_parts.append(f"search: '{search_query}'")
#                 if user_type_filter: log_msg_parts.append(f"user_type: '{user_type_filter}'")
                
#                 if log_msg_parts:
#                     logging.info(f"[AdminFileMetadataView] No file metadata records found matching {', '.join(log_msg_parts)}.")
#                 else:
#                     logging.info("[AdminFileMetadataView] No file metadata records found in the database at all.")


#         except Exception as e:
#             error_message = f"An unexpected error occurred: {str(e)}"
#             logging.error(f"[AdminFileMetadataView] Unexpected error: {error_message}", exc_info=True)


#         endpoint_args = {}
#         if search_query: 
#             endpoint_args['q'] = search_query
#         if user_type_filter: # <<< NEW
#             endpoint_args['user_type'] = user_type_filter # <<< NEW

#         return self.render('admin/file_metadata_list.html',
#                            records=records_list_page,
#                            error_message=error_message,
#                            current_page=page,
#                            total_pages=total_pages,
#                            per_page=per_page,
#                            total_records=total_records,
#                            search_query=search_query,
#                            user_type_filter=user_type_filter, # Pass to template
#                            endpoint_args=endpoint_args) 

#     @expose('/details/<access_id>')
#     def details_view(self, access_id):
#         record = None
#         error_message = None
#         try:
#             record_data, db_error = find_metadata_by_access_id(access_id)
#             if db_error:
#                 error_message = f"Error fetching record details for {access_id}: {db_error}"
#             elif record_data:
#                 record = record_data
#             else:
#                 error_message = f"Record with Access ID '{access_id}' not found."
#         except Exception as e:
#             error_message = f"An unexpected error occurred while fetching details for {access_id}: {str(e)}"
#         if not record and not error_message:
#              error_message = f"Record with Access ID '{access_id}' not found."
#         # For "Back to List" button, preserve filters
#         back_to_list_args = {}
#         if request.args.get('q'): back_to_list_args['q'] = request.args.get('q')
#         if request.args.get('page'): back_to_list_args['page'] = request.args.get('page')
#         if request.args.get('user_type'): back_to_list_args['user_type'] = request.args.get('user_type')
#         return self.render('admin/file_metadata_details.html',
#                            record=record,
#                            error_message=error_message,
#                            json=json,
#                            back_to_list_args=back_to_list_args)

#     @expose('/delete/<access_id>', methods=('POST',))
#     def delete_view(self, access_id):
#         log_prefix = f"[AdminArchiveFile-{access_id}]"
#         admin_username_for_archive = "AdminPanelAction"
#         display_name_for_flash = access_id

#         try:
#             original_record, _ = find_metadata_by_access_id(access_id)
#             if original_record:
#                 display_name_for_flash = original_record.get('batch_display_name', original_record.get('original_filename', access_id))

#             success, msg = archive_file_record_by_access_id(access_id, admin_username_for_archive)
#         #     temp_archived_record, _ = find_archived_metadata_by_access_id(access_id) # Check if it's in archive
#         #     if temp_archived_record and success: # If found in archive and operation was successful
#         #          display_name_for_flash = temp_archived_record.get('batch_display_name', temp_archived_record.get('original_filename', access_id))
#         #     elif not success: # if archive failed, try to get name from original location if still there
#         #          original_record, _ = find_metadata_by_access_id(access_id)
#         #          if original_record:
#         #               display_name_for_flash = original_record.get('batch_display_name', original_record.get('original_filename', access_id))


#         #     if success:
#         #         flash(gettext('Record "%(name)s" (%(id)s) archived successfully.', name=display_name_for_flash, id=access_id), 'success')
#         #         logging.info(f"{log_prefix} Record {access_id} archived by {admin_username_for_archive}.")
#         #     else:
#         #         flash(gettext('Error archiving record "%(name)s": %(error)s', name=display_name_for_flash, error=msg), 'danger')
#         #         logging.error(f"{log_prefix} Failed to archive {access_id}: {msg}")

#         # except Exception as e:
#         #     flash(gettext('An unexpected error occurred during archiving: %(error)s', error=str(e)), 'danger')
#         #     logging.error(f"{log_prefix} Unexpected error archiving {access_id}: {str(e)}", exc_info=True)

#         # return redirect(url_for('.index', q=request.args.get('q', ''), page=request.args.get('page', '1')))
#             if success:
#                 # If archive was successful, name might have been based on original. Let's re-check from archive for consistency if needed,
#                 # but usually, the original name is fine for the flash.
#                 flash(gettext('Record "%(name)s" (%(id)s) archived successfully.', name=display_name_for_flash, id=access_id), 'success')
#                 logging.info(f"{log_prefix} Record {access_id} archived by {admin_username_for_archive}.")
#             else:
#                 flash(gettext('Error archiving record "%(name)s": %(error)s', name=display_name_for_flash, error=msg), 'danger')
#                 logging.error(f"{log_prefix} Failed to archive {access_id}: {msg}")

#         except Exception as e:
#             flash(gettext('An unexpected error occurred during archiving: %(error)s', error=str(e)), 'danger')
#             logging.error(f"{log_prefix} Unexpected error archiving {access_id}: {str(e)}", exc_info=True)

#         # --- MODIFIED: Preserve user_type filter on redirect ---
#         return redirect(url_for('.index', 
#                                 q=request.args.get('q', ''), 
#                                 page=request.args.get('page', '1'), 
#                                 user_type=request.args.get('user_type')))



# In routes/admin/file_admin_views.py

import logging
from flask_admin.babel import gettext
from flask_admin.base import BaseView, expose
from flask import redirect, url_for, request, flash, Response
from markupsafe import Markup  # <-- ADD THIS IMPORT
import json
import io
import mimetypes
from flask_login import current_user
from datetime import datetime
from math import ceil
from database import (
    get_all_file_metadata,
    find_metadata_by_access_id,
    archive_file_record_by_access_id,
    get_file_chunks_data,
    get_metadata_collection # <-- ADD THIS LINE
)

class FileMetadataView(BaseView):
    def is_accessible(self):
        # This check is correct for production
        return current_user.is_authenticated and getattr(current_user, 'is_admin', False)

    def inaccessible_callback(self, name, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'info')
            return redirect(url_for('admin_auth.login', next=request.url))
        else:
            flash('You do not have permission to access this page.', 'danger')
            return redirect(url_for('admin_auth.login'))

    # --- START OF STEP 1 CHANGES ---

    def _actions_formatter(self, context, model, name):
        """
        This is now an instance method. 'self' refers to the view instance.
        'model' is a dictionary containing the data for the current row.
        """
        # We use the record's 'access_id' to build the URL for our new preview page.
        preview_url = url_for('.preview_batch_view', access_id=model.get('access_id'))
        
        # This is the URL for the 'Archive' action. It needs the database _id.
        archive_url = url_for('.archive_action', record_id=str(model.get('_id')))
        
        # Build the HTML for the buttons.
        preview_btn = f'<a href="{preview_url}" class="btn btn-xs btn-info">Preview</a>'
        
        archive_btn = f'''
            <form method="POST" action="{archive_url}" style="display: inline-block;">
                <button type="submit" class="btn btn-xs btn-danger"
                        onclick="return confirm('Are you sure you want to archive this record?');">
                  Archive
                </button>
            </form>
        '''
        
        return Markup(f'<div class="action">{preview_btn} {archive_btn}</div>')

    # Tell Flask-Admin to use our custom method for the 'actions' column.
    column_formatters = {
        'actions': _actions_formatter
    }

    # Add 'actions' to the list of columns to display.
    # We remove the old details_view and delete_view as they are replaced by our formatter.
    column_list = ('access_id', 'username', 'batch_display_name', 'is_batch', 'upload_timestamp', 'total_original_size', 'is_anonymous', 'actions')
    column_labels = {
        'batch_display_name': 'Display Name',
        'is_batch': 'Type',
        'upload_timestamp': 'Upload Date',
        'total_original_size': 'Size',
        'is_anonymous': 'Anonymous'
    }
    
    # We override the default formatting for some columns to make them look nicer.
    def _type_formatter(view, context, model, name):
        is_batch = model.get('is_batch', False)
        if is_batch:
            return Markup('<span class="status-badge status-info-custom">Batch</span>')
        else:
            return Markup('<span class="status-badge status-secondary-custom">Single File</span>')

    def _size_formatter(view, context, model, name):
        from config import format_bytes
        size = model.get('total_original_size', 0)
        return format_bytes(size)

    def _anon_formatter(view, context, model, name):
        is_anon = model.get('is_anonymous', False)
        if is_anon:
            return Markup('<span class="status-badge status-failed">Yes</span>')
        else:
            return Markup('<span class="status-badge status-active">No</span>')

    # Add our new formatters to the view
    column_formatters.update({
        'is_batch': _type_formatter,
        'total_original_size': _size_formatter,
        'is_anonymous': _anon_formatter,
    })


    @expose('/')
    def index(self):
        # This index view logic remains the same.
        search_query = request.args.get('q', '').strip() 
        user_type_filter = request.args.get('user_type', None)
        page = request.args.get('page', 1, type=int)
        per_page = 20
        total_records = 0
        total_pages = 1
        records_list_page = []
        error_message = None

        try:
            all_records_data, db_error = get_all_file_metadata(search_query=search_query, user_type_filter=user_type_filter) 
            if db_error:
                error_message = f"Error fetching file metadata: {db_error}"
            elif all_records_data is not None:
                total_records = len(all_records_data)
                total_pages = ceil(total_records / per_page) if per_page > 0 else 1
                if total_pages == 0 and total_records > 0: total_pages = 1
                start_index = (page - 1) * per_page
                end_index = start_index + per_page
                records_list_page = all_records_data[start_index:end_index]
            else:
                log_msg_parts = []
                if search_query: log_msg_parts.append(f"search: '{search_query}'")
                if user_type_filter: log_msg_parts.append(f"user_type: '{user_type_filter}'")
                if log_msg_parts: logging.info(f"[AdminFileMetadataView] No records found matching {', '.join(log_msg_parts)}.")
                else: logging.info("[AdminFileMetadataView] No file metadata records found.")
        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            logging.error(f"[AdminFileMetadataView] Unexpected error: {error_message}", exc_info=True)

        endpoint_args = {}
        if search_query: endpoint_args['q'] = search_query
        if user_type_filter: endpoint_args['user_type'] = user_type_filter

        # The list is rendered using the columns and formatters defined above.
        return self.render('admin/file_metadata_list.html',
                           list_view={'column_list': self.column_list, 'column_labels': self.column_labels, 'column_formatters': self.column_formatters},
                           records=records_list_page,
                           error_message=error_message,
                           current_page=page,
                           total_pages=total_pages,
                           per_page=per_page,
                           total_records=total_records,
                           search_query=search_query,
                           user_type_filter=user_type_filter,
                           endpoint_args=endpoint_args)

    @expose('/preview-batch/<access_id>')
    def preview_batch_view(self, access_id):
        """
        This view handles the display of the batch preview page.
        It fetches the main batch record and then extracts the list of
        individual files from its 'files_in_batch' field.
        """
        try:
            # --- START OF THE FIX ---
            # 1. Fetch the main batch document using its access_id.
            batch_record, error = find_metadata_by_access_id(access_id)
            
            if error:
                flash(f"Database error: {error}", 'danger')
                return redirect(url_for('.index'))
            
            if not batch_record:
                flash(f"No batch record found for Access ID: {access_id}", 'warning')
                return redirect(url_for('.index'))

            # 2. Extract the list of files from WITHIN the batch record.
            # This is the crucial step.
            files_in_batch_list = batch_record.get('files_in_batch', [])

            # 3. Aggregate the top-level batch details for display.
            total_size_bytes = batch_record.get('total_original_size', 0)
            from config import format_bytes
            
            upload_datetime = None
            timestamp_str = batch_record.get('upload_timestamp')
            if isinstance(timestamp_str, str):
                try:
                    upload_datetime = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except ValueError:
                    upload_datetime = None
            elif isinstance(timestamp_str, datetime):
                upload_datetime = timestamp_str

            batch_details = {
                'batch_display_name': batch_record.get('batch_display_name', 'N/A'),
                'access_id': batch_record.get('access_id'),
                'username': batch_record.get('username', 'Anonymous'),
                'upload_timestamp': upload_datetime,
                'total_size': format_bytes(total_size_bytes),
                'is_anonymous': batch_record.get('is_anonymous', False),
                # We get the count from the list we just extracted.
                'file_count': len(files_in_batch_list) 
            }
            
            # 4. Render the template, passing the batch details AND the extracted list of files.
            return self.render('admin/preview_batch.html', 
                                batch_details=batch_details,
                                files_in_batch=files_in_batch_list) # Pass the list here
            # --- END OF THE FIX ---

        except Exception as e:
            logging.error(f"Error in preview_batch_view for access_id {access_id}: {e}", exc_info=True)
            flash("An unexpected error occurred while loading the preview page.", 'danger')
            return redirect(url_for('.index'))
        

    @expose('/archive/<record_id>', methods=('POST',))
    def archive_action(self, record_id):
        # This logic replaces your old delete_view and is more robust.
        log_prefix = f"[AdminArchiveFile-{record_id}]"
        admin_username_for_archive = current_user.username if current_user.is_authenticated else "AdminPanelAction"
        
        try:
            # We need the access_id to perform the archive. The record_id is the db _id.
            # We must fetch the record first to get its access_id.
            coll, _ = get_metadata_collection()
            from bson import ObjectId
            record_doc = coll.find_one({'_id': ObjectId(record_id)})

            if not record_doc:
                flash(gettext('Record not found.'), 'error')
                return redirect(url_for('.index'))

            access_id_to_archive = record_doc.get('access_id')
            display_name_for_flash = record_doc.get('batch_display_name', record_doc.get('original_filename', access_id_to_archive))

            success, msg = archive_file_record_by_access_id(access_id_to_archive, admin_username_for_archive)

            if success:
                flash(gettext('Record "%(name)s" archived successfully.', name=display_name_for_flash), 'success')
            else:
                flash(gettext('Error archiving record "%(name)s": %(error)s', name=display_name_for_flash, error=msg), 'danger')

        except Exception as e:
            flash(gettext('An unexpected error occurred during archiving: %(error)s', error=str(e)), 'danger')
            logging.error(f"{log_prefix} Unexpected error: {str(e)}", exc_info=True)

        return redirect(url_for('.index', 
                                q=request.args.get('q', ''), 
                                page=request.args.get('page', '1'), 
                                user_type=request.args.get('user_type')))
    
    
    @expose('/view-content/<string:batch_id>/<path:filename>')
    def view_file_content(self, batch_id, filename):
        """
        Fetches, reassembles, and serves the content of a specific file
        within a batch, identified by its batch_id and filename.
        """
        try:
            # We now pass both batch_id and filename to our data retrieval function
            file_chunks, err = get_file_chunks_data(batch_id, filename)

            if err:
                # Handle the specific error for incomplete records
                if "incomplete" in err or "not found" in err:
                    error_message = (f"Cannot preview file '{filename}': The database record is incomplete or the file could not be found within the batch. "
                                     "This may happen if the upload was interrupted.")
                    flash(error_message, 'warning')
                else:
                    flash(f"An error occurred while retrieving file content: {err}", 'danger')
                
                return redirect(request.referrer or url_for('.index'))

            # Reassemble the file in memory
            file_data = io.BytesIO()
            for chunk in file_chunks:
                file_data.write(chunk)
            file_data.seek(0)
            
            # Serve the file intelligently (this part remains the same)
            mime_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
            INLINE_MIMETYPES = ['image/jpeg', 'image/png', 'image/gif', 'text/plain', 'application/pdf', 'video/mp4']
            disposition = 'inline' if mime_type in INLINE_MIMETYPES else 'attachment'
            
            response = Response(file_data, mimetype=mime_type)
            response.headers.set('Content-Disposition', f'{disposition}; filename="{filename}"')
            return response

        except Exception as e:
            logging.error(f"Failed to process preview for file '{filename}' in batch {batch_id}: {e}", exc_info=True)
            flash("An unexpected server error occurred while preparing the file preview.", 'danger')
            return redirect(request.referrer or url_for('.index'))
    
    
    # --- END OF STEP 1 CHANGES ---