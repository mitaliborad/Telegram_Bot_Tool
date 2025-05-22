# # app_setup.py
# import logging
# import os
# from flask_cors import CORS
# from dotenv import load_dotenv
# from flask_admin import Admin
# from flask_admin.contrib.fileadmin import FileAdmin
# from routes.admin.user_admin_views import UserView
# from routes.admin.file_admin_views import FileMetadataView
# from routes.admin.dashboard_view import MyAdminIndexView
# from routes.admin.archive_admin_views import ArchivedFileView
# from routes.admin.archived_user_admin_views import ArchivedUserView
# load_dotenv()

# from config import app, mail, format_bytes 
# from extensions import login_manager, jwt, upload_progress_data, download_prep_data

# admin = Admin(
#     app,
#     name='Storage Admin',
#     template_mode='bootstrap4',
#     url='/admin',  
#     index_view=MyAdminIndexView(
#         name="Dashboard",  
#         endpoint='admin_dashboard',  
#         url='/admin'  
#     )
# )
# logging.info("Flask-Admin initialized with custom dashboard. Accessible at /admin")

# admin.add_view(UserView(name='Manage Users', endpoint='users', menu_icon_type='glyph', menu_icon_value='glyphicon-user'))
# admin.add_view(FileMetadataView(name='File Uploads', endpoint='files', menu_icon_type='glyph', menu_icon_value='glyphicon-file'))
# logging.info("Flask-Admin UserView and FileMetadataView registered.")
# admin.add_view(ArchivedFileView(name='Archived Files', endpoint='archivedfiles', category='File Management', menu_icon_type='glyph', menu_icon_value='glyphicon-folder-open'))
# logging.info("Flask-Admin UserView, FileMetadataView, and ArchivedFileView registered.")

# admin.add_view(ArchivedUserView(name='Archived Users', endpoint='archivedusers', category='User Management', menu_icon_type='glyph', menu_icon_value='glyphicon-trash'))
# logging.info("Flask-Admin UserView, FileMetadataView, ArchivedFileView, and ArchivedUserView registered.")

# # --- Flask Application Extensions Setup ---
# app.jinja_env.filters['format_bytes'] = format_bytes

# env_frontend_url_setting = os.environ.get('FRONTEND_URL')
# allowed_origins_config = "*"
# if env_frontend_url_setting and env_frontend_url_setting.strip() == "*":
#     allowed_origins_config = "*"
# elif env_frontend_url_setting:
#     allowed_origins_config = [url.strip() for url in env_frontend_url_setting.split(',')]
# else:
#     logging.warning("FRONTEND_URL environment variable not set. Defaulting CORS to allow all origins ('*'). "
#                     "For production, it's recommended to set FRONTEND_URL to your specific frontend domain(s).")
# CORS(app,
#      origins=allowed_origins_config,
#      supports_credentials=True,
#      methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
# logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins_config}")

# # Initialize JWT with the app object
# jwt.init_app(app) 
# logging.info("Flask-JWT-Extended initialized.")

# login_manager.init_app(app) 
# login_manager.login_view = 'auth.login' 
# login_manager.login_message = u"Please log in to access this page."
# login_manager.login_message_category = "info"
# logging.info("Flask-Login initialized.")

# logging.info("Global state variables (from extensions.py) are available.")

# # admin = Admin(app, name='Storage Admin', template_mode='bootstrap4', url='/admin')
# # logging.info("Flask-Admin initialized. Accessible at /admin")

# # admin.add_view(UserView(name='Manage Users', endpoint='users'))
# # logging.info("Flask-Admin UserView registered.")

# # admin.add_view(FileMetadataView(name='File Uploads', endpoint='files'))
# # logging.info("Flask-Admin FileMetadataView registered.")

# # --- Import and Register Blueprints ---
# from routes.password_reset_routes import password_reset_bp 
# from routes.auth_routes import auth_bp                     
# from routes.upload_routes import upload_bp                 
# from routes.download_routes import download_bp             
# from routes.file_routes import file_bp 
# from routes.archive_routes import archive_bp

# if 'password_reset' not in app.blueprints:
#     app.register_blueprint(password_reset_bp)
#     logging.info("Password Reset Blueprint registered.")
# else:
#     logging.warning("Blueprint 'password_reset' was already registered. Skipping re-registration.")

# if 'auth' not in app.blueprints:
#     app.register_blueprint(auth_bp)
#     logging.info("Auth Blueprint registered.")
# else:
#     logging.warning("Blueprint 'auth' was already registered. Skipping re-registration.")

# if 'upload' not in app.blueprints:
#     app.register_blueprint(upload_bp)
#     logging.info("Upload Blueprint registered.")
# else:
#     logging.warning("Blueprint 'upload' was already registered. Skipping re-registration.")

# if 'download' not in app.blueprints:
#     app.register_blueprint(download_bp)
#     logging.info("Download Blueprint registered.")
# else:
#     logging.warning("Blueprint 'download' was already registered. Skipping re-registration.")

# if 'file' not in app.blueprints:
#     app.register_blueprint(file_bp)
#     logging.info("File Blueprint registered.")
# else:
#     logging.warning("Blueprint 'file' was already registered. Skipping re-registration.")
    
# if 'archive' not in app.blueprints: 
#     app.register_blueprint(archive_bp)
#     logging.info("Archive Blueprint registered.")
# else:
#     logging.warning("Blueprint 'archive' was already registered. Skipping re-registration.")
    

# # --- Application Runner ---
# if __name__ == '__main__':
#     logging.info("Starting Flask development server...")
#     app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)


# app_setup.py
import logging # Standard library logging
import os
from flask_cors import CORS
from dotenv import load_dotenv
from flask_admin import Admin
# Ensure Flask-Admin views are imported correctly
from routes.admin.user_admin_views import UserView
from routes.admin.file_admin_views import FileMetadataView
from routes.admin.dashboard_view import MyAdminIndexView
from routes.admin.archive_admin_views import ArchivedFileView
from routes.admin.archived_user_admin_views import ArchivedUserView

# Load environment variables from .env file at the very beginning
load_dotenv()

# Import 'app' and other configurations from config.py
# This will also trigger the logging setup within config.py
from config import app, mail, format_bytes, format_time, LOG_DIR # Import LOG_DIR if needed here, or rely on config.py

# Now that config.py (and its logging setup) has run, subsequent logging calls
# in this file will use that configuration.
logging.info("app_setup.py: Started application setup.")


# --- Flask-Admin Setup ---
# Note: MyAdminIndexView might require the app context or database to be ready
# if it performs operations upon initialization.
admin_dashboard_view = MyAdminIndexView(
    name="Dashboard",
    endpoint='admin_dashboard',
    url='/admin'
)
admin = Admin(
    app,
    name='Storage Admin',
    template_mode='bootstrap4',
    url='/admin',
    index_view=admin_dashboard_view
)
logging.info("Flask-Admin initialized with custom dashboard. Accessible at /admin")

# Add Admin views
admin.add_view(UserView(name='Manage Users', endpoint='users', menu_icon_type='glyph', menu_icon_value='glyphicon-user'))
admin.add_view(FileMetadataView(name='File Uploads', endpoint='files', menu_icon_type='glyph', menu_icon_value='glyphicon-file'))
admin.add_view(ArchivedFileView(name='Archived Files', endpoint='archivedfiles', category='File Management', menu_icon_type='glyph', menu_icon_value='glyphicon-folder-open'))
admin.add_view(ArchivedUserView(name='Archived Users', endpoint='archivedusers', category='User Management', menu_icon_type='glyph', menu_icon_value='glyphicon-trash'))
logging.info("Flask-Admin views (Users, Files, Archived Files, Archived Users) registered.")


# --- Flask Application Extensions Setup ---
app.jinja_env.filters['format_bytes'] = format_bytes
app.jinja_env.filters['format_time'] = format_time # If you want to use format_time in Jinja templates

# --- CORS Configuration ---
env_frontend_url_setting = os.environ.get('FRONTEND_URL')
allowed_origins_config = "*" # Default to all

if env_frontend_url_setting:
    if env_frontend_url_setting.strip() == "*":
        allowed_origins_config = "*"
    else:
        allowed_origins_config = [url.strip() for url in env_frontend_url_setting.split(',') if url.strip()]
else:
    logging.warning("FRONTEND_URL environment variable not set. Defaulting CORS to allow all origins ('*'). "
                    "For production, it's recommended to set FRONTEND_URL to your specific frontend domain(s).")

CORS(app,
     origins=allowed_origins_config,
     supports_credentials=True,
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins_config}")

# --- Other Extensions Initialization ---
from extensions import login_manager, jwt # Assuming these are initialized instances

# Initialize JWT with the app object
jwt.init_app(app)
logging.info("Flask-JWT-Extended initialized.")

# Initialize Flask-Login
login_manager.init_app(app)
login_manager.login_view = 'auth.login' # Blueprint.route_function_name
login_manager.login_message = u"Please log in to access this page."
login_manager.login_message_category = "info"
logging.info("Flask-Login initialized.")

# --- Import and Register Blueprints ---
# It's good practice to group these imports
from routes.password_reset_routes import password_reset_bp
from routes.auth_routes import auth_bp
from routes.upload_routes import upload_bp
from routes.download_routes import download_bp
from routes.file_routes import file_bp
from routes.archive_routes import archive_bp
# from routes.admin_routes import admin_bp # If you have a separate admin blueprint for non-Flask-Admin routes

blueprints_to_register = {
    'password_reset': password_reset_bp,
    'auth': auth_bp,
    'upload': upload_bp,
    'download': download_bp,
    'file': file_bp,
    'archive': archive_bp,
    # 'admin_custom': admin_bp, # Example
}

registered_blueprints_count = 0
for name, bp_instance in blueprints_to_register.items():
    if bp_instance: # Check if the blueprint object exists
        if name not in app.blueprints:
            app.register_blueprint(bp_instance)
            logging.info(f"Blueprint '{name}' registered.")
            registered_blueprints_count += 1
        else:
            logging.warning(f"Blueprint '{name}' was already registered. Skipping re-registration.")
    else:
        logging.warning(f"Blueprint instance for '{name}' is None. Skipping registration.")

if registered_blueprints_count > 0:
    logging.info(f"Total of {registered_blueprints_count} blueprints registered.")
else:
    logging.warning("No new blueprints were registered.")


# --- Database Connection Check (Optional but good for early diagnostics) ---
# You might want to attempt a basic DB operation here to ensure connection,
# especially if APP_ENV is 'production' or 'grading'.
# This requires your DB connection logic to be accessible.
# Example:
# from database.connection import get_db
# db, error = get_db()
# if error:
#     logging.critical(f"Failed to connect to database on startup: {error}")
# else:
#     logging.info("Successfully established initial contact with the database.")
# Note: This would attempt connection even if only running dev server. Consider conditionalizing.


# --- Application Runner ---
if __name__ == '__main__':
    # Determine run mode from APP_ENV, default to 'development'
    app_env = os.environ.get('APP_ENV', 'development').lower()
    is_development_mode = (app_env == 'development')

    logging.info(f"Starting Flask server in '{app_env}' mode...")
    logging.info(f"  Debug mode: {is_development_mode}")
    logging.info(f"  Reloader: {is_development_mode}")

    # Ensure the primary log directory from config.py exists (as a safeguard, though config.py should handle it)
    if not os.path.exists(LOG_DIR):
        try:
            os.makedirs(LOG_DIR)
            logging.info(f"Ensured logging directory exists: {LOG_DIR} (from app_setup.py check)")
        except OSError as e:
            logging.error(f"Could not create logging directory {LOG_DIR} (from app_setup.py check): {e}")

    app.run(
        host=os.environ.get('FLASK_RUN_HOST', '0.0.0.0'),
        port=int(os.environ.get('FLASK_RUN_PORT', 5000)),
        debug=is_development_mode,
        use_reloader=is_development_mode # This is key to disable reloader for 'grading' or 'production'
    )