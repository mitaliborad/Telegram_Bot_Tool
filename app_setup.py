# app_setup.py
import logging
import os
from flask_cors import CORS
from dotenv import load_dotenv
from flask_admin import Admin
from routes.admin.user_admin_views import UserView
from routes.admin.file_admin_views import FileMetadataView
from routes.admin.dashboard_view import MyAdminIndexView
from routes.admin.archive_admin_views import ArchivedFileView
from routes.admin.archived_user_admin_views import ArchivedUserView
from routes.admin.auth_routes import admin_auth_bp
from flask_login import LoginManager
from database import find_user_by_id_str, User

load_dotenv()
from config import app, mail, format_bytes, format_time, LOG_DIR
logging.info("app_setup.py: Started application setup.")

# --- Flask-Admin Setup ---
admin_dashboard_view = MyAdminIndexView(name="Dashboard", endpoint='admin_dashboard', url='/admin')
admin = Admin(app, name='Storage Admin', template_mode='bootstrap4', url='/admin', index_view=admin_dashboard_view)
logging.info("Flask-Admin initialized with custom dashboard. Accessible at /admin")
admin.add_view(UserView(name='Manage Users', endpoint='users', menu_icon_type='glyph', menu_icon_value='glyphicon-user'))
admin.add_view(FileMetadataView(name='File Uploads', endpoint='files', menu_icon_type='glyph', menu_icon_value='glyphicon-file'))
admin.add_view(ArchivedFileView(name='Archived Files', endpoint='archivedfiles', category='File Management', menu_icon_type='glyph', menu_icon_value='glyphicon-folder-open'))
admin.add_view(ArchivedUserView(name='Archived Users', endpoint='archivedusers', category='User Management', menu_icon_type='glyph', menu_icon_value='glyphicon-trash'))
logging.info("Flask-Admin views registered.")

# --- Flask-Login Setup (ensure it's initialized only once) ---
# login_manager instance should be from extensions.py if shared, or initialized here if local to app_setup
from extensions import login_manager as ext_login_manager # Assuming it's from extensions
# If you had a local login_manager = LoginManager() here, remove it if ext_login_manager is used.

login_manager = LoginManager()
login_manager.init_app(app)

@login_manager.user_loader
def load_user(user_id_str):
    user_doc, _ = find_user_by_id_str(user_id_str)
    if user_doc:
        try:
            return User(user_doc)
        except ValueError as e:
            app.logger.error(f"Error creating User object for user_id {user_id_str} in user_loader: {e}")
            return None
    return None



login_manager.login_view = 'admin_auth.login'
login_manager.login_message = "You must be logged in as an admin to access this page."
login_manager.login_message_category = "info"
# ext_login_manager.init_app(app) # This should have been done in extensions.py where it's created OR here, but only once.
                             # If extensions.py has login_manager = LoginManager() and then you do init_app(app) there,
                             # then you don't need to call it again here.
                             # If extensions.py just has login_manager = LoginManager(), then init_app(app) here is fine.
logging.info("Flask-Login user_loader configured.")


# --- Flask Application Extensions Setup ---
app.jinja_env.filters['format_bytes'] = format_bytes
app.jinja_env.filters['format_time'] = format_time

# --- CORS Configuration ---
env_frontend_url_setting = os.environ.get('FRONTEND_URL')
allowed_origins_config = "*"
if env_frontend_url_setting:
    if env_frontend_url_setting.strip() == "*":
        allowed_origins_config = "*"
    else:
        allowed_origins_config = [url.strip() for url in env_frontend_url_setting.split(',') if url.strip()]
else:
    logging.warning("FRONTEND_URL environment variable not set. Defaulting CORS to allow all origins ('*').")
CORS(app, origins=allowed_origins_config, supports_credentials=True, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins_config}")

# --- Other Extensions Initialization ---
from extensions import jwt # Assuming jwt is an initialized instance from extensions.py
jwt.init_app(app) # Ensure this is called only once
logging.info("Flask-JWT-Extended initialized.")
# Flask-Login should already be initialized via ext_login_manager.init_app(app)
# If it's not, and ext_login_manager is just an uninitialized LoginManager() object from extensions,
# then you would do: ext_login_manager.init_app(app) here.

# --- Import and Register Blueprints ---
from routes.password_reset_routes import password_reset_bp
from routes.auth_routes import auth_bp
from routes.upload_routes import upload_bp
from routes.download_routes import download_bp as download_prefixed_bp, download_sse_bp # Rename imported download_bp
from routes.file_routes import file_bp
from routes.archive_routes import archive_bp

# Define blueprints with their intended prefixes
# (Blueprint Instance, URL Prefix or None for root)
blueprints_to_register_with_prefix = {
    'password_reset': (password_reset_bp, None),
    'auth': (auth_bp, None),
    'upload': (upload_bp, None),
    'download_prefixed': (download_prefixed_bp, '/download'),
    'download_sse': (download_sse_bp, None),
    'file_routes': (file_bp, '/api'), 
    'archive': (archive_bp, '/api/archive'), 
}

registered_blueprints_count = 0
for name, config_tuple in blueprints_to_register_with_prefix.items():
    bp_instance, url_prefix = config_tuple
    if bp_instance:
        if bp_instance.name in app.blueprints:
            logging.warning(f"Blueprint with internal name '{bp_instance.name}' (config key: '{name}') seems to be already registered or name conflicts. Skipping this registration entry.")
        else:
            app.register_blueprint(bp_instance, url_prefix=url_prefix)
            logging.info(f"Blueprint '{bp_instance.name}' (config key: '{name}') registered with prefix: {url_prefix}")
            registered_blueprints_count += 1
    else:
        logging.warning(f"Blueprint instance for config key '{name}' is None. Skipping registration.")

if registered_blueprints_count > 0:
    logging.info(f"Total of {registered_blueprints_count} blueprints registered via loop.")
else:
    logging.warning("No new blueprints were registered via loop.")

app.register_blueprint(admin_auth_bp, url_prefix='/admin')

# --- Application Runner ---
if __name__ == '__main__':
    app_env = os.environ.get('APP_ENV', 'development').lower()
    is_development_mode = (app_env == 'development')
    logging.info(f"Starting Flask server in '{app_env}' mode...")
    logging.info(f"  Debug mode: {is_development_mode}")
    logging.info(f"  Reloader: {is_development_mode}")
    if not os.path.exists(LOG_DIR):
        try:
            os.makedirs(LOG_DIR)
        except OSError as e:
            logging.error(f"Could not create logging directory {LOG_DIR}: {e}")
    app.run(
        host=os.environ.get('FLASK_RUN_HOST', '0.0.0.0'),
        port=int(os.environ.get('FLASK_RUN_PORT', 5000)),
        debug=is_development_mode,
        use_reloader=is_development_mode
    )