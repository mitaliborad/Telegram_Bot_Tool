# app_setup.py
import logging
import os
from flask_cors import CORS
from dotenv import load_dotenv
from flask_admin import Admin
from flask_admin.contrib.fileadmin import FileAdmin
from routes.admin.user_admin_views import UserView
from routes.admin.file_admin_views import FileMetadataView
from routes.admin.dashboard_view import MyAdminIndexView
load_dotenv()

from config import app, mail, format_bytes 
from extensions import login_manager, jwt, upload_progress_data, download_prep_data

admin = Admin(
    app,
    name='Storage Admin',
    template_mode='bootstrap4',
    url='/admin',  # This is the URL prefix for all admin views
    index_view=MyAdminIndexView(
        name="Dashboard",   # Name displayed in menu/breadcrumbs
        endpoint='admin_dashboard',  # <--- ADD/CHANGE THIS ENDPOINT TO BE UNIQUE
        url='/admin'  # This is the URL for this specific index view, relative to admin.url_prefix if set
                      # Or absolute if admin.url_prefix is not set.
                      # Since Admin's url is '/admin', and this view's url is also '/admin',
                      # this effectively means the dashboard is at the root of the admin interface.
    )
)
logging.info("Flask-Admin initialized with custom dashboard. Accessible at /admin")

admin.add_view(UserView(name='Manage Users', endpoint='users', menu_icon_type='glyph', menu_icon_value='glyphicon-user'))
admin.add_view(FileMetadataView(name='File Uploads', endpoint='files', menu_icon_type='glyph', menu_icon_value='glyphicon-file'))
logging.info("Flask-Admin UserView and FileMetadataView registered.")

# --- Flask Application Extensions Setup ---
app.jinja_env.filters['format_bytes'] = format_bytes

env_frontend_url_setting = os.environ.get('FRONTEND_URL')
allowed_origins_config = "*"
if env_frontend_url_setting and env_frontend_url_setting.strip() == "*":
    allowed_origins_config = "*"
elif env_frontend_url_setting:
    allowed_origins_config = [url.strip() for url in env_frontend_url_setting.split(',')]
else:
    logging.warning("FRONTEND_URL environment variable not set. Defaulting CORS to allow all origins ('*'). "
                    "For production, it's recommended to set FRONTEND_URL to your specific frontend domain(s).")
CORS(app, 
     origins=allowed_origins_config, 
     supports_credentials=True, 
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
logging.info(f"Flask-CORS initialized. Allowing origins: {allowed_origins_config}")

# Initialize JWT with the app object
jwt.init_app(app) 
logging.info("Flask-JWT-Extended initialized.")

login_manager.init_app(app) 
login_manager.login_view = 'auth.login' 
login_manager.login_message = u"Please log in to access this page."
login_manager.login_message_category = "info"
logging.info("Flask-Login initialized.")

logging.info("Global state variables (from extensions.py) are available.")

# admin = Admin(app, name='Storage Admin', template_mode='bootstrap4', url='/admin')
# logging.info("Flask-Admin initialized. Accessible at /admin")

# admin.add_view(UserView(name='Manage Users', endpoint='users'))
# logging.info("Flask-Admin UserView registered.")

# admin.add_view(FileMetadataView(name='File Uploads', endpoint='files'))
# logging.info("Flask-Admin FileMetadataView registered.")

# --- Import and Register Blueprints ---
from routes.password_reset_routes import password_reset_bp 
from routes.auth_routes import auth_bp                     
from routes.upload_routes import upload_bp                 
from routes.download_routes import download_bp             
from routes.file_routes import file_bp 

if 'password_reset' not in app.blueprints:
    app.register_blueprint(password_reset_bp)
    logging.info("Password Reset Blueprint registered.")
else:
    logging.warning("Blueprint 'password_reset' was already registered. Skipping re-registration.")

if 'auth' not in app.blueprints:
    app.register_blueprint(auth_bp)
    logging.info("Auth Blueprint registered.")
else:
    logging.warning("Blueprint 'auth' was already registered. Skipping re-registration.")

if 'upload' not in app.blueprints:
    app.register_blueprint(upload_bp)
    logging.info("Upload Blueprint registered.")
else:
    logging.warning("Blueprint 'upload' was already registered. Skipping re-registration.")

if 'download' not in app.blueprints:
    app.register_blueprint(download_bp)
    logging.info("Download Blueprint registered.")
else:
    logging.warning("Blueprint 'download' was already registered. Skipping re-registration.")

if 'file' not in app.blueprints:
    app.register_blueprint(file_bp)
    logging.info("File Blueprint registered.")
else:
    logging.warning("Blueprint 'file' was already registered. Skipping re-registration.")
    

# --- Application Runner ---
if __name__ == '__main__':
    logging.info("Starting Flask development server...")
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=True)