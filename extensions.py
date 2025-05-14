from flask_login import LoginManager
from flask_jwt_extended import JWTManager
# If other extensions like mail were causing similar issues, they'd go here too.
# from flask_mail import Mail

login_manager = LoginManager()
jwt = JWTManager() # Define the instance here
# mail = Mail() # Example if mail was also structured this way

# Global state variables can also live here to be centrally accessible
upload_progress_data: dict = {}
download_prep_data: dict = {}