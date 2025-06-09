# TELEGRAM_BOT_TOOL/routes/admin/auth_routes.py
import logging
from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, current_user
from werkzeug.security import check_password_hash

# Assuming your database functions and User model are accessible via 'database' package
# If find_user_by_username and User are directly in database/__init__.py or database/user_models.py
from database import find_user_by_username, User
# If your forms.py is inside routes/admin/
from .forms import LoginForm

# Create a Blueprint for admin authentication routes
admin_auth_bp = Blueprint('admin_auth', __name__) # <--- Key: No template_folder here

@admin_auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    from flask import current_app # Import current_app
    logging.info(f"DEBUG: In login route. current_app.template_folder = {current_app.template_folder}")
    logging.info(f"DEBUG: In login route. current_app.root_path = {current_app.root_path}")
    # If user is already authenticated and is an admin, redirect them to the admin dashboard
    # Make sure 'admin.index' is the correct endpoint for your Flask-Admin dashboard
    if current_user.is_authenticated and getattr(current_user, 'is_admin', False):
        return redirect(url_for('admin.index')) 

    form = LoginForm()
    if form.validate_on_submit():
        username_to_check = form.username.data
        password_to_check = form.password.data
        remember_me = form.remember.data

        user_doc, error_msg = find_user_by_username(username_to_check)

        if error_msg:
            flash(f"Database error: {error_msg}", "danger")
            # This render_template call should now correctly find 'templates/admin_auth/login.html'
            return render_template('admin/admin_auth/login.html', title='Admin Login', form=form)

        if user_doc:
            user_obj = User(user_doc)
            if user_obj.check_password(password_to_check):
                if user_obj.is_admin:
                    login_user(user_obj, remember=remember_me)
                    flash('Login successful!', 'success')
                    next_page = request.args.get('next')
                    return redirect(next_page or url_for('admin.index'))
                else:
                    flash('Access Denied: You do not have admin privileges.', 'warning')
            else:
                flash('Login Unsuccessful. Please check username and password.', 'danger')
        else:
            flash('Login Unsuccessful. User not found.', 'danger')
    
    # This render_template call is the one that was failing
    return render_template('admin/admin_auth/login.html', title='Admin Login', form=form)


@admin_auth_bp.route('/logout')
def logout():
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('admin_auth.login'))