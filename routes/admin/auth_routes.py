# routes/admin/auth_routes.py
import logging
from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, current_user
from werkzeug.security import check_password_hash # For checking password

from database import find_user_by_username, User # Or find_user_by_email
from .forms import LoginForm # Import the LoginForm

# Create a Blueprint for admin authentication routes
admin_auth_bp = Blueprint('admin_auth', __name__, template_folder='templates')
# Note: `template_folder` assumes your login.html will be in `templates/admin_auth/login.html`
# Adjust path if your templates are structured differently, e.g., `templates/admin/auth/login.html`

@admin_auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    # If user is already authenticated and is an admin, redirect them to the admin dashboard
    if current_user.is_authenticated and getattr(current_user, 'is_admin', False):
        return redirect(url_for('admin.index')) # 'admin.index' is the default Flask-Admin dashboard

    form = LoginForm()
    if form.validate_on_submit():
        username_to_check = form.username.data
        password_to_check = form.password.data
        remember_me = form.remember.data

        user_doc, error_msg = find_user_by_username(username_to_check)

        if error_msg:
            flash(f"Database error: {error_msg}", "danger")
            return render_template('admin_auth/login.html', title='Admin Login', form=form)

        if user_doc:
            # Instantiate the User object to use its methods
            user_obj = User(user_doc) # Create User instance
            if user_obj.check_password(password_to_check):
                if user_obj.is_admin: # Check if the user has the admin role
                    login_user(user_obj, remember=remember_me)
                    flash('Login successful!', 'success')
                    # Redirect to the page they were trying to access, or admin dashboard
                    next_page = request.args.get('next')
                    return redirect(next_page or url_for('admin.index')) # 'admin.index' is flask-admin's main page
                else:
                    flash('Access Denied: You do not have admin privileges.', 'warning')
            else:
                flash('Login Unsuccessful. Please check username and password.', 'danger')
        else:
            flash('Login Unsuccessful. User not found.', 'danger')

    return render_template('admin_auth/login.html', title='Admin Login', form=form)


@admin_auth_bp.route('/logout')
def logout():
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('admin_auth.login')) # Redirect to login page after logout