# routes/admin/forms.py
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, BooleanField
from wtforms.validators import DataRequired, Email, Optional, Length, Regexp

class EditUserForm(FlaskForm):
    username = StringField('Username', validators=[
        DataRequired(),
        Length(min=3, max=25),
        Regexp('^[A-Za-z0-9_.]*$', 0,
               'Usernames must have only letters, numbers, dots or underscores')
    ])
    email = StringField('Email', validators=[DataRequired(), Email()])
    #is_admin = BooleanField('Is Admin?') 
    submit = SubmitField('Save Changes')
    
class LoginForm(FlaskForm):
    # You can choose to login with username or email
    username = StringField('Username', validators=[DataRequired(), Length(min=3, max=50)])
    # email = StringField('Email', validators=[DataRequired(), Email()]) # Uncomment if using email
    password = PasswordField('Password', validators=[DataRequired()])
    remember = BooleanField('Remember Me')
    submit = SubmitField('Login')