# routes/admin/forms.py
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, BooleanField
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