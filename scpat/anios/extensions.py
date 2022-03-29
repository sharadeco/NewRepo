from flask_sqlalchemy import SQLAlchemy
from flask_material import Material
from flask_login import LoginManager, login_user
import ray

# from flask_login.mixins import UserMixin
db1 = SQLAlchemy()  
bt = Material()         #the design component
db = SQLAlchemy()       #the database component
login = LoginManager()  #the login component
