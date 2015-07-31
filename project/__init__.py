from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_restless import APIManager

app = Flask(__name__)
app.config.from_object('config')

db = SQLAlchemy(app)
manager = APIManager(app, flask_sqlalchemy_db=db)

from project import views, models
