from flask import Flask
from pymongo import MongoClient


app = Flask(__name__)
app.config.from_object('config')

client = MongoClient(app.config['MONGO_URI'], app.config['MONGO_DB_PORT'])
db = client[app.config['MONGO_DB_NAME']]

from tvlistings import views
