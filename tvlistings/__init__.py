from flask import Flask
from pymongo import MongoClient


app = Flask(__name__)
app.config.from_object('config')

client = MongoClient('localhost', 27017)
db = client['staytuned']

from tvlistings import views
