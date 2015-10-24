from flask import Flask
from pymongo import MongoClient
from tvlistings.constants import *


app = Flask(__name__)
app.config.from_object('config')

client = MongoClient(app.config['MONGO_URI'])
db = client[app.config['MONGO_DB_NAME']]

channels_collection = db[TV_CHANNELS_COLLECTION]
listings_collection = db[TV_LISTINGS_COLLECTION]

from tvlistings import views
