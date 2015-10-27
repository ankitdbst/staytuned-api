import os
import sys

from pymongo import MongoClient
from constants import TV_CHANNELS_COLLECTION, TV_LISTINGS_COLLECTION
from config import MONGO_URI, MONGO_DB_NAME

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[TV_CHANNELS_COLLECTION]
listings_collection = db[TV_LISTINGS_COLLECTION]

