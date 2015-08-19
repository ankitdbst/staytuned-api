import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

import tvlistings.constants
import requests
from datetime import datetime, timedelta
from tvlistings.util import build_url
from config import *
from pymongo import MongoClient


client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[tvlistings.constants.TV_CHANNELS_COLLECTION]


def fetch_channels(category, lang):
    payload = {
        tvlistings.constants.QUERY_PARAM_GENRE_NAME: category,
        tvlistings.constants.QUERY_PARAM_USER_ID: '0',  # default is 0
        tvlistings.constants.QUERY_PARAM_LANGUAGE_NAME: lang
    }
    url = build_url('http', tvlistings.constants.TIMES_LISTING_API, tvlistings.constants.TIMES_CHANNEL_LIST_ENDPOINT,
                    None)

    r = requests.get(url, params=payload)

    if r.status_code == 200:
        for channel in r.text.split(','):
            if not channels_collection.find_one({'_id': channel}):
                channels_collection.insert_one({
                    '_id': channel,
                    'name': channel,
                    'type': lang,
                    'category': category
                })


def update_channels():
    # look for new channels
    for category in tvlistings.constants.TV_LISTINGS_CATEGORY:
        for lang in tvlistings.constants.TV_LISTING_LANGUAGES:
            fetch_channels(category, lang)


if __name__ == '__main__':
    update_channels()
    print 'updated channel list'
