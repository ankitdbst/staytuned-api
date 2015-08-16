import os
import constants
import requests
from datetime import datetime, timedelta
from util import build_url
from pymongo import MongoClient

# environ variables for Mongodb
MONGO_DB_HOST = os.environ.get('OPENSHIFT_MONGODB_DB_HOST', '127.0.0.1')
MONGO_DB_PORT = os.environ.get('OPENSHIFT_MONGODB_DB_PORT', 27017)
MONGO_DB_NAME = os.environ.get('OPENSHIFT_MONGODB_DB_NAME', 'staytuned')

MONGO_USER_NAME = os.environ.get('OPENSHIFT_MONGODB_USER_NAME', 'admin')
MONGO_USER_PASS = os.environ.get('OPENSHIFT_MONGODB_USER_PASS', 'admin')

MONGO_URI = 'mongodb://' + MONGO_USER_NAME + ':' + MONGO_USER_PASS + '@' + MONGO_DB_HOST

client = MongoClient(MONGO_URI, MONGO_DB_PORT)
db = client[MONGO_DB_NAME]

channels_collection = db[constants.TV_CHANNELS_COLLECTION]
listings_collection = db[constants.TV_LISTINGS_COLLECTION]


# retrieve next 6 days data from API
LISTINGS_SCHEDULE_DURATION = 6
BATCH_SIZE = 25


start_date = datetime.utcnow()


def fetch_channels(category, lang):
    payload = {
        constants.QUERY_PARAM_GENRE_NAME: category,
        constants.QUERY_PARAM_USER_ID: '0',  # default is 0
        constants.QUERY_PARAM_LANGUAGE_NAME: lang
    }
    url = build_url('http', constants.TIMES_LISTING_API, constants.TIMES_CHANNEL_LIST_ENDPOINT, None)

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


def fetch_imdb_info(programme):
    return True


def fetch_listing(from_date, to_date, channels_param):
    payload = {
        constants.QUERY_PARAM_CHANNEL_LIST: channels_param,
        constants.QUERY_PARAM_USER_ID: 0,
        constants.QUERY_PARAM_FROM_DATE_TIME: from_date,
        constants.QUERY_PARAM_TO_DATE_TIME: to_date
    }

    url = build_url('http', constants.TIMES_LISTING_API, constants.TIMES_LISTINGS_ENDPOINT, None)

    r = requests.get(url, params=payload)

    data = None
    if r.status_code == 200:
        try:
            data = r.json()
        except ValueError:
            # print 'error parsing json data'
            return

        schedule = data.get('ScheduleGrid', None)
        if schedule is not None:
            channel_listings = schedule.get('channel', None)
            if channel_listings is not None:
                update_channel_listing(channel_listings)


def update_channel_listing(channels):
    if channels is None:
        return

    for channel in channels:
        programmes = channel.get('programme', None)
        if programmes is not None:
            for programme in programmes:
                programme['_id'] = programme['programmeid'] + ':' + programme['start']
                programme['channel_name'] = channel['display-name']
                # replace the existing programme with the latest
                listings_collection.replace_one({'_id': programme['_id']}, programme, True)


def fetch_listings(dt, next_dt):
    from_date = dt.strftime("%Y%m%d0000")
    to_date = next_dt.strftime("%Y%m%d0000")

    ctr = 0
    channels_param = ''
    for channel in channels_collection.find():
        ctr += 1
        channels_param += channel.get('name') + ','
        if ctr == BATCH_SIZE:
            # print 'updating channels: ' + channels_param
            fetch_listing(from_date, to_date, channels_param)
            channels_param = ''
            ctr = 0

    if ctr > 0:
        fetch_listing(from_date, to_date, channels_param)


def update_listings():
    # look for new channels
    for category in constants.TV_LISTINGS_CATEGORY:
        for lang in constants.TV_LISTING_LANGUAGES:
            fetch_channels(category, lang)

    # look for updated listings
    dt = start_date
    while (dt - start_date).days < LISTINGS_SCHEDULE_DURATION+1:
        # print 'fetching listings for date: ' + dt.strftime("%Y-%m-%d") + ' delta: ' + str((dt - start_date).days)
        # print '--------------------------------'
        next_dt = dt + timedelta(days=1)
        fetch_listings(dt, next_dt)
        dt = next_dt


if __name__ == '__main__':
    update_listings()
