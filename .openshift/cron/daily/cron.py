import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

import tvlistings.constants
import requests
import time
from datetime import datetime, timedelta
from tvlistings.util import build_url
from config import *
from pymongo import MongoClient

from tvlistings.volley import Volley
import urlparse
import HTMLParser


client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[tvlistings.constants.TV_CHANNELS_COLLECTION]
listings_collection = db[tvlistings.constants.TV_LISTINGS_COLLECTION]


# retrieve next 6 days data from API
LISTINGS_SCHEDULE_DURATION = 1
BATCH_SIZE = 25

volley = Volley(thread_pool=8)

start_date = datetime.utcnow()


def imdb_request_cb(r, *args, **kwargs):
    if r.status_code == 200:
        try:
            data = r.json()
        except ValueError:
            # print 'error parsing json data'
            return

        response = data.get('Response', None)
        if response == 'True':
            o = urlparse.urlparse(r.url)
            query_params = urlparse.parse_qs(o.query)
            programme_id = query_params.get(tvlistings.constants.IMDB_QUERY_PID)[0]

            del data['Response']
            listings_collection.update(
                {'_id': programme_id},
                {'$set': {'imdb': data}}
            )
            # print 'updated response for: ' + programme_id
        else:
            print r.url


def fetch_request_imdb(title, pid, id=None):
    if not title and not id:
        return

    url = build_url('http', tvlistings.constants.IMDB_API)
    payload = {
        tvlistings.constants.IMDB_QUERY_PLOT_TYPE: 'short',
        tvlistings.constants.IMDB_QUERY_RETURN_TYPE: 'json',
        tvlistings.constants.IMDB_QUERY_PID: pid
    }

    if id:
        payload[tvlistings.constants.IMDB_QUERY_BY_ID] = id
    else:
        payload[tvlistings.constants.IMDB_QUERY_BY_TITLE] = title

    volley.get(url, payload, imdb_request_cb)


def update_channel_listing(channels):
    if channels is None:
        return

    for channel in channels:
        programmes = channel.get('programme', None)
        if programmes is not None:
            for programme in programmes:
                programme['_id'] = programme['programmeid'] + ':' + programme['start']
                programme['channel_name'] = channel['display-name']
                programme['title'] = HTMLParser.HTMLParser().unescape(programme['title'])

                # replace the existing programme with the latest
                listings_collection.update(
                    {'_id': programme['_id']},
                    programme,
                    upsert=True
                )

                # IMDb info should be fetched only for movies/entertainment and english/hindi
                if channels_collection.find_one({
                    'name': programme['channel_name'],
                    'category': {'$in': ['entertainment', 'movies']}
                }):
                    # retrieve imdb info
                    fetch_request_imdb(programme.get('title'), programme.get('_id'))


def listing_request_cb(r, *args, **kwargs):
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


def fetch_request(channels_param, from_date, to_date):
    payload = {
        tvlistings.constants.QUERY_PARAM_CHANNEL_LIST: channels_param,
        tvlistings.constants.QUERY_PARAM_USER_ID: 0,
        tvlistings.constants.QUERY_PARAM_FROM_DATE_TIME: from_date,
        tvlistings.constants.QUERY_PARAM_TO_DATE_TIME: to_date
    }
    url = build_url('http', tvlistings.constants.TIMES_LISTING_API,
                    tvlistings.constants.TIMES_LISTINGS_ENDPOINT, None)

    volley.get(url, payload, listing_request_cb)


def fetch_listings(dt, next_dt):
    from_date = dt.strftime("%Y%m%d0000")
    to_date = next_dt.strftime("%Y%m%d0000")

    ctr = 0
    channels_param = ''
    for channel in channels_collection.find():
        ctr += 1
        channels_param += channel.get('_id') + ','  # replace by name
        if ctr == BATCH_SIZE:
            fetch_request(channels_param, from_date, to_date)
            channels_param = ''
            ctr = 0

    if ctr > 0:
        fetch_request(channels_param, from_date, to_date)


def update_listings():
    # look for updated listings
    dt = start_date
    while (dt - start_date).days < LISTINGS_SCHEDULE_DURATION+1:
        next_dt = dt + timedelta(days=1)
        fetch_listings(dt, next_dt)
        dt = next_dt


def main():
    print 'start processing...'
    start = time.time()
    update_listings()
    # we wait for volley to complete execution of all requests
    volley.join()
    print 'updated listings for: ' + start_date.strftime("%Y-%m-%d")
    print "Elapsed Time: %s" % (time.time() - start)


if __name__ == '__main__':
    main()
