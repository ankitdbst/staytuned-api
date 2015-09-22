#!/usr/bin/env python

import os
import sys

try:
    virtenv = os.path.join(os.environ.get('OPENSHIFT_PYTHON_DIR','.'), 'virtenv')
    python_version = "python"+str(sys.version_info[0])+"."+str(sys.version_info[1])
    os.environ['PYTHON_EGG_CACHE'] = os.path.join(virtenv, 'lib', python_version, 'site-packages')
    virtualenv = os.path.join(virtenv, 'bin','activate_this.py')
    if sys.version_info[0] < 3:
        execfile(virtualenv, dict(__file__=virtualenv))
    else:
        exec(open(virtualenv).read(), dict(__file__=virtualenv))

except IOError:
    pass

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

import tvlistings.constants
import time

from datetime import datetime, timedelta
from tvlistings.util import build_url, get_slug
from config import *
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
# from bson.objectid import ObjectId

from tvlistings.volley import Volley
import urlparse
import HTMLParser
from bs4 import BeautifulSoup
import threading
import pprint

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[tvlistings.constants.TV_CHANNELS_COLLECTION]
listings_collection = db[tvlistings.constants.TV_LISTINGS_COLLECTION]


# retrieve next 6 days data from API
# LISTINGS_SCHEDULE_DURATION = 6
IMDB_BATCH_SIZE = 400

providers = {
    'omdb': 0
}

volley = Volley(thread_pool=25, providers=providers)


total = 0
start_date = datetime.utcnow()

lock = threading.Lock()


def imdb_request_cb(r, *args, **kwargs):
    if r.status_code == 200:
        try:
            data = r.json()
        except ValueError, e:
            time.sleep(5)
            volley.get(r.url, None, fetch_request_imdb, providers['omdb'])
            print 'error: ', e
            return

        response = data.get('Response', None)
        if response == 'True':
            o = urlparse.urlparse(r.url)
            query_params = urlparse.parse_qs(o.query)
            programme_id = query_params.get(tvlistings.constants.IMDB_QUERY_PID)[0]

            del data['Response']
            listings_collection.update(
                {'_id': programme_id},
                {'$set': {'imdb': data, 'imdb_query': False}}
            )
            # print 'updated response for: ' + programme_id
        # else:
        #     print r.url


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

    volley.get(url, payload, imdb_request_cb, providers['omdb'])


def update_programmes():
    cursor = listings_collection.find({
        'imdb_query': True
    }).sort([
        ('startime', ASCENDING),
    ]).limit(IMDB_BATCH_SIZE)

# db.getCollection('tv_listings').find({imdb: {$exists: true}}).count()
    for programme in cursor:
        fetch_request_imdb(programme.get('title'), programme.get('_id'))


def main():
    print 'start processing...'
    start = time.time()
    update_programmes()
    # we wait for volley to complete execution of all requests
    volley.join()
    # print 'updated listings for: ' + start_date.strftime("%Y-%m-%d")
    # print 'Total: ', total
    print "Elapsed Time: %s" % (time.time() - start)


if __name__ == '__main__':
    main()
