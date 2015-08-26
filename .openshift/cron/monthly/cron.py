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
import urlparse
import time
from tvlistings.util import build_url
from config import *
from pymongo import MongoClient
from tvlistings.volley import Volley

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[tvlistings.constants.TV_CHANNELS_COLLECTION]

volley = Volley(thread_pool=10)


def channels_cb(r, *args, **kwargs):
    if r.status_code == 200:
        o = urlparse.urlparse(r.url)
        query_params = urlparse.parse_qs(o.query)

        for channel in r.text.split(','):
            if not channels_collection.find_one({'_id': channel}):
                channels_collection.insert_one({
                    '_id': channel,
                    'name': channel,
                    'type': query_params.get(tvlistings.constants.QUERY_PARAM_LANGUAGE_NAME),
                    'category': query_params.get(tvlistings.constants.QUERY_PARAM_GENRE_NAME)
                })


def main():
    start = time.time()
    # populate queue with data
    for category in tvlistings.constants.TV_LISTINGS_CATEGORY:
        for lang in tvlistings.constants.TV_LISTING_LANGUAGES:
            payload = {
                tvlistings.constants.QUERY_PARAM_GENRE_NAME: category,
                tvlistings.constants.QUERY_PARAM_USER_ID: '0',  # default is 0
                tvlistings.constants.QUERY_PARAM_LANGUAGE_NAME: lang
            }
            url = build_url('http', tvlistings.constants.TIMES_LISTING_API,
                            tvlistings.constants.TIMES_CHANNEL_LIST_ENDPOINT, None)

            volley.get(url, payload, channels_cb)

    volley.join()
    print "Elapsed Time: %s" % (time.time() - start)


if __name__ == '__main__':
    main()
