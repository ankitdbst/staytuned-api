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
import logging

"""
Usual count distribution of programmes:
movies
2695
entertainment
13655
entertainment (hindi)
11096
sports
2964
kids
4671
news
20454
documentary
4531
religious
6995
music
2949
food & lifestyle
2973

Query:
var categories = ["movies", "entertainment", "sports", "kids", "news", "documentary", "religious", "music", "food & lifestyle"];

for (var i = 0; i < categories.length; ++i) {
    var totalCount = 0;
    db.getCollection('tv_listings').group({
       key: { channel_name: 1 },
       cond: { channel_name: {$in: db.getCollection('tv_channels').find({category: categories[i], type: 'hindi'}).map(function(item) { return item._id; })} },
       reduce: function(curr, result) { result.count++; },
       initial: { count: 0 }
    }).forEach(function(item) { totalCount += item.count; });
    print(categories[i]);
    print(totalCount);
}

Order:
sports->documentary->food & lifestyle->entertainment (hindi)->religious->kids->music
"""


client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[tvlistings.constants.TV_CHANNELS_COLLECTION]
listings_collection = db[tvlistings.constants.TV_LISTINGS_COLLECTION]


# retrieve next 6 days data from API
# LISTINGS_SCHEDULE_DURATION = 6
SYNOPSIS_BATCH_SIZE = 100

providers = {
    'times': 0
}

volley = Volley(thread_pool=25, providers=providers)


total = 0
start_date = datetime.utcnow()

lock = threading.Lock()


def synopsis_request_cb(r, *args, **kwargs):
    if r.status_code == 200:
        logging.info("Retrieved response for %s" % r.url)
        data = r.text
        soup = BeautifulSoup(data, "html.parser")

        synopsis = 'There is no synopsis available for this episode.'
        user_rating = 'NA'
        description = 'NA'

        o = urlparse.urlparse(r.url)
        query_params = urlparse.parse_qs(o.query)
        programme_id = query_params.get(tvlistings.constants.IMDB_QUERY_PID)[0]

        try:
            synopsis = soup.find('div', class_='content').find_next('p').text
            user_rating = soup.find('span', class_='avgusrrate').text
            # print synopsis
        except AttributeError, e:
            logging.error("Error encountered %s for url %s" % (str(e), r.url))
            listings_collection.update(
                {'_id': programme_id},
                {'$set': {
                    'times_query': False
                }}
            )
            return
            # print r.url

        try:
            programme_time = r.url[-4:]  # time of programme
            for day in soup.find_all('span', class_='epicday'):
                time_12 = day.find_next('span').text.strip().split(', ')[0].split(' - ')[0].split(' ')
                delta = 0
                if time_12[1] == 'PM':
                    delta = 12
                time_arr = time_12[0].split(':')
                time_24 = str(int(time_arr[0]) + delta) + time_arr[1]
                if time_24 == programme_time:
                    description = day.find_next('div', class_='accordionContent').text
                    # print r.url
                    logging.info("Found description %s for %s" % (description, r.url))
                    break
        except:
            # nothing to do, no description available
            pass

        listings_collection.update(
            {'_id': programme_id},
            {'$set': {
                'times_query': False,
                'times': {
                    'synopsis': synopsis,
                    'description': description,
                    'user_rating': user_rating
                }
            }}
        )
        # print programme_id


def fetch_request_synopsis(programme):
    slug = get_slug(programme.get('title'))
    programme_id = tvlistings.constants.TIMES_DESC_QUERY_PROGRAMME_ID + programme.get('programmeid')
    channel_id = tvlistings.constants.TIMES_DESC_QUERY_CHANNEL_ID + programme.get('channelid')
    start_time = tvlistings.constants.TIMES_DESC_QUERY_START_TIME + programme.get('start')

    url = build_url('http', tvlistings.constants.TIMES_LISTING_API)
    url += 'tv/programmes/params/tvprogramme/' + programme_id + '/' + channel_id + '/' + start_time

    payload = {
        tvlistings.constants.IMDB_QUERY_PID: programme.get('_id')
    }

    logging.info("Added URL %s to queue" % url)
    volley.get(url, payload, synopsis_request_cb, providers['times'])


def update_programmes():
    cursor = listings_collection.find({
        'times_query': True
    }).sort([
        ('startime', ASCENDING),
    ]).limit(SYNOPSIS_BATCH_SIZE)

# db.getCollection('tv_listings').find({imdb: {$exists: true}}).count()
    for programme in cursor:
        fetch_request_synopsis(programme)


def main():
    print 'start processing...'
    logging.basicConfig(level=logging.ERROR, stream=sys.stderr)
    start = time.time()
    update_programmes()
    # we wait for volley to complete execution of all requests
    volley.join()
    # print 'updated listings for: ' + start_date.strftime("%Y-%m-%d")
    # print 'Total: ', total
    print "Elapsed Time: %s" % (time.time() - start)


if __name__ == '__main__':
    main()
