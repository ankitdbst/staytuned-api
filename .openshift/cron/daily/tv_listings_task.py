#!/usr/bin/env python
import sys
import time
from datetime import datetime, timedelta

import HTMLParser
import logging

import tvlistings.constants
from config import *

# utility to build url
from tvlistings.util import build_url
# mongo driver for python
from pymongo import MongoClient

# patch all the libraries from gevent (Socket, Thread, etc)
from gevent import monkey
monkey.patch_all()

# for sending requests using gevent concurrently
import grequests

# we use the multiprocessing module to perform the SQL operation
from multiprocessing import Process, JoinableQueue

# TODO: move this to initialize
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[tvlistings.constants.TV_CHANNELS_COLLECTION]
listings_collection = db[tvlistings.constants.TV_LISTINGS_COLLECTION]

# retrieve next 6 days data from API
# TODO: need to move this to config
LISTINGS_SCHEDULE_DURATION = 7
BATCH_SIZE = 25
POOL_SIZE = 15


start_date = datetime.utcnow()

requests = []
to_process_mq = JoinableQueue()


class WorkerProcessor(Process):
    """
    Worker to process the response for each request sent for retrieving listings
    """
    def __init__(self, queue, processor_fn):
        super(WorkerProcessor, self).__init__()
        self.queue = queue
        self.processor_fn = processor_fn

    def run(self):
        while True:
            rs = self.queue.get()
            self.processor_fn(rs)
            self.queue.task_done()


def process_listing_response(rs):
    schedule = rs.get('ScheduleGrid', None)
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
                programme['_id'] = programme['channelid'] + ':' + programme['programmeid'] + ':' + programme['start']
                programme['channel_name'] = channel['display-name']
                programme['title'] = HTMLParser.HTMLParser().unescape(programme['title']).replace('&apos;', "'")
                programme['synopsis'] = 'N/A'

                # IMDb info should be fetched only for movies/entertainment and english/hindi
                if channels_collection.find_one({
                    'name': programme['channel_name'],
                    '$or': [
                        {'category': 'movies'},
                        {'category': 'entertainment', 'type': 'english'}
                    ]
                }):
                    programme['imdb_query'] = True

                if channels_collection.find_one({
                    'name': programme['channel_name'],
                    '$or': [
                        {'category': 'sports'},
                        {'category': 'documentary'}
                    ]
                }):
                    # retrieve info from TIMES about program desc
                    programme['times_query'] = True

                # replace the existing programme with the latest
                listings_collection.update(
                    {'_id': programme['_id']},
                    programme,
                    upsert=True
                )

    logging.info("Finished processing task.")


def listing_request_cb(r, *args, **kwargs):
    if r.status_code == 200:
        try:
            data = r.json()
        except ValueError, e:
            logging.error("Error %s \n occurred while processing the response for URL: %s" % (r.url, e))
            # Currently we sleep for 5 seconds and do nothing for the failed URL
            time.sleep(5)
            r = grequests.get(r.url, hooks={'response': listing_request_cb})
            return

        to_process_mq.put(data)


def fetch_request(channels_param, from_date, to_date):
    payload = {
        tvlistings.constants.QUERY_PARAM_CHANNEL_LIST: channels_param,
        tvlistings.constants.QUERY_PARAM_USER_ID: 0,
        tvlistings.constants.QUERY_PARAM_FROM_DATE_TIME: from_date,
        tvlistings.constants.QUERY_PARAM_TO_DATE_TIME: to_date
    }
    url = build_url('http', tvlistings.constants.TIMES_LISTING_API,
                    tvlistings.constants.TIMES_LISTINGS_ENDPOINT, None)

    r = grequests.get(url, params=payload, hooks={'response': listing_request_cb})
    requests.append(r)


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

    grequests.map(requests, size=POOL_SIZE, stream=False)


def init():
    # TODO: Currently we drop the db collection during scraping
    # Ideally, we would follow the below process:
    # 1. Rename the current db collection to temp
    # 2. Start the fetch task:
    #   a. Create a new db collection with LISTING_COLLECTION
    #   b. Feed the new listings into this collection
    # 3. API should serve data during this time from temp (while temp exists)
    # 4. Once the fetch task is complete
    #   a. We delete the temp db collection
    listings_collection.drop()
    # Set up indexes
    indexes = ('channel_name', 'start', 'stop', 'imdb_query', 'times_query')

    for index in indexes:
        listings_collection.create_index(index)


if __name__ == '__main__':
    # initialize the scraping module
    init()

    # set log level
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    logging.info("Started at: %s" % time.time())
    start = time.time()

    # we spawn 4 child processes to process the response mq
    for i in range(4):
        p = WorkerProcessor(to_process_mq, process_listing_response)
        p.daemon = True
        p.start()

    # update listings is blocking in the sense that
    # we only reach the next statement once all the
    # urls have received response
    # this doesn't mean that the child processes are
    # not working while the responses are put into the queue
    update_listings()

    to_process_mq.join()
    logging.info("Elapsed Time: %s" % (time.time() - start))
