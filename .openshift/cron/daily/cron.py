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

import Queue
import threading

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[tvlistings.constants.TV_CHANNELS_COLLECTION]
listings_collection = db[tvlistings.constants.TV_LISTINGS_COLLECTION]


# retrieve next 6 days data from API
LISTINGS_SCHEDULE_DURATION = 6
BATCH_SIZE = 25


start_date = datetime.utcnow()

# Holds the urls for fetching listings for BATCH_SIZE channels
queue = Queue.Queue()
# Processor queue for fetching extra information for each programme.
# using either IMDb APIs or TIMES APIs
processor_queue = Queue.Queue()


class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, q):
        threading.Thread.__init__(self)
        self.queue = q

    def run(self):
        while True:
            # grabs host from queue
            request = self.queue.get()

            r = requests.get(request.get('url'), params=request.get('params'))

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

            # signals to queue job is done
            self.queue.task_done()


class PopulateDataThread(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.processor_queue = queue

    def run(self):
        while True:
            # grabs host from queue
            item = self.processor_queue.get()

            # processing for extra information

            # signals to queue job is done
            self.processor_queue.task_done()


def imdb_query_url(title, id):
    if not title and not id:
        return

    url = build_url('http', tvlistings.constants.IMDB_API)
    payload = {
        tvlistings.constants.IMDB_QUERY_PLOT_TYPE: 'short',
        tvlistings.constants.IMDB_QUERY_RETURN_TYPE: 'json'
    }

    if id:
        payload[tvlistings.constants.IMDB_QUERY_BY_ID] = id
    else:
        payload[tvlistings.constants.IMDB_QUERY_BY_ID] = id

    processor_queue.put({
        'url': url,
        'params': payload
    })


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

                # place chunk into info queue
                if programme.

def update_queue(channels_param, from_date, to_date):
    payload = {
        tvlistings.constants.QUERY_PARAM_CHANNEL_LIST: channels_param,
        tvlistings.constants.QUERY_PARAM_USER_ID: 0,
        tvlistings.constants.QUERY_PARAM_FROM_DATE_TIME: from_date,
        tvlistings.constants.QUERY_PARAM_TO_DATE_TIME: to_date
    }
    url = build_url('http', tvlistings.constants.TIMES_LISTING_API,
                    tvlistings.constants.TIMES_LISTINGS_ENDPOINT, None)
    queue.put({
        'url': url,
        'params': payload
    })


def fetch_listings(dt, next_dt):
    from_date = dt.strftime("%Y%m%d0000")
    to_date = next_dt.strftime("%Y%m%d0000")

    ctr = 0
    channels_param = ''
    for channel in channels_collection.find():
        ctr += 1
        channels_param += channel.get('name') + ','
        if ctr == BATCH_SIZE:
            update_queue(channels_param, from_date, to_date)
            channels_param = ''
            ctr = 0

    if ctr > 0:
        update_queue(channels_param, from_date, to_date)


def update_listings():
    # look for updated listings
    dt = start_date
    while (dt - start_date).days < LISTINGS_SCHEDULE_DURATION+1:
        print 'fetching listings for date: ' + dt.strftime("%Y-%m-%d") + ' delta: ' + str((dt - start_date).days)
        print '--------------------------------'
        next_dt = dt + timedelta(days=1)
        fetch_listings(dt, next_dt)
        dt = next_dt


start = time.time()
if __name__ == '__main__':
    # spawn a pool of threads, and pass them queue instance
    for i in range(6):
        t = ThreadUrl(queue)
        t.setDaemon(True)
        t.start()


    update_listings()

    # # spawn a pool of threads, and pass them queue instance
    # for i in range(5):
    #     t = PopulateDataThread(processor_queue)
    #     t.setDaemon(True)
    #     t.start()

    queue.join()
    # processor_queue.join()

    print 'updated listings for: ' + start_date.strftime("%Y-%m-%d")
    print "Elapsed Time: %s" % (time.time() - start)
