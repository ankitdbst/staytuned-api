import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

import tvlistings.constants
import requests
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

queue = Queue.Queue()
imdb_queue = Queue.Queue()


class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, q, imdb_q):
        threading.Thread.__init__(self)
        self.queue = q
        self.imdb_queue = imdb_q

    def run(self):
        while True:
            # grabs host from queue
            item = self.queue.get()

            r = requests.get(item['url'], params=item['params'])

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
                        update_channel_listing(channel_listings, self.imdb_queue)

            # signals to queue job is done
            self.queue.task_done()


class DatamineThread(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, q):
        threading.Thread.__init__(self)
        self.imdb_queue = q

    def run(self):
        while True:
            # grabs host from queue
            item = self.imdb_queue.get()

            #parse the chunk
            soup = BeautifulSoup(chunk)
            print soup.findAll(['title'])

            #signals to queue job is done
            self.out_queue.task_done()


def fetch_listing(from_date, to_date, channels_param):
    payload = {
        tvlistings.constants.QUERY_PARAM_CHANNEL_LIST: channels_param,
        tvlistings.constants.QUERY_PARAM_USER_ID: 0,
        tvlistings.constants.QUERY_PARAM_FROM_DATE_TIME: from_date,
        tvlistings.constants.QUERY_PARAM_TO_DATE_TIME: to_date
    }

    url = build_url('http', tvlistings.constants.TIMES_LISTING_API, tvlistings.constants.TIMES_LISTINGS_ENDPOINT, None)

    queue.put({
        'url': url,
        'params': payload
    })


def update_channel_listing(channels, imdb_q):
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

                # place chunk into out queue
                imdb_q.put(programme)


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
    print 'updated listings for: ' + start_date.strftime("%Y-%m-%d")
