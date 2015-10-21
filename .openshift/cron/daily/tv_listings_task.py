#!/usr/bin/env python
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

from gevent import monkey
monkey.patch_all()

import grequests
# import Queue
from multiprocessing import Queue, Process, JoinableQueue

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[tvlistings.constants.TV_CHANNELS_COLLECTION]
listings_collection = db[tvlistings.constants.TV_LISTINGS_COLLECTION]


# retrieve next 6 days data from API
LISTINGS_SCHEDULE_DURATION = 7
BATCH_SIZE = 25
POOL_SIZE = 15

providers = {
    'times': 0
}

# volley = Volley(thread_pool=25, providers=providers)


total = 0
start_date = datetime.utcnow()

lock = threading.Lock()

requests = []
to_process_mq = JoinableQueue()

# def imdb_request_cb(r, *args, **kwargs):
#     if r.status_code == 200:
#         try:
#             data = r.json()
#         except ValueError, e:
#             time.sleep(5)
#             volley.get(r.url, None, listing_request_cb, providers['omdb'])
#             print 'error: ', e
#             return
#
#         response = data.get('Response', None)
#         if response == 'True':
#             o = urlparse.urlparse(r.url)
#             query_params = urlparse.parse_qs(o.query)
#             programme_id = query_params.get(tvlistings.constants.IMDB_QUERY_PID)[0]
#
#             del data['Response']
#             listings_collection.update(
#                 {'_id': programme_id},
#                 {'$set': {'imdb': data}}
#             )
#             # print 'updated response for: ' + programme_id
#         # else:
#         #     print r.url
#
#
# def fetch_request_imdb(title, pid, id=None):
#     if not title and not id:
#         return
#
#     url = build_url('http', tvlistings.constants.IMDB_API)
#     payload = {
#         tvlistings.constants.IMDB_QUERY_PLOT_TYPE: 'short',
#         tvlistings.constants.IMDB_QUERY_RETURN_TYPE: 'json',
#         tvlistings.constants.IMDB_QUERY_PID: pid
#     }
#
#     if id:
#         payload[tvlistings.constants.IMDB_QUERY_BY_ID] = id
#     else:
#         payload[tvlistings.constants.IMDB_QUERY_BY_TITLE] = title
#
#     volley.get(url, payload, imdb_request_cb, providers['omdb'])
#
#
# def desc_request_cb(r, *args, **kwargs):
#     if r.status_code == 200:
#         data = r.text
#         soup = BeautifulSoup(data, "html.parser")
#
#         synopsis = 'There is no synopsis available for this episode.'
#         user_rating = 'NA'
#
#         try:
#             synopsis = soup.find('div', class_='content').find_next('p').text
#             user_rating = soup.find('span', class_='avgusrrate').text
#         except AttributeError, e:
#             print 'Error: ' + str(e)
#             return
#
#         o = urlparse.urlparse(r.url)
#         query_params = urlparse.parse_qs(o.query)
#         programme_id = query_params.get(tvlistings.constants.IMDB_QUERY_PID)[0]
#
#         listings_collection.update(
#             {'_id': programme_id},
#             {'$set': {
#                 'synopsis': synopsis,
#                 'user_rating': user_rating
#             }}
#         )
#
#
# def fetch_request_desc(programme):
#     slug = get_slug(programme.get('title'))
#     programme_id = tvlistings.constants.TIMES_DESC_QUERY_PROGRAMME_ID + programme.get('programmeid')
#     channel_id = tvlistings.constants.TIMES_DESC_QUERY_CHANNEL_ID + programme.get('channelid')
#     start_time = tvlistings.constants.TIMES_DESC_QUERY_START_TIME + programme.get('start')
#
#     url = build_url('http', tvlistings.constants.TIMES_LISTING_API)
#     url += 'tv/programmes/' + slug + '/params/tvprogramme/' + programme_id + '/' + channel_id + '/' + start_time
#
#     payload = {
#         tvlistings.constants.IMDB_QUERY_PID: programme.get('_id')
#     }
#
#     volley.get(url, payload, desc_request_cb, providers['times'])


def update_channel_listing(channels):
    if channels is None:
        return

    global total

    for channel in channels:
        programmes = channel.get('programme', None)
        if programmes is not None:
            # lock.acquire()
            # total += len(programmes)
            # lock.release()
            for programme in programmes:
                programme['_id'] = programme['channelid'] + ':' + programme['programmeid'] + ':' + programme['start']
                programme['channel_name'] = channel['display-name']
                programme['title'] = HTMLParser.HTMLParser().unescape(programme['title']).replace('&apos;', "'")
                programme['synopsis'] = 'N/A'
                # try:
                #     lock.acquire()
                # print 'dbwrite:'
                # listings_collection.insert_one(programme)
                # finally:
                #     lock.release()

                # # IMDb info should be fetched only for movies/entertainment and english/hindi
                if channels_collection.find_one({
                    'name': programme['channel_name'],
                    '$or': [
                        {'category': 'movies'},
                        {'category': 'entertainment', 'type': 'english'}
                    ]
                }):
                    programme['imdb_query'] = True
                    # retrieve imdb info
                    # fetch_request_imdb(programme.get('title'), programme.get('_id'))

                if channels_collection.find_one({
                    'name': programme['channel_name'],
                    '$or': [
                        {'category': 'sports'},
                        {'category': 'documentary'}
                    ]
                }):
                    # retrieve info from TIMES about program desc
                    programme['times_query'] = True
                    # fetch_request_desc(programme)

                # time.sleep(0.05)
                # # replace the existing programme with the latest
                listings_collection.update(
                    {'_id': programme['_id']},
                    programme,
                    upsert=True
                )

    print 'Finished task'


def listing_request_cb(r, *args, **kwargs):
    # print 'fetched: ' + r.url
    if r.status_code == 200:
        try:
            data = r.json()
        except ValueError, e:
            time.sleep(5)
            # volley.get(r.url, None, listing_request_cb, providers['times'])
            r = grequests.get(r.url, hooks={'response': listing_request_cb})
            print 'error: ', e
            return

        to_process_mq.put(data)

    # volley.complete()


def fetch_request(channels_param, from_date, to_date):
    # print from_date, to_date
    payload = {
        tvlistings.constants.QUERY_PARAM_CHANNEL_LIST: channels_param,
        tvlistings.constants.QUERY_PARAM_USER_ID: 0,
        tvlistings.constants.QUERY_PARAM_FROM_DATE_TIME: from_date,
        tvlistings.constants.QUERY_PARAM_TO_DATE_TIME: to_date
    }
    url = build_url('http', tvlistings.constants.TIMES_LISTING_API,
                    tvlistings.constants.TIMES_LISTINGS_ENDPOINT, None)

    r = grequests.get(url, params=payload, hooks={'response': listing_request_cb})
    # volley.get(url, payload, listing_request_cb, providers['times'])
    requests.append(r)


def fetch_listings(dt, next_dt):
    from_date = dt.strftime("%Y%m%d0000")
    to_date = next_dt.strftime("%Y%m%d0000")

    # fetch_request('Star Movies', from_date, to_date)

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

    grequests.map(requests, size=POOL_SIZE, stream=True)

def init():
    listings_collection.drop()
    # Set up indexes
    indexes = ('channel_name', 'start', 'stop', 'imdb_query', 'times_query')

    for index in indexes:
        listings_collection.create_index(index)
    # listings_collection.create_index('channel_name')
    # listings_collection.create_index('start')
    # listings_collection.create_index('stop')
    # listings_collection.create_index('imdb_query')
    # listings_collection.create_index('times_query')


class WorkerProcessor(Process):
    def __init__(self, queue):
        super(WorkerProcessor, self).__init__()
        self.queue = queue

    def run(self):
        while True:
            rs = self.queue.get()
            schedule = rs.get('ScheduleGrid', None)
            if schedule is not None:
                channel_listings = schedule.get('channel', None)
                if channel_listings is not None:
                    update_channel_listing(channel_listings)

            self.queue.task_done()


def main():
    init()

    print 'start processing...'
    start = time.time()

    # processes = []
    for i in range(4):
        p = WorkerProcessor(to_process_mq)
        p.daemon = True
        p.start()
        # processes.append(p)

    update_listings()

    to_process_mq.join()
    #
    # for p1 in processes:
    #     p1.join()

    # we wait for volley to complete execution of all requests
    # volley.join()
    # print 'updated listings for: ' + start_date.strftime("%Y-%m-%d")
    # print 'Total: ', total
    print "Elapsed Time: %s" % (time.time() - start)


if __name__ == '__main__':
    main()
