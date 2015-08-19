import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

import tvlistings.constants
import requests
import time
from tvlistings.util import build_url
from config import *
from pymongo import MongoClient

import Queue
import threading

queue = Queue.Queue()

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]

channels_collection = db[tvlistings.constants.TV_CHANNELS_COLLECTION]


class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, q):
        threading.Thread.__init__(self)
        self.queue = q

    def run(self):
        while True:
            # grabs host from queue
            item = self.queue.get()

            r = requests.get(item['url'], params=item['params'])
            if r.status_code == 200:
                for channel in r.text.split(','):
                    if not channels_collection.find_one({'_id': channel}):
                        channels_collection.insert_one({
                            '_id': channel,
                            'name': channel,
                            'type': lang,
                            'category': category
                        })

            # signals to queue job is done
            self.queue.task_done()


start = time.time()
if __name__ == '__main__':
    # spawn a pool of threads, and pass them queue instance
    for i in range(5):
        t = ThreadUrl(queue)
        t.setDaemon(True)
        t.start()

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

                queue.put({
                    'url': url,
                    'params': payload
                })

    # wait on the queue until everything has been processed
    queue.join()
    print 'updated channel list'
    print "Elapsed Time: %s" % (time.time() - start)
