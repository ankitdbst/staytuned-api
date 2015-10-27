import sys
import time
from datetime import datetime

import urlparse
import logging

from constants import *
from util import build_url
from worker_http import WorkerHTTP
from app import channels_collection


class WorkerHTTPTimesChannels(WorkerHTTP):
    """
    Worker to send HTTP requests to Imdb for retrieving Movie/TVSeries info
    """
    def process_response(self, item):
        r = item.get('request')
        data = item.get('response')

        o = urlparse.urlparse(r.url)
        query_params = urlparse.parse_qs(o.query)

        for channel in data.split(','):
            category = query_params.get(QUERY_PARAM_GENRE_NAME)
            lang = query_params.get(QUERY_PARAM_LANGUAGE_NAME)

            channel_doc = channels_collection.find_one({'_id': channel})
            if channel_doc:
                channels_collection.update_one(
                    {'_id': channel},
                    {
                        '$set': {
                            'category': channel_doc.get('category') + category
                        }
                    }
                )
            else:
                channels_collection.insert_one({
                    '_id': channel,
                    'name': channel,
                    'type': lang,
                    'category': category
                })

    def process_request(self, r, *args, **kwargs):
        if r.status_code == 200:
            self.put_response({
                'request': r,
                'response': r.text
            })

    def prepare(self):
        # populate queue with data
        for category in TV_LISTINGS_CATEGORY:
            for lang in TV_LISTING_LANGUAGES:
                payload = {
                    QUERY_PARAM_GENRE_NAME: category,
                    QUERY_PARAM_USER_ID: '0',  # default is 0
                    QUERY_PARAM_LANGUAGE_NAME: lang
                }
                url = build_url('http', TIMES_LISTING_API,
                                TIMES_CHANNEL_LIST_ENDPOINT, None)

                self.put_request(url, payload=payload)

    
def init():
    channels_collection.drop()


def main():
    # set log level
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    init()
    logging.info("Started at: %s" % datetime.utcnow().strftime("%Y%m%d0000"))
    start = time.time()

    worker = WorkerHTTPTimesChannels(sleep_interval=0)
    worker.start()

    logging.info("Elapsed Time: %s" % (time.time() - start))


if __name__ == '__main__':
    main()
