#!/usr/bin/env python
import sys
import time
from datetime import datetime

import urlparse
import logging

from pymongo import ASCENDING

from tvlistings.constants import *
from tvlistings.util import build_url
from tvlistings.cron.worker_http import WorkerHTTP
from tvlistings import listings_collection


class WorkerHTTPImdb(WorkerHTTP):
    """
    Worker to send HTTP requests to Imdb for retrieving Movie/TVSeries info
    """
    def process_response(self, item):
        """
        Processor function to process response for each HTTP request
        :param item: Object put in the queue during process_request
        :return: None
        """
        r = item.get('request', None)
        if r is None:
            return

        rs = item.get('response', None)
        if rs is None:
            return

        o = urlparse.urlparse(r.url)
        query_params = urlparse.parse_qs(o.query)
        programme_id = query_params.get(QUERY_PID)[0]

        response = rs.get('Response', None)
        if response == 'True':
            del rs['Response']
            listings_collection.update(
                {'_id': programme_id},
                {'$set': {'imdb': rs, 'imdb_query': False}}
            )
        else:
            listings_collection.update(
                {'_id': programme_id},
                {'$set': {'imdb_query': False}}
            )

        logging.info("Finished processing task.")

    def process_request(self, r, *args, **kwargs):
        """
        Processor function to put the objects into the Processor queue for processing the response.
        :param r: HTTP Response object
        :param args:
        :param kwargs:
        :return: None
        """
        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError, e:
                logging.error("Error %s \n occurred while processing the response for URL: %s" % (e, r.url))
                self.put_request(r.url, payload=None, retry=True)
                return

            self.put_response({
                'request': r,
                'response': data
            })

    def request_imdb(self, title, pid, id=None):
        """
        Create a request for querying OMDB API
        :param title: Title of the movie/tv show
        :param pid: Programme id of the listings
        :param id: IMDb ID if available
        :return: None
        """
        if not title and not id:
            return

        url = build_url('http', IMDB_API)
        payload = {
            IMDB_QUERY_PLOT_TYPE: 'short',
            IMDB_QUERY_RETURN_TYPE: 'json',
            QUERY_PID: pid
        }

        if id:
            payload[IMDB_QUERY_BY_ID] = id
        else:
            payload[IMDB_QUERY_BY_TITLE] = title

        self.put_request(url, payload=payload)

    def prepare(self):
        """
        Prepare the requests to be sent by the HTTP Worker to OMDb
        :return: None
        """
        cursor = listings_collection.find({
            'imdb_query': True
        }).sort([
            ('startime', ASCENDING),
        ])

        for programme in cursor:
            self.request_imdb(programme.get('title'), programme.get('_id'))


def main():
    """
    Main
    :return: None
    """
    # set log level
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    logging.info("Started at: %s" % datetime.utcnow().strftime("%Y%m%d0000"))
    start = time.time()

    worker = WorkerHTTPImdb()
    worker.start()

    logging.info("Elapsed Time: %s" % (time.time() - start))

if __name__ == '__main__':
    main()
