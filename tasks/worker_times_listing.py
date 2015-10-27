import sys
import time
from datetime import datetime, timedelta

import HTMLParser
import logging

from constants import *
from util import build_url
from worker_http import WorkerHTTP
from app import channels_collection, listings_collection


class WorkerHTTPTimesListings(WorkerHTTP):
    """
    Worker to send HTTP requests to Imdb for retrieving Movie/TVSeries info
    """
    def process_response(self, rs):
        """
        Processor function to process response for each HTTP request
        :param rs: Object put in the queue during process_request
        :return: None
        """
        schedule = rs.get('ScheduleGrid', None)
        if schedule is not None:
            channel_listings = schedule.get('channel', None)
            if channel_listings is not None:
                self.update_channel_listing(channel_listings)

    @staticmethod
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

                    # retrieve info from TIMES about program desc
                    if channels_collection.find_one({
                        'name': programme['channel_name'],
                        '$or': [
                            {'category': 'sports'},
                            {'category': 'documentary'}
                        ]
                    }):
                        programme['times_query'] = True

                    # replace the existing programme with the latest
                    listings_collection.update(
                        {'_id': programme['_id']},
                        programme,
                        upsert=True
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
                self.put_request(r.url, payload=None)
                return

            self.put_response(data)

    def fetch_request(self, channels_param, from_date, to_date):
        """
        Create a request for querying TIMES API
        :param channels_param: List of channels to query, comma separated
        :param from_date: From date
        :param to_date: To date
        :return: None
        """
        payload = {
            QUERY_PARAM_CHANNEL_LIST: channels_param,
            QUERY_PARAM_USER_ID: 0,
            QUERY_PARAM_FROM_DATE_TIME: from_date,
            QUERY_PARAM_TO_DATE_TIME: to_date
        }
        url = build_url('http', TIMES_LISTING_API, TIMES_LISTINGS_ENDPOINT, None)

        self.put_request(url, payload=payload)

    def fetch_listings(self, dt, next_dt):
        """
        Look for updated listings on TIMES
        :param dt: From date
        :param next_dt: To date
        :return: None
        """
        from_date = dt.strftime("%Y%m%d0000")
        to_date = next_dt.strftime("%Y%m%d0000")

        ctr = 0
        channels_param = ''
        for channel in channels_collection.find():
            ctr += 1
            channels_param += channel.get('_id') + ','  # replace by name
            if ctr == BATCH_SIZE:
                self.fetch_request(channels_param, from_date, to_date)
                channels_param = ''
                ctr = 0

        if ctr > 0:
            self.fetch_request(channels_param, from_date, to_date)

    def prepare(self):
        """
        Prepare the requests to be sent by the HTTP Worker to OMDb
        :return: None
        """
        start_date = datetime.utcnow()
        dt = start_date
        while (dt - start_date).days < LISTINGS_SCHEDULE_DURATION+1:
            next_dt = dt + timedelta(days=1)
            self.fetch_listings(dt, next_dt)
            dt = next_dt


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


def main():
    # initialize the scraping module
    init()

    # set log level
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    logging.info("Started at: %s" % datetime.utcnow().strftime("%Y%m%d0000"))
    start = time.time()

    worker = WorkerHTTPTimesListings()
    worker.start()

    logging.info("Elapsed Time: %s" % (time.time() - start))


if __name__ == '__main__':
    main()