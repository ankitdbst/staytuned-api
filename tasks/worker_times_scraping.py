import sys
import time
from datetime import datetime

from pymongo import ASCENDING

import urlparse
import logging

from bs4 import BeautifulSoup

from constants import *
from util import build_url
from worker_http import WorkerHTTP
from app import listings_collection

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

# set log level
logging.basicConfig(level=logging.INFO, stream=sys.stderr)


class WorkerHTTPTimesScraping(WorkerHTTP):
    """
    Worker to send HTTP requests to TIMES for retrieving programme info
    """
    def process_response(self, item):
        """
        Processor function to process response for each HTTP request
        :param rs: Object put in the queue during process_request
        :return: None
        """
        r = item.get('request')
        data = item.get('response')
        soup = BeautifulSoup(data, "html.parser")

        synopsis = 'There is no synopsis available for this episode.'
        user_rating = 'NA'
        description = 'NA'

        o = urlparse.urlparse(r.url)
        query_params = urlparse.parse_qs(o.query)
        programme_id = query_params.get(QUERY_PID)[0]

        try:
            synopsis = soup.find('div', class_='content').find_next('p').text
            user_rating = soup.find('span', class_='avgusrrate').text
        except AttributeError, e:
            logging.error("Error encountered %s for url %s" % (e, r.url))
            # no retries here
            listings_collection.update(
                {'_id': programme_id},
                {'$set': {
                    'times_query': False
                }}
            )
            return

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
        logging.info("Finished processing task.")

    def process_request(self, r, *args, **kwargs):
        if r.status_code == 200:
            self.put_response({
                'request': r,
                'response': r.text
            })

            # No retry here

    def request_synopsis(self, programme):
        programme_id = TIMES_DESC_QUERY_PROGRAMME_ID + programme.get('programmeid')
        channel_id = TIMES_DESC_QUERY_CHANNEL_ID + programme.get('channelid')
        start_time = TIMES_DESC_QUERY_START_TIME + programme.get('start')

        path = 'tv/programmes/params/tvprogramme/' + programme_id + '/' + channel_id + '/' + start_time
        payload = {
            QUERY_PID: programme.get('_id')
        }

        url = build_url('http', TIMES_LISTING_API, path)
        self.put_request(url, payload=payload)

    def prepare(self):
        cursor = listings_collection.find({
            'times_query': True
        }).sort([
            ('startime', ASCENDING),
        ])

        for programme in cursor:
            self.request_synopsis(programme)


def main():
    # set log level
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    logging.info("Started at: %s" % datetime.utcnow().strftime("%Y%m%d0000"))
    start = time.time()

    worker = WorkerHTTPTimesScraping()
    worker.start()

    logging.info("Elapsed Time: %s" % (time.time() - start))


if __name__ == '__main__':
    main()
