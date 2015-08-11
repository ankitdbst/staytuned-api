import constants
import requests
from util import build_url


def fetch():
    payload = {
        constants.QUERY_PARAM_GENRE_NAME: 'all',
        constants.QUERY_PARAM_USER_ID: '0'  # default is 0
    }
    url = build_url('http', constants.TIMES_LISTING_API, constants.TIMES_CHANNEL_LIST_ENDPOINT)

    requests.get(url, params=payload)