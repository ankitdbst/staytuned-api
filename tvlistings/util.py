import urllib

URL_SEPARATOR = '/'
URL_COLON = ':'
URL_QUERY_SEPARATOR = '?'
URL_QUERY_PARAM_SEPARATOR = '&'


def build_url(scheme, base_url, path, query_params):
    url = scheme + \
        URL_COLON + URL_SEPARATOR + URL_SEPARATOR + \
        base_url + URL_SEPARATOR + path

    if query_params:
        url += URL_QUERY_SEPARATOR

    for key, value in query_params.iteritems():
        url += key + '=' + urllib.urlencode(value) + URL_QUERY_PARAM_SEPARATOR

    return url[:-1]
