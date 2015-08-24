import urllib

URL_SEPARATOR = '/'
URL_COLON = ':'
URL_QUERY_SEPARATOR = '?'
URL_QUERY_PARAM_SEPARATOR = '&'

filterWords = ['and', 'of', 'the', 'an', 'a']
filterSymbols = ['&', ':', ';']


def build_url(scheme, base_url, path='', query_params=None):
    url = scheme + \
        URL_COLON + URL_SEPARATOR + URL_SEPARATOR + \
        base_url + URL_SEPARATOR + path

    if query_params is None:
        return url

    url += URL_QUERY_SEPARATOR

    for key, value in query_params.iteritems():
        url += key + '=' + urllib.urlencode(value) + URL_QUERY_PARAM_SEPARATOR

    return url[:-1]


def cleanse_title(title):
    title_new = ''

    for f in filterSymbols + filterWords:
        for word in title.split(' '):
            if word.lower() != f:
                title_new += word + ' '

    return title[:-1]


def compute_closest_match(title, results):
    min_distance = 1000
    best_match = ''

    for result in results:
        result_title = result.get('title')

        n1 = len(title)
        n2 = len(result_title)

        dp = [[0 for x in range(n2+1)] for x in range(n1+1)]
        for x in range(n1+1):
            dp[x][0] = x

        for x in range(n2+1):
            dp[0][x] = x

        for k in xrange(1, n1+1):
            for j in xrange(1, n2+1):
                dp[k][j] = min(dp[k-1][j]+1, dp[k][j-1]+1,
                               dp[k-1][j-1] + title[k-1] != title[j-1])

        if dp[n1][n2] < min_distance:
            min_distance = dp[n1][n2]
            best_match = result_title

    return best_match
