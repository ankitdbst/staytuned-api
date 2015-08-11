import json
from tvlistings import app


@app.route('/')
def index():
    return json.dumps({})


@app.route('/cron')
def cron():
    return json.dumps(
        {'a': 'b'}
    )
