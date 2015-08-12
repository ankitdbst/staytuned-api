from tvlistings import app
from cron import update_listings


@app.route('/')
def cron():
    update_listings()
