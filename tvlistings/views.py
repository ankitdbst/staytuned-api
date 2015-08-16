from tvlistings import app, constants, db
from bson.json_util import dumps
from flask import request, Response
import json

channels_collection = db[constants.TV_CHANNELS_COLLECTION]
listings_collection = db[constants.TV_LISTINGS_COLLECTION]


@app.route('/api/channels', methods=['GET'])
def get_channels():
    category = request.args.get('category', '')
    lang = request.args.get('language', '')  # we don't support all the languages

    if category == '' and lang == '':
        data = {
            'error': 'category or language parameter not provided'
        }
        return Response(response=json.dumps(data),
                        status=400,
                        mimetype="application/json")

    cursor = channels_collection.find({'category': category, 'type': lang})
    return Response(response=dumps(cursor),
                    status=200,
                    mimetype="application/json")


@app.route('/api/listings', methods=['GET'])
def get_channel_listings():
    start = request.args.get('startime', '')
    stop = request.args.get('stoptime', '')
    channels = request.args.get('channels', [])

    if channels is not []:
        channels = channels.split(',')

    if start == '' or stop == '':
        data = {
            'error': 'startime or stoptime parameter not provided'
        }
        return Response(response=json.dumps(data),
                        status=400,
                        mimetype="application/json")

    cursor = listings_collection.find({
        'start': {'$lt': stop},
        'stop': {'$gt': start},
        'channel_name': {'$in': channels}
    })

    return Response(response=dumps(cursor),
                    status=200,
                    mimetype="application/json")
