from tvlistings import app, listings_collection, channels_collection
from bson.json_util import dumps
from flask import request, Response
import json


@app.route('/api/channels', methods=['GET'])
def get_channels():
    category = request.args.get('category', '')

    lang = request.args.get('language', '')  # hindi/english

    if category == '' and lang == '':
        data = {
            'error': 'category or language required'
        }
        return Response(response=json.dumps(data),
                        status=400,
                        mimetype="application/json")

    condition = dict()
    if category:
        condition['category'] = category
    if lang:
        condition['type'] = lang

    cursor = channels_collection.find(condition)
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
