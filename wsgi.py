from __future__ import print_function

import os
import json
import re

from flask import Flask, request
from flask_restful import Resource, Api

from pymongo import MongoClient, GEO2D

DB_URI = os.environ.get('DB_URI')

DB_HOST = os.environ.get('DB_HOST', 'mongodb-nationalparks')
DB_SERVICE_NAME = os.environ.get('DATABASE_SERVICE_NAME')

if os.environ.get('uri'):
	match = re.match("mongodb?:\/\/([^:^/]*):?(\d*)?", os.environ.get('uri'))
    
	if match:
		DB_HOST = match.group(1)	

if DB_SERVICE_NAME:
    DB_HOST = DB_SERVICE_NAME
    
DB_NAME = os.environ.get('DB_NAME', 'mongodb')

DB_USERNAME = os.environ.get('DB_USERNAME', 'mongodb')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'mongodb')

if not DB_URI:
    DB_URI = 'mongodb://%s:%s@%s:27017/%s' % (DB_USERNAME, DB_PASSWORD,
            DB_HOST, DB_NAME)

DATASET_FILE = 'nationalparks.json'

application = Flask(__name__)

api = Api(application)

class HealthCheck(Resource):
    def get(self):
        return 'OK'

api.add_resource(HealthCheck, '/ws/healthz/')

class Info(Resource):
    description = {
        'id': 'nationalparks-py',
        'displayName': 'National Parks (PY)',
        'type': 'cluster',
        'center': {'latitude': '47.039304', 'longitude': '14.505178'},
        'zoom': 4
    }

    def get(self):
        return self.description

api.add_resource(Info, '/ws/info/')

class DataLoad(Resource):
    def get(self):
        client = MongoClient(DB_URI)
        database = client[DB_NAME]
        collection = database.nationalparks

        collection.remove({})
        collection.create_index([('Location', GEO2D)])

        with open(DATASET_FILE, 'r') as fp:
            entries = []

            for data in fp.readlines():
                entry = json.loads(data)

                loc = [entry['coordinates'][1], entry['coordinates'][0]]
                entry['Location'] = loc

                entries.append(entry)

                if len(entries) >= 1000:
                    collection.insert_many(entries)
                    entries = []

            if entries:
                collection.insert_many(entries)

        return 'Items inserted in database: %s' % collection.count()

api.add_resource(DataLoad, '/ws/data/load')

def format_result(entries):
    result = []

    for entry in entries:
        data = {}

        data['id'] = entry['name']
        data['latitude'] = str(entry['coordinates'][0])
        data['longitude'] = str(entry['coordinates'][1])
        data['name'] = entry['toponymName']

        result.append(data)

    return result

class DataAll(Resource):
    def get(self):
        client = MongoClient(DB_URI)
        database = client[DB_NAME]
        collection = database.nationalparks

        return format_result(collection.find())

api.add_resource(DataAll, '/ws/data/all')

class DataWithin(Resource):
    def get(self):
        args = request.args

        box = [[float(args['lon1']), float(args['lat1'])],
               [float(args['lon2']), float(args['lat2'])]]

        query = {'Location': {'$within': {'$box': box}}}

        client = MongoClient(DB_URI)
        database = client[DB_NAME]
        collection = database.nationalparks

        return format_result(collection.find(query))

api.add_resource(DataWithin, '/ws/data/within')

@application.route('/')
def index():
    return 'Welcome to the National Parks data service.'
