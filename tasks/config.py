import os

MONGO_DB_HOST = os.environ.get('OPENSHIFT_MONGODB_DB_HOST', '127.0.0.1')
MONGO_DB_PORT = os.environ.get('OPENSHIFT_MONGODB_DB_PORT', '27017')

MONGO_DB_NAME = os.environ.get('OPENSHIFT_MONGODB_DB_NAME', 'stay')
MONGO_USER_NAME = os.environ.get('OPENSHIFT_MONGODB_USER_NAME', 'admin')
MONGO_USER_PASS = os.environ.get('OPENSHIFT_MONGODB_USER_PASS', 'admin')

MONGO_URI = 'mongodb://' + MONGO_USER_NAME + ':' + MONGO_USER_PASS + '@' \
            + MONGO_DB_HOST + ':' + MONGO_DB_PORT