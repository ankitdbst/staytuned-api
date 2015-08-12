import os
basedir = os.path.abspath(os.path.dirname(__file__))

BASE_DIR = basedir
DEBUG = True
PROPAGATE_EXCEPTIONS = True
SECRET_KEY = os.environ.get('SECRET_KEY','\xfb\x13\xdf\xa1@i\xd6>V\xc0\xbf\x8fp\x16#Z\x0b\x81\xeb\x16')
HOST_NAME = os.environ.get('OPENSHIFT_APP_DNS','localhost')
APP_NAME = os.environ.get('OPENSHIFT_APP_NAME','flask')
IP = os.environ.get('OPENSHIFT_PYTHON_IP','127.0.0.1')
PORT = int(os.environ.get('OPENSHIFT_PYTHON_PORT',8080))

MONGO_DB_HOST = os.environ.get('OPENSHIFT_MONGO_DB_HOST', '127.0.0.1')
MONGO_DB_PORT = os.environ.get('OPENSHIFT_MONGO_DB_PORT', '27017')

MONGO_DB_NAME = os.environ.get('OPENSHIFT_MONGO_DB_NAME', 'test')
MONGO_USER_NAME = os.environ.get('OPENSHIFT_MONGO_USER_NAME', 'admin')
MONGO_USER_PASS = os.environ.get('OPENSHIFT_MONGO_USER_PASS', 'admin')
