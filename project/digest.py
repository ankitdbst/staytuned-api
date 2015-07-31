from project import app, db
from xml.dom.minidom import parse

TAG_CHANNEL = 'channel'


def mc2xml_ingest():
    dom = parse(app.config['BASE_DIR'] + '/bin/xmltv.xml')
    channels = dom.getElementsByTagName(TAG_CHANNEL)

    for channel in channels:
        print channel

    print 'hello'


