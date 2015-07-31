from project import app, manager
from project.models import Channel
from project.digest import mc2xml_ingest


# APIs
manager.create_api(Channel, methods=['GET'])


@app.route('/digest')
def digest():
    mc2xml_ingest()
