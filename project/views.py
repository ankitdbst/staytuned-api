from project import app, manager
from project.models import Channel


# APIs
manager.create_api(Channel, methods=['GET'])
