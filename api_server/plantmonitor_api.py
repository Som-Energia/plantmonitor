from flask import Flask, request, Response
from flask_restful import Resource, Api

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from plantmonitor.task import PonyMetricStorage
from yamlns import namespace as ns

app = Flask(__name__)
api = Api(app)

class Meta(Resource):

    def get(self):
        return dict(
            version='1.0',
        )


class PlantMetrics(Resource): 

    def put(self, plant_id):
        data = ns.loads(request.data)
        return Response(
            request.data, 200, mimetype='application/x-yaml'
        )

api.add_resource(PlantMetrics,'/plant/<string:plant_id>')
api.add_resource(Meta,'/version')

if __name__ == '__main__':
    app.run(debug=True)


# vim: et sw=4 ts=4
