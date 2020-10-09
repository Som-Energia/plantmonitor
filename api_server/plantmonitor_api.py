from flask import Flask, request
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

class PlantMetrics(Resource): 

    def put(self, plant_id):
        return {}

    def get(self):
        return {'hello': 'plant'}

api.add_resource(PlantMetrics,'/plant/<string:plant_id>')
# api.add_resource(PlantMetrics,'/')

if __name__ == '__main__':
    app.run(debug=True)