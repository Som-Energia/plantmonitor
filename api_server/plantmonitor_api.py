from fastapi import FastAPI, Request, Response

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from plantmonitor.task import PonyMetricStorage
from yamlns import namespace as ns

class PlantReading:
    plant: str

api = FastAPI()

@api.get('/version')
def api_version():
    return dict(
        version='1.0',
    )

@api.put('/plant/{plant_id}/readings')
async def api_putPlantReadings(plant_id: str, request: Request): 
    body = await request.body()
    data = ns.loads(body)

    return Response(
        data.dump(), 200, media_type='application/x-yaml'
    )



# vim: et sw=4 ts=4
