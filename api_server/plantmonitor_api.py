from fastapi import FastAPI, Request, Response

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')

from conf.logging_configuration import LOGGING
from pony import orm
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from plantmonitor.task import PonyMetricStorage
from yamlns import namespace as ns

import datetime

import typing
class YAMLResponse(Response):
    media_type = "application/x-yaml"

    def render(self, content: typing.Any) -> bytes:
        return ns(content).dump().encode("utf-8")

class PlantReading:
    plant: str

api = FastAPI(
    default_response_class = YAMLResponse,
)

@api.get('/version')
def api_version():
    return dict(
        version='1.0',
    )


@api.put('/plant/{plant_id}/readings')
async def api_putPlantReadings(plant_id: str, request: Request): 
    body = await request.body()
    data = ns.loads(body)
    with orm.db_session:
        storage = PonyMetricStorage()
        inverter_name = 'inversor1' # TODO take it from yaml
        inverter_reading = ns(data.devices[0].reading)
        inverter_reading.setdefault('time', data.time)
        inverter_reading.time = datetime.datetime.fromisoformat(inverter_reading.time)
        print(inverter_reading.time)
        storage.storeInverterMeasures(
            plant_name =plant_id,
            inverter_name = inverter_name,
            metrics = inverter_reading,
            )
    return data


# vim: et sw=4 ts=4
