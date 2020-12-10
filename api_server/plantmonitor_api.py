from fastapi import FastAPI, Request, Response
from pydantic import BaseModel

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

from typing import (
    Dict, List, Optional, Union
)

#TODO find a way to not use Union because of time
class Device(BaseModel):
    id: str
    reading: Dict[str, Union[int, datetime.datetime]]

class PlantReading(BaseModel):
    plant: str
    version: str
    time: datetime.datetime
    devices: List[Device]

api = FastAPI()

@api.get('/version')
def api_version():
    return dict(
        version='1.0',
    )


@api.put('/plant/{plant_id}/readings')
async def api_putPlantReadings(plant_id: str, plant_reading: PlantReading):
    print("Putting plant data into plant {} : {}".format(plant_id, plant_reading))
    logger.info("Putting plant data into plant {} : {}".format(plant_id, plant_reading))
    with orm.db_session:
        storage = PonyMetricStorage()
        for d in plant_reading.devices:
            device_type, device_name = d.id.split(':')
            device_reading = d.reading
            device_reading.setdefault('time', plant_reading.time)
            if device_type == "Inverter":
                storage.storeInverterMeasures(
                    plant_name=plant_id,
                    inverter_name=device_name,
                    metrics=device_reading,
                    )
            else:
                logger.error("Unsupported device {}".format(device_type))
    return plant_reading


# vim: et sw=4 ts=4
