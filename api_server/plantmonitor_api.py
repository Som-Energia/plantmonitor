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

from ORM.pony_manager import PonyManager
from plantmonitor.task import PonyMetricStorage
from yamlns import namespace as ns

import datetime

from typing import (
    Dict, List, Optional, Union
)

#TODO find a way to not use Union because of time
class Device(BaseModel):
    id: str
    readings: List[Dict[str, Union[int, datetime.datetime, None]]]

class PlantReading(BaseModel):
    plant: str
    version: str
    time: datetime.datetime
    devices: List[Device]

api = FastAPI()

from conf import envinfo
pony_manager = PonyManager(envinfo.DB_CONF)
pony_manager.define_all_models()
pony_manager.binddb()
storage = PonyMetricStorage(pony_manager.db)

@api.get('/version')
def api_version():
    return dict(
        version='1.0',
    )


@api.put('/plant/{plant_id}/readings')
async def api_putPlantReadings(plant_id: str, plant_reading: PlantReading):
    logger.info("Putting plant data into plant {} : {}".format(plant_id, plant_reading))
    # TODO add db_session(retry=2) once the server migration is done and we're stable
    # or something else
    # to be robust against db restarts (which provoke pony.orm.dbapiprovider.OperationalError: terminating connection due to administrator command )
    with orm.db_session:
        storage.insertPlantData(plant_reading.dict())
    return plant_reading


# vim: et sw=4 ts=4
