# -*- coding: utf-8 -*-
import requests

from pymodbus.client.sync import ModbusTcpClient
from influxdb import InfluxDBClient
from plantmonitor.resource import ProductionPlant
from yamlns import namespace as ns
from erppeek import Client
from .meters import (
    telemeasure_meter_names,
    measures_from_date,
    upload_measures,
    uploaded_plantmonitor_measures,
    last_uploaded_plantmonitor_measures,
    transfer_meter_to_plantmonitor,
)

from meteologica.daily_upload_to_api import upload_meter_data
from meteologica.daily_download_from_api import download_meter_data

import sys
import psycopg2
import time
import datetime
import conf.config as config

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

import os

from plantmonitor.storage import (
    PonyMetricStorage,
    ApiMetricStorage,
    InfluxMetricStorage,
    TimeScaleMetricStorage,
)

from pony import orm

import datetime

from .standardization import erp_meter_readings_to_plant_data

from ORM.models import database
from ORM.models import (
    Plant,
    Meter,
    MeterRegistry,
    Inverter,
    InverterRegistry,
    Sensor,
    SensorTemperatureAmbient,
    SensorTemperatureModule,
    SensorIrradiationRegistry,
    SensorTemperatureAmbientRegistry,
    SensorTemperatureModuleRegistry,
)

from ORM.orm_util import connectDatabase, getTablesToTimescale, timescaleTables


class ReadingsFacade():
  def __init__(self):
    self.client = Client(**config.erppeek)

  def getDeviceReadings(self, meterName, lastDate=None, upto=datetime.datetime.utcnow()):

    measures = measures_from_date(
      self.client,
      meterName,
      beyond=lastDate.strftime("%Y-%m-%d %H:%M:%S"),
      upto=upto.strftime("%Y-%m-%d %H:%M:%S")
    )

    device = {
        "id": "Meter:{}".format(meterName),
        "readings": erp_meter_readings_to_plant_data(measures),
    }

    logger.debug("Retrieved {} measures for meter {} older than {} from erp to ponyorm".format(len(measures), meterName, lastDate))

    return device

  def getPlantNewMetersReadings(self, plant, upto=datetime.datetime.utcnow()):

    return {
      "plant": plant['plant'],
      "devices": [self.getDeviceReadings(
          meterdate['id'].split(':')[1], meterdate['time'], upto)
          for meterdate in plant['devices']
      ],
    }

  def getNewMetersReadings(self, upto=datetime.datetime.utcnow()):

    return [self.getPlantNewMetersReadings(plant, upto)
      for plant in Meter.getLastReadingDatesOfAllMeters()
    ]

  def checkNewMeters(self, meterNames):
    ormMeters = orm.select(m.name for m in Meter)[:]
    return [m for m in meterNames if m not in ormMeters]

  def transfer_ERP_readings_to_model(self):
      with orm.db_session:
          meter_names = telemeasure_meter_names(self.client)

          newMeters = self.checkNewMeters(meter_names)

          if newMeters:
            logger.error("New meters in ERP unknown to ORM. Please add them: {}".format(newMeters))

          plants_data = self.getNewMetersReadings()

          Plant.insertPlantsData(plants_data)
