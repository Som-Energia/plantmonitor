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

  def getDeviceReadings(self, meterName, lastDate):
    measures = measures_from_date(
      self.client,
      meterName,
      beyond=lastDate.strftime("%Y-%m-%d %H:%M:%S"),
      upto=datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    )

    device = {
        "id": "Meter:{}".format(meterName),
        "readings": erp_meter_readings_to_plant_data(measures),
    }

    logger.debug("Retrieved {} measures for meter {} older than {} from erp to ponyorm".format(len(measures), meterName, lastDate))

    return device

  def getPlantNewMetersReadings(self, plant):

    return {
      "plant": plant['plant'],
      "devices": [self.getDeviceReadings(
          meterdate['id'].split(':')[1], meterdate['time'])
          for meterdate in plant['devices']
      ],
    }


  def getNewMetersReadings(self):

    return [self.getPlantNewMetersReadings(plant)
      for plant in Meter.getLastReadingDatesOfAllMeters()
    ]


  def transfer_ERP_readings_to_model(self):

    plants_data = self.getNewMetersReadings()

    Plant.insertPlantsData(plants_data)
