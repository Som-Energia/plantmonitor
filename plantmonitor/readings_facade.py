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

  def transfer_ERP_readings_to_model(self):

    last_dates = Meter.getLastReadingsDate()

    # build plants_data from last_dates
    plants_data = {}

    # zip o algo
    plants_names = []

    for plant in plants_names:
        for meter_name, last_date in last_dates:
            plants_data[plant_name] = {'plant'}
            measures[meter_name] = measures_from_date(self.client, meter_name, beyond=last_date, upto=datetime.datetime.utcnow())
            logger.debug("Uploading {} measures for meter {} older than {} from erp to ponyorm".format(len(measures), meter_name, last_date))

    # TODO we need to ask the orm for the meter <-> plant correlation
    # otherwise plant.insertPlantData can't be used, or we can use a static/global method
    # that does the insert by searching the meter in Meter regardless of plant

    plant_data = erp_meter_readings_to_plant_data(meter_name, measures)

    for plant in plants:
        plant.insertPlantData(plant_data)
