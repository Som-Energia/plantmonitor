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
    self.erpMeters = []

  def getNewErpReadings(self, meterName, lastDate=None, upto=None):
    upto = upto or datetime.datetime.now(datetime.timezone.utc)

    logger.debug(
      "Asking for measures for meter {} older than {} upto {} from erp to ponyorm".format(
        meterName,
        None if not lastDate else lastDate.strftime("%Y-%m-%d %H:%M:%S"),
        upto.strftime("%Y-%m-%d %H:%M:%S")
      )
    )

    # TODO we're filtering out the meters that might be in pony
    # but not in ERP at getPlantNewMetersReadings,
    # but maybe we should raise or handle it more clearly here

    measures = self.measuresFromDate(meterName, lastDate, upto)

    logger.debug("Retrieved {} measures for meter {} older than {} from erp to ponyorm".format(len(measures), meterName, lastDate))

    readings = erp_meter_readings_to_plant_data(measures)

    return readings

  # returns plant_data
  def getPlantNewMetersReadings(self, plant, upto=None):
    if not self.erpMeters:
      self.refreshERPmeters()

    upto = upto or datetime.datetime.now(datetime.timezone.utc)
    return {
      "plant": plant.name,
      "devices": [
        {
          "id": "Meter:{}".format(meter.name),
          "readings": self.getNewErpReadings(meter.name, meter.getLastReadingDate(), upto)
        } for meter in plant.meters if meter.name in self.erpMeters
      ]
    }

  # returns plants_data
  def getNewMetersReadings(self, upto=None):
    upto = upto or datetime.datetime.now(datetime.timezone.utc)
    return [self.getPlantNewMetersReadings(plant, upto)
      for plant in Plant.select().order_by(Plant.name)]

  def checkNewMeters(self, meterNames):
    ormMeters = orm.select(m.name for m in Meter)[:]
    return [m for m in meterNames if m not in ormMeters]

  def telemeasureMetersNames(self):
    return telemeasure_meter_names(self.client)

  def measuresFromDate(self, meterName, beyond, upto):
    naivebeyond = beyond.astimezone(tz=datetime.timezone.utc).replace(tzinfo=None)
    naiveupto = upto.astimezone(tz=datetime.timezone.utc).replace(tzinfo=None)

    return measures_from_date(
      self.client,
      meterName,
      beyond=naivebeyond and naivebeyond.strftime("%Y-%m-%d %H:%M:%S"),
      upto=naiveupto.strftime("%Y-%m-%d %H:%M:%S")
    )

  def refreshERPmeters(self):
    self.erpMeters = self.telemeasureMetersNames()

  def warnNewMeters(self):
    newMeters = self.checkNewMeters(self.erpMeters)

    if newMeters:
      logger.error("New meters in ERP unknown to ORM. Please add them: {}".format(newMeters))

    return newMeters

  def transfer_ERP_readings_to_model(self, refreshERPmeters=True):
    with orm.db_session:

      if refreshERPmeters:
        self.refreshERPmeters()

      self.warnNewMeters()

      plants_data = self.getNewMetersReadings()

      Plant.insertPlantsData(plants_data)
