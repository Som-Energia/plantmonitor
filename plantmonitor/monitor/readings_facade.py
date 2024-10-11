# -*- coding: utf-8 -*-
from erppeek import Client
from plantmonitor.monitor.meters import (
    telemeasure_meter_names,
    measures_from_date,
    meter_connection_protocol,
)

import datetime
import conf.config as config

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from pony import orm

import datetime

from plantmonitor.monitor.standardization import erp_meter_readings_to_plant_data

class ReadingsFacade():
  def __init__(self, db):
    self.client = Client(**config.erppeek)
    self.erpMeters = []
    self.db = db

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
      for plant in self.db.Plant.select().order_by(self.db.Plant.name)]

  def ormMeters(self):
    return list(orm.select(m.name for m in self.db.Meter))

  def checkNewMeters(self, meterNames):
    return [m for m in meterNames if m not in self.ormMeters()]

  def telemeasureMetersNames(self):
    return telemeasure_meter_names(self.client)

  def measuresFromDate(self, meterName, beyond, upto):
    naivebeyond = beyond.astimezone(tz=datetime.timezone.utc).replace(tzinfo=None) if beyond else None
    naiveupto = upto.astimezone(tz=datetime.timezone.utc).replace(tzinfo=None)

    return measures_from_date(
      self.client,
      meterName,
      beyond=naivebeyond.strftime("%Y-%m-%d %H:%M:%S") if naivebeyond else None,
      upto=naiveupto.strftime("%Y-%m-%d %H:%M:%S")
    )

  def refreshERPmeters(self):
    self.erpMeters = self.telemeasureMetersNames()

  def warnNewMeters(self):
    newMeters = self.checkNewMeters(self.erpMeters)

    if newMeters:
      logger.error("New meters in ERP unknown to ORM. Please add them: {}".format(newMeters))

    return newMeters

  # TODO: Unit test me
  def updateMetersProtocols(self):
    meters_protocols = meter_connection_protocol(self.client, self.ormMeters())
    self.db.Meter.updateMeterProtocol(meters_protocols)

  def transfer_ERP_readings_to_model(self, refreshERPmeters=True):
    with orm.db_session:

      if refreshERPmeters:
        self.refreshERPmeters()

      self.warnNewMeters()

      self.updateMetersProtocols()

      plants_data = self.getNewMetersReadings()

      self.db.Plant.insertPlantsData(plants_data)

# vim: et sw=2 ts=2
