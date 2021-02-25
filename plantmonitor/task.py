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

from conf import envinfo


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

from .standardization import registers_to_plant_data
from .readings_facade import ReadingsFacade

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

def client_db(db):
    try:
        logger.info("Connecting to Influxdb")
        flux_client = InfluxDBClient(db['influxdb_ip'],
                                db['influxdb_port'],
                                db['influxdb_user'],
                                db['influxdb_password'],
                                db['influxdb_database'],
                                ssl=db['influxdb_ssl'],
                                verify_ssl=db['influxdb_verify_ssl'])
    except Exception as e:
        logger.warning("Failed to connect to influx client: {}".format(repr(e)))
        flux_client = None

    return flux_client

def publish_timescale(plant_name, inverter_name, metrics, db):
    with psycopg2.connect(
            user=db['user'], password=db['password'],
            host=db['host'], port=db['port'], database=db['database']
        ) as conn:
        with conn.cursor() as cur:
            measurement    = 'sistema_inversor'
            query_content  = ', '.join(metrics['fields'].keys())
            values_content = ', '.join(["'{}'".format(v) for v in metrics['fields'].values()])

            cur.execute(
                "INSERT INTO {}(time, inverter_name, location, {}) \
                VALUES (timezone('utc',NOW()), '{}', '{}', {});".format(
                    measurement,query_content,
                    inverter_name,plant_name,values_content
                )
            )

def task():
    try:

        plant = ProductionPlant()
        plantname = envinfo.ACTIVEPLANT_CONF['activeplant']

        if not plant.load('conf/modmap.yaml', plantname):
            logger.error('Error loading yaml definition file...')
            sys.exit(-1)

        devices_registers = plant.get_registers()

        logger.debug("Registers: {}".format(devices_registers))

        plant_data = registers_to_plant_data(plant.name, devices_registers)

        ponyStorage = PonyMetricStorage()
        #apiStorage = ApiMetricStorage(url='http://')

        logger.info("**** Saving data in database ****")

        ponyStorage.insertPlantData(plant_data)
        # apiStorage.insertPlantData(plant_data)

    except Exception as err:
        logger.exception("[ERROR] %s" % err)

def task_counter_erp():
    c = Client(**config.erppeek)
    flux_client = client_db(db=config.influx)
    utcnow = datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
    meter_names = telemeasure_meter_names(c)
    try:
        for meter in meter_names:
            transfer_meter_to_plantmonitor(c, flux_client, meter, utcnow)

    except Exception as err:
        logger.error("[ERROR] %s" % err)
        raise

def task_meters_erp_to_orm():
    r = ReadingsFacade()

    try:
        # TODO mock measures or fake meters
        r.transfer_ERP_readings_to_model()
    except Exception as err:
        logger.error("[ERROR] %s" % err)
        raise


def task_get_meteologica_forecast():
    forecast()


def task_daily_upload_to_api_meteologica(test_env=True):
    configdb = ns.load('conf/config_meteologica.yaml')
    upload_meter_data(configdb, test_env=test_env)


def task_daily_download_from_api_meteologica(test_env=True):
    configdb = ns.load('conf/config_meteologica.yaml')
    download_meter_data(configdb, test_env=test_env)
