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

from .operations import computeIntegralMetrics

from meteologica.daily_upload_to_api import upload_meter_data
from meteologica.forecasts import (
    downloadMeterForecasts,
    uploadMeterReadings
)

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

def legacyInfluxUpload():
    plantInflux = ProductionPlant()

    if not plantInflux.load('conf/modmapLegacyInflux.yaml', 'Alcolea'):
        logger.error('Error loading yaml definition file...')
        sys.exit(-1)

    data = ns.load('conf/modmap.yaml')
    for plant_data in data.plantmonitor:
        influxdb = plant_data.influx
    plantInfluxRegisters = plantInflux.get_registers()

    fluxStorage = InfluxMetricStorage(influxdb)
    for i, device in enumerate(plantInflux.devices):
        inverter_name = plantInflux.devices[i].name
        inverter_registers = plantInfluxRegisters[i]['Alcolea'][0]['fields']
        logger.info("**** Saving data in database ****")
        logger.info("**** Metrics - tag - %s ****" %  inverter_name)
        logger.info("**** Metrics - tag - location %s ****" % plantInflux.name)
        logger.info("**** Metrics - fields -  %s ****" % inverter_registers)
        logger.info("**** Log to flux ****")
        fluxStorage.storeInverterMeasures(plantInflux.name, inverter_name, inverter_registers)

    logger.info("Sleeping 5 secs")
    import time
    time.sleep(5)
    logger.info("Done influx upload")

def task():
    try:

        plantname = envinfo.ACTIVEPLANT_CONF['activeplant']

        # TODO remove legacy
        if plantname == 'Alcolea':
            legacyInfluxUpload()

        plant = ProductionPlant()

        apiconfig = envinfo.API_CONFIG

        if not plant.load('conf/modmap.yaml', plantname):
            logger.error('Error loading yaml definition file...')
            sys.exit(-1)

        plants_registers = plant.get_registers()

        logger.debug("plants Registers: {}".format(plants_registers))

        if len(plants_registers) > 0:
            logger.error("plants Registers: {}".format(plants_registers))

        if plantname not in plants_registers[0]:
            logger.error("plant {} is not in registers {}. Check the plant modmap.".format(plantname, plants_registers[0].keys()))
            return

        plant_data = registers_to_plant_data(plantname, plants_registers)

        if not plant_data:
            logger.error("Registers to plant data returned {}".format(plant_data))
            return

        ponyStorage = PonyMetricStorage()
        apiStorage = ApiMetricStorage(apiconfig)

        logger.info("**** Saving data in database ****")
        logger.debug("plant_data: {}".format(plant_data))
        ponyStorage.insertPlantData(plant_data)

        logger.info("**** Saving data in Api ****")
        result = apiStorage.insertPlantData(plant_data)
        logger.debug("api response: {}".format(result))

    except Exception as err:
        logger.exception("[ERROR] %s" % err)

def task_counter_erp():
    c = Client(**config.erppeek)
    flux_client = client_db(db=config.influx)
    utcnow = datetime.datetime.strftime(datetime.datetime.now(datetime.timezone.utc), "%Y-%m-%d %H:%M:%S")
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

def task_daily_upload_to_api_meteologica(test_env=True):
    configdb = ns.load('conf/config_meteologica.yaml')
    with orm.db_session:
        uploadMeterReadings(configdb, test_env=test_env)


def task_daily_download_from_api_meteologica(test_env=True):
    configdb = ns.load('conf/config_meteologica.yaml')
    with orm.db_session:
        downloadMeterForecasts(configdb, test_env=test_env)


def task_integral():
    with orm.db_session:
        computeIntegralMetrics()

def task_update_meter_protocol()
