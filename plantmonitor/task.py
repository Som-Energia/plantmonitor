# -*- coding: utf-8 -*-
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

from pony import orm

import datetime

from ORM.models import database
from ORM.models import (
    Plant,
    Meter,
    MeterRegistry,
    Inverter,
    InverterRegistry,
)

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables

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

def publish_orm(plant_name, inverter_name=None, metrics=None):
    with orm.db_session:
        plant = Plant.get(name=plant_name)
        if not plant:
            return
        inverter = Inverter.get(name=inverter_name, plant=plant)
        if not inverter:
            return
        inverter.insertRegistry(**dict(metrics))

class PonyMetricStorage:
    def storeInverterMeasures(self, plant_name, inverter_name, metrics):
        publish_orm(plant_name, inverter_name, metrics)


def publish_influx(plant_name, inverter_name, metrics, flux_client):
    if flux_client is None:
        return
    point = dict(
        mesurements = 'sistema_inversor',
        tags = dict(
            location = plant_name,
            inverter_name = inverter_name,
        ),
        fields = metrics,
    )

    flux_client.write_points([point] )
    logger.info("[INFO] Sent to InfluxDB")

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

        if not plant.load('conf/modmap.yaml','Alcolea'):
            logger.error('Error loadinf yaml definition file...')
            sys.exit(-1)

        result = plant.get_registers()

        plant_name = plant.name

        flux_client = client_db(plant.db)

        for i, device in enumerate(plant.devices):
            inverter_name = plant.devices[i].name
            inverter_registers = result[i]['Alcolea'][0]['fields']

            logger.info("**** Saving data in database ****")
            logger.info("**** Metrics - tag - %s ****" %  inverter_name)
            logger.info("**** Metrics - tag - location %s ****" % plant_name)
            logger.info("**** Metrics - fields -  %s ****" % inverter_registers)

            publish_influx(plant_name, inverter_name, inverter_registers, flux_client)
            publish_timescale(plant_name, inverter_name, inverter_registers, db=config.plant_postgres)
            publish_orm(plant_name, inverter_name, inverter_registers)

    except Exception as err:
        logger.error("[ERROR] %s" % err)


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


def task_get_meteologica_forecast():
    forecast()


def task_daily_upload_to_api_meteologica(test_env=True):
    configdb = ns.load('conf/config_meteologica.yaml')
    upload_meter_data(configdb, test_env=test_env)


def task_daily_download_from_api_meteologica(test_env=True):
    configdb = ns.load('conf/config_meteologica.yaml')
    download_meter_data(configdb, test_env=test_env)
