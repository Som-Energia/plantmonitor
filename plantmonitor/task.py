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

def publish_influx(metrics,flux_client):
    flux_client.write_points([metrics] )
    logger.info("[INFO] Sent to InfluxDB")

def publish_timescale(metrics,db):
    with psycopg2.connect(
            user=db['user'], password=db['password'],
            host=db['host'], port=db['port'], database=db['database']
        ) as conn:
        with conn.cursor() as cur:
            measurement    = metrics['measurement']
            inverter_name  = metrics['tags']['inverter_name']
            location       = metrics['tags']['location']
            query_content  = ', '.join(metrics['fields'].keys())
            values_content = ', '.join(["'{}'".format(v) for v in metrics['fields'].values()])

            cur.execute(
                "INSERT INTO {}(time, inverter_name, location, {}) \
                VALUES (timezone('utc',NOW()), '{}', '{}', {});".format(
                    measurement,query_content,
                    inverter_name,location,values_content
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

            if flux_client is not None:
                metrics = {}
                tags = {}
                fields = {}
                metrics['measurement'] = 'sistema_inversor'
                tags['location'] = plant_name
                tags['inverter_name'] = inverter_name
                metrics['tags'] = tags
                metrics['fields'] = inverter_registers

                publish_influx(metrics,flux_client)

        publish_timescale(metrics, db=config.plant_postgres)

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
