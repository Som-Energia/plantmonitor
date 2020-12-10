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

from pony import orm

import datetime

from ORM.models import database
from ORM.models import (
    Plant,
    Meter,
    MeterRegistry,
    Inverter,
    InverterRegistry,
    Sensor,
    SensorTemperature,
    SensorTemperatureRegistry,
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

class PonyMetricStorage:

    def inverter(self, inverter_fk):
        device = Inverter[inverter_fk]
        return device.to_dict()

    def inverterReadings(self):
        inverterReadings = orm.select(c for c in InverterRegistry)
        return list(x.to_dict() for x in inverterReadings)

    def storeInverterMeasures(self, plant_name, inverter_name, metrics):
        with orm.db_session:
            plant = Plant.get(name=plant_name)
            if not plant:
                logger.debug("No plant named {}".format(plant_name))
                return
            inverter = Inverter.get(name=inverter_name, plant=plant)
            if not inverter:
                logger.debug("No inverter named {}".format(inverter_name))
                return
            inverterMetricsAndSensors = metrics
            excludedColumns = [
                'probe1value',
                'probe2value',
                'probe3value',
                'probe4value',
                ]
            inverterMetricsAndSensorsDict = dict(inverterMetricsAndSensors)
            register_values_dict = { k:v for k,v in inverterMetricsAndSensorsDict.items() if k not in excludedColumns}
            inverter.insertRegistry(**register_values_dict)

class ApiMetricStorage:
    def __init__(self, config):
        self.api_url = config['api_url']
        self.version = config['version']

    def inverterReadings(self):
        # connect to api and get readings
        pass

    def plant_data(self, plant_name, device_type, device_name, readings):

        time = datetime.datetime.now(datetime.timezone.utc)
        
        plant_data = {
            "plant": plant_name, 
            "version": self.version,
            "time": time.isoformat(),
        }

        id = "{}:{}".format(device_type, device_name)

        plant_data["devices"] = [{"id" : id, "reading": reading} for reading in readings]

        return plant_data

    def storeInverterMeasures(self, plant_name, inverter_name, metrics):
        # connect to api and put readings

        with orm.db_session:
            # TODO REST get endpoints
            # plants = requests.get("{}/plants".format(self.api_url))
            # if plant_name not in plants:
            #     logger.debug("No plant named {}".format(plant_name))
            #     return
            # plant_id = plants.id
            # inverter = requests.get("{}/plant/{}/inverters".format(self.api_url, plant_id))
            # if inverter_name not in inverter:
            #     logger.debug("No inverter named {}".format(inverter_name))
            #     return
            
            inverterMetricsAndSensorsDict = dict(metrics)
            
            device_type = "Inverter"
            plant_data = self.plant_data(
                plant_name=plant_name, 
                device_type=device_type,
                device_name=inverter_name, 
                readings=inverterMetricsAndSensorsDict
                )

            r = requests.put("{}/plant/{}/readings".format(self.api_url, plant_name), json=plant_data)

            if r.status_code != 200:
                logger.error("Error {} putting plant data".format(r.status_code))

            return r.status_code == 200

class InfluxMetricStorage:
    def __init__(self, config):
        self._flux = client_db(config)

    def storeInverterMeasures(self, plant_name, inverter_name, metrics):
        if self._flux is None:
            return
        point = dict(
            mesurements = 'sistema_inversor',
            tags = dict(
                location = plant_name,
                inverter_name = inverter_name,
            ),
            fields = metrics,
        )

        self._flux.write_points([point] )
        logger.info("[INFO] Sent to InfluxDB")

class TimeScaleMetricStorage:

    def __init__(self,config):
        self._config = config
        self._dbconnetion = None

    def __enter__(self):
        return self._db()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._dbconnetion = None

    def _db(self):
        if self._dbconnetion: return self._dbconnetion
        self._dbconnetion = psycopg2.connect(**self._config)
        return self._dbconnetion

    def storeInverterMeasures(self, plant_name, inverter_name, metrics):
        with self._db().cursor() as cur:
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

        ponyStorage = PonyMetricStorage()
        fluxStorage = InfluxMetricStorage(plant.db)
        tsStorage = TimeScaleMetricStorage(config.plant_postgres)
        #apiStorage = ApiMetricStorage(url='http://')


        for i, device in enumerate(plant.devices):
            inverter_name = plant.devices[i].name
            inverter_registers = result[i]['Alcolea'][0]['fields']

            logger.info("**** Saving data in database ****")
            logger.info("**** Metrics - tag - %s ****" %  inverter_name)
            logger.info("**** Metrics - tag - location %s ****" % plant_name)
            logger.info("**** Metrics - fields -  %s ****" % inverter_registers)

            fluxStorage.storeInverterMeasures(plant_name, inverter_name, inverter_registers)
            ponyStorage.storeInverterMeasures(plant_name, inverter_name, inverter_registers)
            tsStorage.storeInverterMeasures(plant_name, inverter_name, inverter_registers)


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
