# -*- coding: utf-8 -*-
import json
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
    SensorTemperatureAmbient,
    SensorTemperatureModule,
    SensorTemperatureAmbientRegistry,
    SensorTemperatureModuleRegistry,
)

from ORM.orm_util import connectDatabase, getTablesToTimescale, timescaleTables

class PonyMetricStorage:

    def sensor(self, sensor_fk):
        device = Sensor[sensor_fk]
        return device.to_dict()

    def sensorTemperatureAmbientReadings(self):
        sensorReadings = orm.select(c for c in SensorTemperatureAmbientRegistry)
        return list(x.to_dict() for x in sensorReadings)

    #TODO return all data (or filter by date)
    def plantData(self, plant_name):
        plant = Plant.get(name=plant_name)
        if plant:
            return plant.plantData()
        return {}

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

    def insertPlantData(self, plant_data):
        with orm.db_session:
            plant_name = plant_data["plant"]
            plant = Plant.get(name=plant_name)
            if not plant:
                logger.warning("No plant named {}".format(plant_name))
                return
            data_time = plant_data["time"] if "time" in plant_data else datetime.datetime.utcnow()
            result = plant.insertPlantData(plant_data)
            print(result)
            return result


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

        # change datetime to string
        #TODO find a better way
        for r in readings:
            if "time" in r:
                r["time"] = r["time"].isoformat()

        plant_data["devices"] = [{"id" : id, "readings": [reading for reading in readings]}]

        return plant_data

    # TODO temporary hack. make json support datetime
    @staticmethod
    def datetimeToStr(plant_data):
        if 'time' in plant_data and not isinstance(plant_data['time'], str):
            plant_data['time'] = plant_data['time'].isoformat()

        if 'devices' in plant_data:
            for d in plant_data['devices']:
                if 'readings' in d:
                    for r in d['readings']:
                        if 'time' in r and not isinstance(r['time'], str):
                            r['time'] = r['time'].isoformat()

    def insertPlantData(self, plant_data):
        plant_name = plant_data['plant']
        self.datetimeToStr(plant_data)
        r = requests.put("{}/plant/{}/readings".format(self.api_url, plant_name), json=plant_data)
        response_data = json.loads(r.text)
        if r.status_code == 200:
            return response_data

        logger.error("Api error code {} response: {}".format(r.status_code, r))
        return response_data


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

            reading = dict(metrics)
            readings = [reading]

            device_type = "Inverter"
            plant_data = self.plant_data(
                plant_name=plant_name,
                device_type=device_type,
                device_name=inverter_name,
                readings=readings
                )

            logger.info("puting metrics to endpoint {}".format(plant_data))

            r = requests.put("{}/plant/{}/readings".format(self.api_url, plant_name), json=plant_data)

            print(r.content)

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