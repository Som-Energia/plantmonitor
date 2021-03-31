#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

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
    SensorIntegratedIrradiation,
    SensorIrradiation,
    SensorTemperatureAmbient,
    SensorTemperatureModule,
    SensorIrradiationRegistry,
    SensorTemperatureAmbientRegistry,
    SensorTemperatureModuleRegistry,
    IntegratedIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)
from plantmonitor.storage import PonyMetricStorage, ApiMetricStorage

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables
from yamlns import namespace as ns
import datetime

from .operations import irradiance_to_df

setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)

class ApiClient_Test(unittest.TestCase):

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        database.drop_all_tables(with_all_data=True)

        self.maxDiff=None
        # orm.set_sql_debug(True)

        database.create_tables()

        # database.generate_mapping(create_tables=True)
        orm.db_session.__enter__()

        # log_level = "debug"
        log_level = "warning"


    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()


    def samplePlantNS(self):
        plantNS = ns.loads("""\
            name: alibaba
            codename: SCSOM04
            description: la bonica planta
            meters:
            - meter:
                name: '1234578'
            inverters:
            - inverter:
                name: '5555'
            - inverter:
                name: '6666'
            irradiationSensors:
            - irradiationSensor:
                name: alberto
            temperatureModuleSensors:
            - temperatureModuleSensor:
                name: pol
            temperatureAmbientSensors:
            - temperatureAmbientSensor:
                name: joana
            integratedSensors:
            - integratedSensor:
                name: voki""")
        return plantNS

    def samplePlantsData(self, time, dt):
        plantsData = [
              {
              "plant": "alibaba",
              "devices":
                sorted([{
                    "id": "SensorIrradiation:1234578",
                    "readings": [{
                        "time": time,
                        "irradiation_w_m2": 120,
                        "temperature_dc": 1230,
                    },{
                        "time": time+dt,
                        "irradiation_w_m2": 130,
                        "temperature_dc": 1230,
                    },{
                        "time": time+2*dt,
                        "irradiation_w_m2": 135,
                        "temperature_dc": 1230,
                    }],
                }],key=lambda d: d['id']),
              }
            ]
        return plantsData


    def test_orm_to_df(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=30)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        sensor = SensorIrradiation.select().first()
        df = irradiance_to_df(sensor)

        print(df)

        self.assertEqual(df, [])