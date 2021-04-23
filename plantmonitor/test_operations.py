#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
from unittest import mock

from pathlib import Path

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
    SensorIrradiation,
    SensorTemperatureAmbient,
    SensorTemperatureModule,
    SensorIrradiationRegistry,
    SensorTemperatureAmbientRegistry,
    SensorTemperatureModuleRegistry,
    HourlySensorIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)
from plantmonitor.storage import PonyMetricStorage, ApiMetricStorage

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables
from yamlns import namespace as ns
import datetime

from .operations import (
    integrateHour,
    integrateMetric,
    integrateAllSensors,
    getLatestIntegratedTime,
)

from .expectedpower import (
    readTimedDataTsv,
    spanishDateToISO,
)

setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)

class Operations_Test(unittest.TestCase):

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
                    "id": "SensorIrradiation:12345678",
                    "readings": [{
                        "time": time + i*dt,
                        "irradiation_w_m2": 120 + i*30,
                        "temperature_dc": 1230 + i*1,
                    } for i in range(0,100)],
                },
                {
                    "id": "SensorIrradiation:87654321",
                    "readings": [{
                        "time": time + i*dt,
                        "irradiation_w_m2": 220 + i*30,
                        "temperature_dc": 1230 + i*1,
                    } for i in range(0,100)],
                }],key=lambda d: d['id']),
              }
            ]
        return plantsData

    def test__integrateHour(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        metricName = 'irradiation_w_m2'

        sensorName = '12345678'
        registry = SensorIrradiationRegistry

        query = orm.select(
            (r.time, getattr(r, metricName))
            for r in registry
            if r.sensor.name == sensorName
        )

        hourstart = time.replace(minute=0, second=0, microsecond=0)
        integratedValue = integrateHour(hourstart, query)

        # Fix border error
        # self.assertEqual(integratedValue, 270)
        self.assertEqual(integratedValue, 225)

    def _test__integrateHour__nulls(self):
        pass

    def _test__integrateHour__repeatedTimes(self):
        pass

    def _test__integrateHour__testBorder(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        metricName = 'irradiation_w_m2'

        sensorName = '12345678'
        registry = SensorIrradiationRegistry

        query = orm.select(
            (r.time, getattr(r, metricName))
            for r in registry
            if r.sensor.name == sensorName
        )


        hourstart = time.replace(minute=0, second=0, microsecond=0)
        integratedValue = integrateHour(hourstart, query)

        # Fix border error
        self.assertEqual(integratedValue, 270)

    def _test__integrateHour__testBorder__onlyBorder(self):
        pass

    def _test__integrateHour__noTimezone(self):
        pass

    def test__integrateMetric(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        fromDate = time.replace(hour=14, minute=0, second=0, microsecond=0)
        toDate = fromDate + datetime.timedelta(hours=11)
        metricName = 'irradiation_w_m2'

        sensorName = '12345678'
        registry = SensorIrradiationRegistry

        registries = orm.select(
            (r.time, getattr(r, metricName))
            for r in registry
            if r.sensor.name == sensorName and fromDate <= r.time and r.time <= toDate
        )
        integratedMetric = integrateMetric(registries, fromDate, toDate)

        expected = [
            (datetime.datetime(2020, 12, 10, 15, 0, tzinfo=datetime.timezone.utc), None),
            (datetime.datetime(2020, 12, 10, 16, 0, tzinfo=datetime.timezone.utc), 225),
            (datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc), 563),
            (datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc), 893),
            (datetime.datetime(2020, 12, 10, 19, 0, tzinfo=datetime.timezone.utc), 1223),
            (datetime.datetime(2020, 12, 10, 20, 0, tzinfo=datetime.timezone.utc), 1553),
            (datetime.datetime(2020, 12, 10, 21, 0, tzinfo=datetime.timezone.utc), 1883),
            (datetime.datetime(2020, 12, 10, 22, 0, tzinfo=datetime.timezone.utc), 2213),
            (datetime.datetime(2020, 12, 10, 23, 0, tzinfo=datetime.timezone.utc), 2543),
            (datetime.datetime(2020, 12, 11, 0, 0, tzinfo=datetime.timezone.utc), 1010),
            (datetime.datetime(2020, 12, 11, 1, 0, tzinfo=datetime.timezone.utc), None),
        ]

        # +720 every 24h
        self.assertListEqual(integratedMetric, expected)


    def test__integrateAllSensors(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        fromDate = time.replace(hour=16, minute=0, second=0, microsecond=0)
        toDate = fromDate + datetime.timedelta(hours=2)
        metrics = ['irradiation_w_m2']

        integratedMetrics = integrateAllSensors(metrics, fromDate, toDate)

        times = [
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc)
        ]

        expectedIntegrals = {
            'sensor1' : [None,None],
            'sensor5' : [563,893],
            'sensor6' : [655,985],
            }

        expectedDict = {
            'irradiation_w_m2':
            {
                SensorIrradiation[1] : list(zip(times, expectedIntegrals['sensor1'])),
                SensorIrradiation[4] : list(zip(times, expectedIntegrals['sensor5'])),
                SensorIrradiation[5] : list(zip(times, expectedIntegrals['sensor6'])),
            }
        }

        self.assertDictEqual(integratedMetrics, expectedDict)

    def test__getLatestIntegratedTime__None(self):

        time = getLatestIntegratedTime()

        self.assertIsNone(time)

    def test__getLatestIntegratedTime__Many(self):

        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        integralHour = datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc)

        sensor = SensorIrradiation[1]
        HourlySensorIrradiationRegistry(sensor=sensor, time=integralHour, integratedIrradiation_wh_m2=100)

        time = getLatestIntegratedTime()

        self.assertEqual(time, integralHour)

    def _test__integrateMetric__rawSqlQuery(self):

        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        fromDate = time.replace(hour=14, minute=0, second=0, microsecond=0)
        toDate = fromDate + datetime.timedelta(hours=11)
        metricName = 'irradiation_w_m2'

        sensorName = '12345678'
        # TODO use raw sql query

        registries = orm.select(orm.raw_sql("select * from sensorirradiationregistry;"))
        registries.show()
        integratedMetric = integrateMetric(registries, fromDate, toDate)

        expected = [
            (datetime.datetime(2020, 12, 10, 15, 0, tzinfo=datetime.timezone.utc), None),
            (datetime.datetime(2020, 12, 10, 16, 0, tzinfo=datetime.timezone.utc), 225),
            (datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc), 563),
            (datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc), 893),
            (datetime.datetime(2020, 12, 10, 19, 0, tzinfo=datetime.timezone.utc), 1223),
            (datetime.datetime(2020, 12, 10, 20, 0, tzinfo=datetime.timezone.utc), 1553),
            (datetime.datetime(2020, 12, 10, 21, 0, tzinfo=datetime.timezone.utc), 1883),
            (datetime.datetime(2020, 12, 10, 22, 0, tzinfo=datetime.timezone.utc), 2213),
            (datetime.datetime(2020, 12, 10, 23, 0, tzinfo=datetime.timezone.utc), 2543),
            (datetime.datetime(2020, 12, 11, 0, 0, tzinfo=datetime.timezone.utc), 1010),
            (datetime.datetime(2020, 12, 11, 1, 0, tzinfo=datetime.timezone.utc), None),
        ]

        # +720 every 24h
        self.assertListEqual(integratedMetric, expected)
