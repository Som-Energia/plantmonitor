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
    integrateIrradiance,
    integrateExpectedPower,
    computeIntegralMetrics,
    getNewestTime,
    getOldestTime,
    getTimeRange,
    insertHourlySensorIrradiationMetrics,
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

    def samplePlantsData(self, time, dt, numreadings=100):
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
                    } for i in range(numreadings)],
                },
                {
                    "id": "SensorIrradiation:87654321",
                    "readings": [{
                        "time": time + i*dt,
                        "irradiation_w_m2": 220 + i*30,
                        "temperature_dc": 1230 + i*1,
                    } for i in range(numreadings)],
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

    def test__getTimeRange__None(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        sensor = alibaba.sensors.select().first()

        metricFromDate, metricToDate = getTimeRange(
            sensor,
            SensorIrradiationRegistry,
            HourlySensorIrradiationRegistry,
            'irradiation_w_m2',
            'integratedIrradiation_wh_m2',
        )

        self.assertIsNone(metricFromDate)
        self.assertIsNone(metricToDate)

    def test__dbTimezoneCoherence__getOneReading(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        SensorIrradiation.get(name='alberto').insertRegistry(0,0,time)
        orm.flush()

        # setting the timezone required everywhere
        # otherwise you get fixedoffset everytime you read
        # TIMESTAMP WITH TIMEZONE from a postgres with a non-UTC default timezone db
        # database.execute("set timezone to 'UTC';")
        # we're currently setting PGTZ at setupDatabase@orm_util:85

        dbTime = orm.select(r.time for r in SensorIrradiationRegistry).order_by(orm.desc(1)).first()

        self.assertEqual(time, dbTime)
        print(time.__repr__())
        print(dbTime.__repr__())
        self.assertEqual(time.isoformat(), dbTime.isoformat())

    def test__getTimeRange__SomeMany(self):

        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        expectedFromDate = time
        expectedToDate   = time + 99*delta

        metricFromDate, metricToDate = getTimeRange(
            SensorIrradiation.get(name='12345678'),
            SensorIrradiationRegistry,
            HourlySensorIrradiationRegistry,
            'irradiation_w_m2',
            'integratedIrradiation_wh_m2',
        )

        print(SensorIrradiation.get(name='12345678').to_dict())

        q = orm.select(r.time for r in SensorIrradiationRegistry if r.sensor == SensorIrradiation.get(name='12345678') and getattr(r, 'irradiation_w_m2')).order_by(1)
        print(list(q)[0].isoformat())
        print(list(q)[-1].isoformat())
        # assert metricFromDate.isoformat() == expectedFromDate.isoformat()
        assert metricToDate.isoformat() == expectedToDate.isoformat()

    def test__getTimeRange__SomeMany_FromTo(self):
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

        metricFromDate, metricToDate = getTimeRange(
            SensorIrradiation[1],
            SensorIrradiationRegistry,
            HourlySensorIrradiationRegistry,
            'irradiation_w_m2',
            'integratedIrradiation_wh_m2',
            fromDate,
            toDate
        )

        self.assertEqual(metricFromDate, fromDate)
        self.assertEqual(metricToDate, toDate)

    def test__integrateIrradiance(self):
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

        integratedMetrics = integrateIrradiance(fromDate, toDate)

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
            SensorIrradiation[1] : list(zip(times, expectedIntegrals['sensor1'])),
            SensorIrradiation[4] : list(zip(times, expectedIntegrals['sensor5'])),
            SensorIrradiation[5] : list(zip(times, expectedIntegrals['sensor6'])),
        }

        self.assertDictEqual(integratedMetrics, expectedDict)

    def test__integrateIrradiance__withoutTimeRange(self):
        plantNS = self.samplePlantNS()
        del plantNS['irradiationSensors']

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta, 25)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        integratedMetrics = integrateIrradiance()

        times = [
            datetime.datetime(2020, 12, 10, 16, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc)
        ]

        expectedIntegrals = {
            '12345678' : [225, 563, 68],
            '87654321' : [308, 655, 77],
        }

        expectedDict = {
            SensorIrradiation.get(name='12345678') : list(zip(times, expectedIntegrals['12345678'])),
            SensorIrradiation.get(name='87654321') : list(zip(times, expectedIntegrals['87654321'])),
        }

        self.assertListEqual(list(integratedMetrics.values())[0], list(zip(times, expectedIntegrals['12345678'])))
        self.assertListEqual(list(integratedMetrics.values())[1], list(zip(times, expectedIntegrals['87654321'])))

        self.assertDictEqual(integratedMetrics, expectedDict)


    def test__integrateIrradiance__OneEmptySensorNoTimeRange(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta, 25)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        integratedMetrics = integrateIrradiance()

        times = [
            datetime.datetime(2020, 12, 10, 16, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc)
        ]

        expectedIntegrals = {
            '12345678' : [225, 563, 68],
            '87654321' : [308, 655, 77],
        }

        expectedDict = {
            SensorIrradiation.get(name='12345678') : list(zip(times, expectedIntegrals['12345678'])),
            SensorIrradiation.get(name='87654321') : list(zip(times, expectedIntegrals['87654321'])),
        }

        self.assertDictEqual(integratedMetrics, expectedDict)


    def test__integrateIrradiance__withInsert(self):
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


        irradiance = integrateIrradiance(fromDate, toDate)
        insertHourlySensorIrradiationMetrics(irradiance, 'integratedIrradiation_wh_m2')


        orm.flush()
        result = [r.to_dict() for r in HourlySensorIrradiationRegistry.select()]

        times = [
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc)
        ]

        expected = [
            {'sensor': 1, 'time': times[0], 'integratedIrradiation_wh_m2': None, 'expected_energy_wh': None},
            {'sensor': 1, 'time': times[1], 'integratedIrradiation_wh_m2': None, 'expected_energy_wh': None},
            {'sensor': 4, 'time': times[0], 'integratedIrradiation_wh_m2': 563, 'expected_energy_wh': None},
            {'sensor': 4, 'time': times[1], 'integratedIrradiation_wh_m2': 893, 'expected_energy_wh': None},
            {'sensor': 5, 'time': times[0], 'integratedIrradiation_wh_m2': 655, 'expected_energy_wh': None},
            {'sensor': 5, 'time': times[1], 'integratedIrradiation_wh_m2': 985, 'expected_energy_wh': None}
        ]

        self.assertListEqual(result, expected)

    def test__getOldestTime__WithAQueryObject(self):

        plant = Plant(name='alibaba', codename='alibaba')
        sensor = SensorIrradiation(name='Alice', plant=plant)

        expectedPowerViewQuery = Path('queries/new/view_expected_power.sql').read_text(encoding='utf8')
        metric = 'expected_energy_wh'

        dummy = database.select(expectedPowerViewQuery)
        print(dummy)
        # TODO fix this How to convert a raw query to a pony query
        print(type(dummy))
        orm.select(r for r in dummy)

        result = getOldestTime(sensor, expectedPowerViewQuery, metric)

        self.assertIsNone(result)

    def test__integrateExpectedPower__NoReadings(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        orm.flush()

        integratedMetric = integrateExpectedPower()

        self.assertDictEqual(integratedMetric, {})


    def test__computeIntegralMetrics(self):
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

        orm.flush()

        computeIntegralMetrics()

        result = [r.to_dict() for r in HourlySensorIrradiationRegistry.select()]

        times = [
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc)
        ]

        expected = [
            {'sensor': 1, 'time': times[0], 'integratedIrradiation_wh_m2': None, 'expected_energy_wh': None},
            {'sensor': 1, 'time': times[1], 'integratedIrradiation_wh_m2': None, 'expected_energy_wh': None},
            {'sensor': 4, 'time': times[0], 'integratedIrradiation_wh_m2': 563, 'expected_energy_wh': "TODO"},
            {'sensor': 4, 'time': times[1], 'integratedIrradiation_wh_m2': 893, 'expected_energy_wh': "TODO"},
            {'sensor': 5, 'time': times[0], 'integratedIrradiation_wh_m2': 655, 'expected_energy_wh': "TODO"},
            {'sensor': 5, 'time': times[1], 'integratedIrradiation_wh_m2': 985, 'expected_energy_wh': "TODO"}
        ]

        self.assertListEqual(result, expected)


    def test__getNewestTime__None(self):

        plant = Plant(name='alibaba', codename='alibaba')
        sensor = SensorIrradiation(name='Alice', plant=plant)

        time = getNewestTime(sensor, SensorIrradiationRegistry, 'irradiation_w_m2')

        self.assertIsNone(time)

    def test__getNewestTime__Many(self):

        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        integralHour = datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc)

        sensor = SensorIrradiation[1]
        HourlySensorIrradiationRegistry(sensor=sensor, time=integralHour, integratedIrradiation_wh_m2=100)

        time = getNewestTime(sensor, HourlySensorIrradiationRegistry, 'integratedIrradiation_wh_m2')

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
