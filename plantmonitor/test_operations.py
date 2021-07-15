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
    PlantModuleParameters,
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
    integrateHourFromTimeseries,
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

    def samplePlantNSWithModuleParameters(self):
        plantNS = ns.loads("""\
            name: alibaba
            codename: SCSOM04
            description: la bonica planta
            moduleParameters:
                nominalPowerMWp: 2.16
                efficiency: 15.5
                nModules: 4878
                degradation: 97.5
                Imp: 9.07
                Vmp: 37.5
                temperatureCoefficientI: 0.05
                temperatureCoefficientV: -0.31
                temperatureCoefficientPmax: -0.442
                irradiationSTC: 1000.0
                temperatureSTC: 25
                Voc: 46.1
                Isc: 9.5
            irradiationSensors:
            - irradiationSensor:
                name: alberto
            """)
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

    parametersAlcolea = dict(
        nominalPowerMWp = 2.16,
        efficiency = 15.5,
        nModules = 8640, # plant parameter
        degradation = 97.0,
        Imp = 8.27, # A, module model param
        Vmp = 30.2, # V, module model param
        temperatureCoefficientI = 0.088, # %/ºC, module model param
        temperatureCoefficientV = -0.352, # %/ºC, module model param
        temperatureCoefficientPmax = -0.442, # %/ºC, module model param
        irradiationSTC = 1000.0, # W/m2, module model param
        temperatureSTC = 25, # ºC, module model param
    )

    parametersFlorida = dict(
        nominalPowerMWp = 2.16, # TODO use Florida values
        efficiency = 15.5, # TODO use Florida values
        nModules = 4878, # plant parameter
        degradation=97.5, # %, module model param
        Imp = 9.07, # A, module model param
        Vmp = 37.5, # V, module model param
        temperatureCoefficientI = 0.05, # %/ºC, module model param
        temperatureCoefficientV = -0.31, # %/ºC, module model param
        temperatureCoefficientPmax = -0.442, # %/ºC, module model param
        irradiationSTC = 1000.0, # W/m2, module model param
        temperatureSTC = 25, # ºC, module model param
        Voc = 46.1, # V, module model param
        Isc = 9.5, # A, module model param
    )

    def createPlantParameters(self, plant_id, **data):
        data = ns(data)
        plant = PlantModuleParameters(
            plant=plant_id,
            nominal_power_wp=int(data.nominalPowerMWp*1000000),
            efficency_cpercent=int(data.efficiency*100),
            n_modules = data.nModules,
            degradation_cpercent = int(data.degradation*100),
            max_power_current_ma = int(data.Imp*1000),
            max_power_voltage_mv = int(data.Vmp*1000),
            current_temperature_coefficient_mpercent_c = int(data.temperatureCoefficientI*1000),
            voltage_temperature_coefficient_mpercent_c = int(data.temperatureCoefficientV*1000),
            max_power_temperature_coefficient_mpercent_c = int(data.temperatureCoefficientPmax*1000),
            standard_conditions_irradiation_w_m2 = int(data.irradiationSTC),
            standard_conditions_temperature_dc = int(data.temperatureSTC*10),
            opencircuit_voltage_mv = int(data.Voc*1000),
            shortcircuit_current_ma = int(data.Isc*1000),
        )
        plant.flush()

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
        self.assertEqual(integratedValue, 259)

    def test__integrateHour__irradiation(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2021, 5, 25, 10, 0, 0, 0, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        irradiance_w_m2 = [980,970,970,964,956,943,957,904,809,623,895,869]
        plantsData = [
              {
              "plant": "alibaba",
              "devices":
                [{
                    "id": "SensorIrradiation:12345678",
                    "readings": [{
                        "time": time + i*delta,
                        "irradiation_w_m2": irradiance_w_m2[i],
                        "temperature_dc": 0,
                    } for i in range(0,12)],
                }],
              }
            ]
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
        self.assertEqual(integratedValue, 826)

    def test__integrateHour__irradiation2(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2021, 5, 25, 16, 0, 6, 926371, tzinfo=datetime.timezone.utc)
        time = time.replace(minute=0, second=0, microsecond=0)
        delta = datetime.timedelta(minutes=5)
        irradiance_w_m2 = [909, 900, 889, 877, 866, 851, 840, 828, 818, 808, 792, 778, 780]
        plantsData = [
              {
              "plant": "alibaba",
              "devices":
                [{
                    "id": "SensorIrradiation:12345678",
                    "readings": [{
                        "time": time + i*delta,
                        "irradiation_w_m2": irradiance_w_m2[i],
                        "temperature_dc": 0,
                    } for i in range(13)],
                }],
              }
            ]
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
        self.assertEqual(integratedValue, 840)

    def test__integrateHour__irradiation_border(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2021, 5, 25, 16, 0, 6, 926371, tzinfo=datetime.timezone.utc)
        hourstart = time.replace(minute=0, second=0, microsecond=0)
        delta = datetime.timedelta(minutes=5)
        time = time - delta
        irradiance_w_m2 = [915, 909, 900, 889, 877, 866, 851, 840, 828, 818, 808, 792, 778, 780, 760]
        plantsData = [
              {
              "plant": "alibaba",
              "devices":
                [{
                    "id": "SensorIrradiation:12345678",
                    "readings": [{
                        "time": time + i*delta,
                        "irradiation_w_m2": irradiance_w_m2[i],
                        "temperature_dc": 0,
                    } for i in range(15)],
                }],
              }
            ]
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

        integratedValue = integrateHour(hourstart, query)

        # Fix border error
        self.assertEqual(integratedValue, 841)

    def test__integrateHourFromTimeSeries__OnlyOnePoint(self):

        readingTime = datetime.datetime(2021, 3, 26, 11, 55, 6, 766707, tzinfo=datetime.timezone.utc)

        hourstart = readingTime.replace(minute=0, second=0, microsecond=0)

        timeseries = [(readingTime, 922)]

        integralMetricValue = integrateHourFromTimeseries(hourstart, timeseries)

        self.assertIsNone(integralMetricValue)

    def test__integrateHourFromTimeSeries__AllValuesNoneButOne(self):

        dt = datetime.timedelta(minutes=5)

        readingTime = datetime.datetime(2021, 3, 26, 11, 55, 6, 766707, tzinfo=datetime.timezone.utc)

        hourstart = readingTime.replace(minute=0, second=0, microsecond=0)

        timeseries = [(hourstart + i*dt, None) for i in range(11)]
        timeseries.append((hourstart + 12*dt, 0.0))

        integralMetricValue = integrateHourFromTimeseries(hourstart, timeseries)

        self.assertIsNone(integralMetricValue)

    def test__integrateHourFromTimeSeries__AllValuesNoneButOne(self):

        dt = datetime.timedelta(minutes=5)

        readingTime = datetime.datetime(2021, 3, 26, 11, 55, 6, 766707, tzinfo=datetime.timezone.utc)

        hourstart = readingTime.replace(minute=0, second=0, microsecond=0)

        timeseries = [(hourstart + i*dt, None) for i in range(11)]
        timeseries.append((hourstart + 12*dt, 0.0))

        integralMetricValue = integrateHourFromTimeseries(hourstart, timeseries)

        self.assertIsNone(integralMetricValue)

    def test__integrateHourFromTimeSeries__UnorderedPositiveTest(self):

        readingTime = datetime.datetime(2021, 5, 21, 8, 0, 6, 766707, tzinfo=datetime.timezone.utc)

        hourstart = readingTime.replace(minute=0, second=0, microsecond=0)

        timeseries = [
            (datetime.datetime(2021, 5, 21, 8, 25, 6, 839190, tzinfo=datetime.timezone.utc), 82),
            (datetime.datetime(2021, 5, 21, 8, 40, 6, 707526, tzinfo=datetime.timezone.utc), 136),
            (datetime.datetime(2021, 5, 21, 8, 5, 7, 62183, tzinfo=datetime.timezone.utc), 45),
            (datetime.datetime(2021, 5, 21, 8, 30, 6, 904319, tzinfo=datetime.timezone.utc), 90),
            (datetime.datetime(2021, 5, 21, 8, 50, 6, 890866, tzinfo=datetime.timezone.utc), 173),
            (datetime.datetime(2021, 5, 21, 8, 45, 6, 765174, tzinfo=datetime.timezone.utc), 143),
            (datetime.datetime(2021, 5, 21, 8, 55, 6, 873454, tzinfo=datetime.timezone.utc), 152),
            (datetime.datetime(2021, 5, 21, 8, 35, 6, 780839, tzinfo=datetime.timezone.utc), 109),
            (datetime.datetime(2021, 5, 21, 8, 0, 6, 927305, tzinfo=datetime.timezone.utc), 42),
            (datetime.datetime(2021, 5, 21, 8, 10, 6, 812438, tzinfo=datetime.timezone.utc), 51),
            (datetime.datetime(2021, 5, 21, 8, 20, 6, 776351, tzinfo=datetime.timezone.utc), 67),
            (datetime.datetime(2021, 5, 21, 8, 15, 6, 699902, tzinfo=datetime.timezone.utc), 59)
        ]

        integralMetricValue = integrateHourFromTimeseries(hourstart, timeseries)

        self.assertTrue(integralMetricValue >= 0)

    @unittest.skipIf(True, "Fix or change this test if we want to use integrals again")
    def test__integrateHourFromTimeSeries__AverageSameResultTest(self):

        readingTime = datetime.datetime(2021, 5, 21, 8, 0, 6, 766707, tzinfo=datetime.timezone.utc)

        hourstart = readingTime.replace(minute=0, second=0, microsecond=0)

        timeseries = [
            (datetime.datetime(2021, 5, 21, 8, 0, 0, 0, tzinfo=datetime.timezone.utc), 142),
            (datetime.datetime(2021, 5, 21, 8, 5, 7, 62183, tzinfo=datetime.timezone.utc), 45),
            (datetime.datetime(2021, 5, 21, 8, 10, 6, 812438, tzinfo=datetime.timezone.utc), 51),
            (datetime.datetime(2021, 5, 21, 8, 15, 6, 699902, tzinfo=datetime.timezone.utc), 59),
            (datetime.datetime(2021, 5, 21, 8, 20, 6, 776351, tzinfo=datetime.timezone.utc), 67),
            (datetime.datetime(2021, 5, 21, 8, 25, 6, 839190, tzinfo=datetime.timezone.utc), 82),
            (datetime.datetime(2021, 5, 21, 8, 30, 6, 904319, tzinfo=datetime.timezone.utc), 90),
            (datetime.datetime(2021, 5, 21, 8, 35, 6, 780839, tzinfo=datetime.timezone.utc), 109),
            (datetime.datetime(2021, 5, 21, 8, 40, 6, 707526, tzinfo=datetime.timezone.utc), 136),
            (datetime.datetime(2021, 5, 21, 8, 45, 6, 765174, tzinfo=datetime.timezone.utc), 143),
            (datetime.datetime(2021, 5, 21, 8, 50, 6, 890866, tzinfo=datetime.timezone.utc), 173),
            (datetime.datetime(2021, 5, 21, 8, 55, 6, 873454, tzinfo=datetime.timezone.utc), 152),
            (datetime.datetime(2021, 5, 21, 9, 0, 0, 0, tzinfo=datetime.timezone.utc), 152),
        ]

        integralMetricValue = integrateHourFromTimeseries(hourstart, timeseries)

        # TODO remove 1000* factor when expectedpower returns W instead of kW
        powerAvg = 1000*sum([power for (t, power) in timeseries])/len(timeseries)

        self.assertEqual(integralMetricValue, powerAvg)

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
            (datetime.datetime(2020, 12, 10, 16, 0, tzinfo=datetime.timezone.utc), 259),
            (datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc), 628),
            (datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc), 988),
            (datetime.datetime(2020, 12, 10, 19, 0, tzinfo=datetime.timezone.utc), 1348),
            (datetime.datetime(2020, 12, 10, 20, 0, tzinfo=datetime.timezone.utc), 1708),
            (datetime.datetime(2020, 12, 10, 21, 0, tzinfo=datetime.timezone.utc), 2068),
            (datetime.datetime(2020, 12, 10, 22, 0, tzinfo=datetime.timezone.utc), 2428),
            (datetime.datetime(2020, 12, 10, 23, 0, tzinfo=datetime.timezone.utc), 2788),
            (datetime.datetime(2020, 12, 11, 0, 0, tzinfo=datetime.timezone.utc), 1018),
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
            'sensor5' : [627,896],
            'sensor6' : [727,988],
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
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc)        ]

        expectedIntegrals = {
            '12345678' : [259, 565],
            '87654321' : [351, 657],
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
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc)
        ]

        expectedIntegrals = {
            '12345678' : [259, 565],
            '87654321' : [351, 657],
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
            {'sensor': 4, 'time': times[0], 'integratedIrradiation_wh_m2': 627, 'expected_energy_wh': None},
            {'sensor': 4, 'time': times[1], 'integratedIrradiation_wh_m2': 896, 'expected_energy_wh': None},
            {'sensor': 5, 'time': times[0], 'integratedIrradiation_wh_m2': 727, 'expected_energy_wh': None},
            {'sensor': 5, 'time': times[1], 'integratedIrradiation_wh_m2': 988, 'expected_energy_wh': None}
        ]

        self.assertListEqual(result, expected)

    # TODO deprecate if orm knows about views
    # or activate if getOldestTime suports python lists instead of pony queries
    def _test__getOldestTime__WithAQueryObject(self):

        plant = Plant(name='alibaba', codename='alibaba')
        sensor = SensorIrradiation(name='Alice', plant=plant)

        expectedPowerViewQuery = Path('queries/view_expected_power.sql').read_text(encoding='utf8')
        metric = 'expected_energy_wh'

        dummy = database.select(expectedPowerViewQuery)
        print(dummy)
        # TODO fix this How to convert a raw query to a pony query
        print(type(dummy))
        orm.select(r for r in dummy)

        result = getOldestTime(sensor, expectedPowerViewQuery, metric)

        self.assertIsNone(result)

    # TODO check this dummy integral of acceleration 10 to distance
    def test__integrateHourFromTimeseries(self):

        def speed(dt):
            return (datastart + dt, 50 + 10*dt.total_seconds()/3600.0)

        def distance(dt):
            return (datastart + dt, 50*dt.total_seconds()/3600 + 10*0.5*dt.total_seconds()/3600*dt.total_seconds()/3600.0)

        datastart = datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc)
        hourstart = datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc)
        dt = datetime.timedelta(minutes=5)
        # timeseries = [(hourstart + i*dt, 50 + i*10) for i in range(20)]
        timeseries = [speed(i*dt) for i in range(50)]
        print(timeseries)

        integralMetricValue = integrateHourFromTimeseries(hourstart, timeseries)

        expected = distance(hourstart-datastart+datetime.timedelta(hours=1))[1] - distance(hourstart-datastart)[1]

        # TODO remove 1000* when expectedPower is in wh
        self.assertEqual(integralMetricValue, 1000*expected)

    def test__integrateExpectedPower__NoReadings(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        orm.flush()

        integratedMetric = integrateExpectedPower()

        self.assertDictEqual(integratedMetric, {})

    def test__integrateExpectedPower__SomeReadings(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)
        orm.flush()
        self.createPlantParameters(alibaba.id, **self.parametersFlorida)

        fromDate = time.replace(hour=16, minute=0, second=0, microsecond=0)
        toDate = fromDate + datetime.timedelta(hours=2)

        result = integrateExpectedPower(fromDate, toDate)

        times = [
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 18, 0, tzinfo=datetime.timezone.utc)
        ]

        expected = {
            SensorIrradiation[4]:
            [
                (times[0], 758500),
                (times[1], 1155289),
            ],
            SensorIrradiation[5]:
            [
                (times[0], 870270),
                (times[1], 1266457),
            ]
        }

        self.assertDictEqual(result, expected)

    def test__insertHourlySensorIrradiationMetrics__updateRows(self):
        plantNS = self.samplePlantNS()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta, numreadings=20)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        fromDate = time.replace(hour=16, minute=0, second=0, microsecond=0)
        toDate = fromDate + datetime.timedelta(hours=2)

        orm.flush()

        times = [
            datetime.datetime(2020, 12, 10, 16, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc),
        ]

        sensor = Sensor.select().first()
        irradiance = {sensor: [(times[0], 10), (times[1], 20)]}

        insertHourlySensorIrradiationMetrics(irradiance, 'integratedIrradiation_wh_m2')

        expectedEnergy = {sensor: [(times[0], 30), (times[1], 40)]}

        insertHourlySensorIrradiationMetrics(expectedEnergy, 'expected_energy_wh')

        result = [r.to_dict() for r in HourlySensorIrradiationRegistry.select().order_by(1)]

        expected = [
            {'sensor': 1, 'time': times[0], 'integratedIrradiation_wh_m2': 10, 'expected_energy_wh': 30},
            {'sensor': 1, 'time': times[1], 'integratedIrradiation_wh_m2': 20, 'expected_energy_wh': 40},
        ]

        print("HourlySensor {}".format(result))
        print("ExpectedHourlySensor {}".format(expected))

        self.assertListEqual(result, expected)

        self.assertListEqual(result, expected)

    def test__plantParametersInsert(self):
        plantNS = self.samplePlantNSWithModuleParameters()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta, numreadings=20)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        numpp = PlantModuleParameters.select().count()

        self.assertEqual(numpp, 1)

    def test__selectPowerViaQuery(self):
        plantNS = self.samplePlantNSWithModuleParameters()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta, numreadings=20)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        fromDate = time.replace(hour=16, minute=0, second=0, microsecond=0)
        toDate = fromDate + datetime.timedelta(hours=2)

        orm.flush()

        expectedPowerViewQuery = Path('queries/view_expected_power.sql').read_text(encoding='utf8')

        result = database.select(expectedPowerViewQuery)

        self.assertFalse(all(r.expectedpower is None for r in result))

    @unittest.skipIf(True, "Disabled until expected power is correctly working")
    def test__computeIntegralMetrics(self):
        plantNS = self.samplePlantNSWithModuleParameters()

        alibaba = Plant(name=plantNS.name, codename=plantNS.codename)
        alibaba = alibaba.importPlant(plantNS)
        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        delta = datetime.timedelta(minutes=5)
        plantsData = self.samplePlantsData(time, delta, numreadings=20)
        Plant.insertPlantsData(plantsData)
        orm.flush()

        fromDate = time.replace(hour=16, minute=0, second=0, microsecond=0)
        toDate = fromDate + datetime.timedelta(hours=2)

        orm.flush()

        computeIntegralMetrics()

        result = [r.to_dict() for r in HourlySensorIrradiationRegistry.select().order_by(1)]

        times = [
            datetime.datetime(2020, 12, 10, 16, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 12, 10, 17, 0, tzinfo=datetime.timezone.utc),
        ]

        expected = [
            {'sensor': 2, 'time': times[0], 'integratedIrradiation_wh_m2': 225, 'expected_energy_wh': 298},
            {'sensor': 2, 'time': times[1], 'integratedIrradiation_wh_m2': 380, 'expected_energy_wh': 462},
            {'sensor': 3, 'time': times[0], 'integratedIrradiation_wh_m2': 308, 'expected_energy_wh': 392},
            {'sensor': 3, 'time': times[1], 'integratedIrradiation_wh_m2': 446, 'expected_energy_wh': 536}
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
