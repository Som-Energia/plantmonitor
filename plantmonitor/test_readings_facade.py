# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

import datetime as dt

from yamlns import namespace as ns

from .readings_facade import ReadingsFacade

from pony import orm

from ORM.models import database
from ORM.models import (
    importPlants,
    exportPlants,
    Plant,
    Meter,
)
from ORM.orm_util import setupDatabase

setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)

class ReadingsFacade_Test(unittest.TestCase):

    def setUp(self):
        from conf import dbinfo
        self.assertEqual(dbinfo.SETTINGS_MODULE, 'conf.settings.testing')

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

    def samplePlantsNS(self):
        alcoleaPlantsNS = ns.loads("""\
            municipalities:
            - municipality:
                name: Figueres
                ineCode: '17066'
                countryCode: ES
                country: Spain
                regionCode: '09'
                region: Catalonia
                provinceCode: '17'
                province: Girona
            - municipality:
                name: Girona
                ineCode: '17079'
            plants:
            - plant:
                name: alcolea
                codename: SCSOM04
                description: la bonica planta
                municipality: '17066'
                meters:
                - meter:
                    name: '88300864'
                inverters:
                - inverter:
                    name: '5555'
            - plant:
                name: figuerea
                codename: Som_figuerea
                description: la bonica planta
                municipality: '17079'
                meters:
                - meter:
                    name: '9876'
                - meter:
                    name: '5432'
                inverters:
                - inverter:
                    name: '4444'
                - inverter:
                    name: '2222'
                irradiationSensors:
                - irradiationSensor:
                    name: oriol
                temperatureModuleSensors:
                - temperatureModuleSensor:
                    name: joan
                temperatureAmbientSensors:
                - temperatureAmbientSensor:
                    name: benjami
                integratedSensors:
                - integratedSensor:
                    name: david""")
        return alcoleaPlantsNS


    def samplePlantsData(self, time, dt):
        plantsData = [
              {
              "plant": "alcolea",
              "devices":
                sorted([{
                    "id": "Meter:88300864",
                    "readings": [{
                        "time": time,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                },
                {
                    "id": "Inverter:5555",
                    "readings": [{
                        "time": time,
                        "power_w": 156,
                        "energy_wh": 154,
                        "intensity_cc_mA": 222,
                        "intensity_ca_mA": 500,
                        "voltage_cc_mV": 200,
                        "voltage_ca_mV": 100,
                        "uptime_h": 15,
                        "temperature_dc": 1700,
                    }]
                }],key=lambda d: d['id']),
              },
              {
              "plant": "figuerea",
              "devices":
                sorted([{
                    "id": "Meter:9876",
                    "readings": [{
                        "time": time,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                },
                {
                    "id": "Meter:5432",
                    "readings": [{
                        "time": time,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                }],key=lambda d: d['id']),
            }]
        return plantsData

    def setupPlants(self, time):
        delta = dt.timedelta(minutes=30)
        plantsns = self.samplePlantsNS()
        importPlants(plantsns)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)

    def test_getNewMetersReadings__noMeterInERP(self):
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        self.setupPlants(time)

        r = ReadingsFacade()

        plantData = r.getNewMetersReadings()

        expectedPlantData = [
              {
              "plant": "alcolea",
              "devices":
                [{
                    "id": "Meter:1234578",
                    "readings": [],
                }],
              },
              {
              "plant": "figuerea",
              "devices":
                sorted([{
                    "id": "Meter:9876",
                    "readings": [],
                },
                {
                    "id": "Meter:5432",
                    "readings": [],
                }],key=lambda d: d['id']),
            }]

        self.assertListEqual(expectedPlantData, plantData)


    def test_getNewMetersReadings__meterInERPmanyReadings(self):
        time = dt.datetime(2018, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        self.setupPlants(time)

        r = ReadingsFacade()

        plantData = r.getNewMetersReadings()

        expectedPlantData = [
              {
              "plant": "alcolea",
              "devices":
                [{
                    "id": "Meter:88300864",
                    "readings": [{
                        "time": dt.datetime(2019,10,2,10,00,00),
                        "energy": 123,
                    }]
                }],
              },
              {
              "plant": "figuerea",
              "devices":
                sorted([{
                    "id": "Meter:9876",
                    "readings": [],
                },
                {
                    "id": "Meter:5432",
                    "readings": [],
                }],key=lambda d: d['id']),
            }]

        self.assertListEqual(expectedPlantData, plantData)

    def _test_transfer_ERP_readings_to_model(self):
        r = ReadingsFacade()

        # TODO mock measures or fake meters
        r.transfer_ERP_readings_to_model()