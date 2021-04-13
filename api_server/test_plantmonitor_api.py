import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import datetime
from yamlns import namespace as ns
from fastapi.testclient import TestClient
from pony import orm
import unittest
import json

from ORM.models import database
from ORM.models import (
    importPlants,
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
from plantmonitor.task import PonyMetricStorage

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables

from .plantmonitor_api import api

setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)

class Api_Test(unittest.TestCase):

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        database.drop_all_tables(with_all_data=True)

        self.maxDiff=None
        # orm.set_sql_debug(True)

        database.create_tables()

        # database.generate_mapping(create_tables=True)
        # orm.db_session.__enter__()

        self.client = TestClient(api)

    def tearDown(self):
        orm.rollback()
        # orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()

    def setUpPlant(self):
        plantsns = ns.loads("""\
            plants:
            - plant:
                name: Alcolea
                codename: SCSOM04
                description: la bonica planta
                inverters:
                - inverter:
                    name: 'inversor1'
            """)

        importPlants(plantsns)

    def test_Environment(self):
        #TODO will it be too late if the config is misconfigured?
        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

    def test_connection(self):
        with orm.db_session:
            self.assertEqual(database.get_connection().status, 1)

    def test__api_version(self):

        response = self.client.get('/version')

        self.assertEqual(response.status_code,200)
        self.assertDictEqual(response.json(), {"version": "1.0"})

    def __test__api_plantReadings__Empty(self):

        emptyplant = {}
        plant_id = 1

        rv = self.client.put('/' + plant_id, emptyplant)

    def test__api_putPlantReadings__endpoint_response(self):

        time = datetime.datetime.now(datetime.timezone.utc)

        data = {
            "plant": "Alcolea",
            "version": "1.0",
            "time": time.isoformat(),
            "devices":
            [{
                "id": "Inverter:inversor1",
                "readings":
                [{
                    'power_w': 10,
                    'energy_wh': 10,
                    'intensity_cc_mA': 10,
                    'intensity_ca_mA': 10,
                    'voltage_cc_mV': 10,
                    'voltage_ca_mV': 10,
                    'uptime_h': 10,
                    'temperature_dc': 10,
                }]
            }]
        }

        with orm.db_session:
            self.setUpPlant()

            response = self.client.put('/plant/{}/readings'.format(data['plant']), json=data)

            # data["devices"][0]["readings"][0]["time"] = time.isoformat()
            self.assertDictEqual(response.json(), data)
            self.assertEqual(response.status_code, 200)

    def test__api_putPlantReadings__database_insert(self):

        time = datetime.datetime.now(datetime.timezone.utc)

        data = {
            "plant": "Alcolea",
            "version": "1.0",
            "time": time.isoformat(), #consider using fastapi.jsonable_encoder
            "devices":
            [{
                "id": "Inverter:inversor1",
                "readings":
                [{
                    'power_w': 10,
                    'energy_wh': 10,
                    'intensity_cc_mA': 10,
                    'intensity_ca_mA': 10,
                    'voltage_cc_mV': 10,
                    'voltage_ca_mV': 10,
                    'uptime_h': 10,
                    'temperature_dc': 10,
                }]
            }]
        }

        with orm.db_session:
            self.setUpPlant()

            response = self.client.put('/plant/{}/readings'.format(data['plant']), json=data)
            self.assertEqual(response.status_code, 200)
            # check reading content
            storage = PonyMetricStorage()
            readings = storage.inverterReadings()
            self.assertListEqual(
                readings,
                [{
                    "inverter" : 1,
                    'power_w': 10,
                    'energy_wh': 10,
                    'intensity_cc_mA': 10,
                    'intensity_ca_mA': 10,
                    'voltage_cc_mV': 10,
                    'voltage_ca_mV': 10,
                    'uptime_h': 10,
                    'temperature_dc': 10,
                    "time": time,
                }]
            )

            # check inverter content
            inverter_fk = readings[0]["inverter"]
            inverter = storage.inverter(inverter_fk)

            _,device_name = data["devices"][0]["id"].split(":")
            self.assertEqual(inverter["name"], device_name)

    def test__api_putPlantReadings__time_reading(self):

        time = datetime.datetime.now(datetime.timezone.utc)
        thermoname = "thermometer1"
        data = {
            "plant": "Alcolea",
            "version": "1.0",
            "time": time.isoformat(), #consider using fastapi.jsonable_encoder
            "devices":
            [{
                "id": "SensorTemperatureAmbient:{}".format(thermoname),
                "readings":
                [{
                    "temperature_dc": 120,
                    "time": time.isoformat(),
                }]
            }]
        }

        expecteddata = {
            "plant": "Alcolea",
            "devices":
            [{
                "id": "Inverter:inversor1",
                "readings": []
            },
            {
                "id": "SensorTemperatureAmbient:{}".format(thermoname),
                "readings":
                [{
                    "temperature_dc": 120,
                    "time": time,
                }]
            }]
        }

        with orm.db_session:
            self.setUpPlant()
            plant_name = data['plant']
            SensorTemperatureAmbient(plant=Plant.get(name=plant_name), name=thermoname)

            response = self.client.put('/plant/{}/readings'.format(plant_name), json=data)

            # check reading content
            storage = PonyMetricStorage()
            plantdata = storage.plantData(plant_name)

            self.assertDictEqual(
                plantdata,
                expecteddata
            )


    def test__api_putPlantReadings__None_reading(self):

        time = datetime.datetime.now(datetime.timezone.utc)
        thermoname = "thermometer1"
        data = {
            "plant": "Alcolea",
            "version": "1.0",
            "time": time.isoformat(), #consider using fastapi.jsonable_encoder
            "devices":
            [{
                "id": "SensorTemperatureAmbient:{}".format(thermoname),
                "readings":
                [{
                    "temperature_dc": None,
                    "time": time.isoformat(),
                }]
            }]
        }

        expecteddata = {
            "plant": "Alcolea",
            "devices":
            [{
                "id": "Inverter:inversor1",
                "readings": []
            },
            {
                "id": "SensorTemperatureAmbient:{}".format(thermoname),
                "readings":
                [{
                    "temperature_dc": None,
                    "time": time,
                }]
            }]
        }

        with orm.db_session:
            self.setUpPlant()
            plant_name = data['plant']
            SensorTemperatureAmbient(plant=Plant.get(name=plant_name), name=thermoname)

            response = self.client.put('/plant/{}/readings'.format(plant_name), json=data)
            print(response.text)

            # check reading content
            storage = PonyMetricStorage()
            plantdata = storage.plantData(plant_name)

            self.assertDictEqual(
                plantdata,
                expecteddata
            )



# vim: et sw=4 ts=4
