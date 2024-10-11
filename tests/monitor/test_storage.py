import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

from pony import orm

import datetime

from plantmonitor.ORM.pony_manager import PonyManager

from plantmonitor.monitor.storage import PonyMetricStorage, ApiMetricStorage

from yamlns import namespace as ns
import datetime

# Test against a fully functioning api
from multiprocessing import Process
import uvicorn
import time
import requests


class ApiClient_Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo
        assert envinfo.SETTINGS_MODULE == 'conf.settings.testing'

        cls.pony = PonyManager(envinfo.DB_CONF)

        cls.pony.define_all_models()
        cls.pony.binddb(create_tables=True)

        from plantmonitor.api_server.plantmonitor_api import api

        cls.api = api

    @classmethod
    def tearDownClass(cls):
        cls.pony.db.drop_all_tables(with_all_data=True)
        cls.pony.db.disconnect()

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        self.pony.db.drop_all_tables(with_all_data=True)

        self.pony.db.create_tables()

        orm.db_session.__enter__()

        # log_level = "debug"
        log_level = "warning"

        # create api and launch local test server
        self.proc = Process(target=uvicorn.run,
                    args=(self.api,),
                    kwargs={
                        "host": "127.0.0.1",
                        "port": self.apiPort(),
                        "log_level": log_level},
                    daemon=True)
        self.proc.start()
        time.sleep(0.1)

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        self.proc.terminate()

    def apiPort(self):
        return 5000

    def createApiClient(self):
        #TODO configfile
        config = {
            "api_url":"http://localhost:{}".format(self.apiPort()),
            "version":"1.0",
        }
        return ApiMetricStorage(config)

    def createPlantDict(self):
        plant_name = 'SomEnergia_Alibaba'
        inverter_name = 'Mary'
        metrics = ns([
            ('daily_energy_h_wh', 0),
            ('daily_energy_l_wh', 17556),
            ('e_total_h_wh', 566),
            ('e_total_l_wh', 49213),
            ('h_total_h_h', 0),
            ('h_total_l_h', 18827),
            ('pac_r_w', 0),
            ('pac_s_w', 0),
            ('pac_t_w', 0),
            ('powerreactive_t_v', 0),
            ('powerreactive_r_v', 0),
            ('powerreactive_s_v', 0),
            ('temp_inv_dc', 320),
            ('time', datetime.datetime.now(datetime.timezone.utc)),
        ])

        return plant_name, inverter_name, metrics

    def samplePlantNS(self):
        plantNS = ns.loads("""\
                name: alibaba
                codename: SCSOM04
                description: la bonica planta
                inverters:
                - inverter:
                    name: '5555'
                temperatureAmbientSensors:
                - temperatureAmbientSensor:
                    name: benjami""")
        return plantNS

    def samplePlantData(self, time):
        plantData = {
              "plant": "alibaba",
              "devices":
                sorted([{
                    "id": "Meter:1234578",
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
                    "id": "SensorTemperatureAmbient:5555",
                    "readings": [{
                        "time": time,
                        "temperature_dc": 170,
                    }]
                }],key=lambda d: d['id']),
              }
        return plantData

    def createPlant(self):
        plantNS = self.samplePlantNS()

        alibaba = self.pony.db.Plant(name=plantNS.name, codename=plantNS.codename, description=plantNS.description)
        alibaba = alibaba.importPlant(plantNS)
        orm.flush()
        # do we need to add some data?
        # time = datetime.datetime.now(datetime.timezone.utc)
        # plant_data = self.samplePlantData(time)

        # storage = PonyMetricStorage()
        # storage.insertPlantData(plant_data)


    def test__api_hello_version(self):
        response = requests.get("http://localhost:{}/version".format(self.apiPort()))
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(response.json(),{"version":"1.0"})

    def test_Environment(self):
        #TODO will it be too late if the config is misconfigured?
        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

    def test__ApiMetricStorage__storeInverterMeasures(self):
        plant_name, inverter_name, inverter_registers = self.createPlantDict()

        apiClient = self.createApiClient()
        result = apiClient.storeInverterMeasures(plant_name, inverter_name, inverter_registers)

        self.assertTrue(result)

    def test__ApiMetricStorage_insertPlantData__empty(self):
        plant_data = {
            "plant": "Alibaba",
            "version": "1.0",
        }

        apiClient = self.createApiClient()
        result = apiClient.insertPlantData(plant_data)

        expectedError = {"detail":[{"loc":["body","time"],"msg":"field required","type":"value_error.missing"},{"loc":["body","devices"],"msg":"field required","type":"value_error.missing"}]}

        self.assertDictEqual(result, expectedError)

    def test__ApiMetricStorage_insertPlantData__someData(self):
        self.createPlant()
        time = datetime.datetime.now(datetime.timezone.utc)

        plant_data = {
            "plant": "alibaba",
            "version": "1.0",
            "time": time.isoformat(),
            "devices":
            [{
                "id": "SensorTemperatureAmbient:benjami",
                "readings":
                [{
                        "temperature_dc": 120,
                        "time": time.isoformat(),
                }]
            }]
        }

        apiClient = self.createApiClient()
        result = apiClient.insertPlantData(plant_data)
        print(result)
        # expectedResponse = {"detail":[{"loc":["body","time"],"msg":"OK","type":"None"},{"loc":["body","devices"],"msg":"OK","type":"None"}]}

        self.assertDictEqual(result, plant_data)


class Storage_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        self.pony = PonyManager(envinfo.DB_CONF)

        self.pony.define_all_models()
        self.pony.binddb(create_tables=True)

        self.pony.db.drop_all_tables(with_all_data=True)

        self.storage = PonyMetricStorage(self.pony.db)

        self.maxDiff=None
        # orm.set_sql_debug(True)

        self.pony.db.create_tables()

        # database.generate_mapping(create_tables=True)
        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()

    def samplePlantData(self, time):
        plantData = {
            "plant": "alibaba",
            "devices":
                sorted([{
                    "id": "Meter:1234578",
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
                    "id": "SensorTemperatureAmbient:5555",
                    "readings": [{
                        "time": time,
                        "temperature_dc": 170,
                    }]
                }],key=lambda d: d['id']),
            }
        return plantData

    def test_Environment(self):
        #TODO will it be too late if the config is misconfigured?
        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

    def test_connection(self):
        with orm.db_session:
            self.assertEqual(self.pony.db.get_connection().status, 1)

    def test__datetimeToStr(self):
        time = datetime.datetime.now(datetime.timezone.utc)
        plant_data = self.samplePlantData(time)
        ApiMetricStorage.datetimeToStr(plant_data)
        expected_plant_data = self.samplePlantData(time.isoformat())

        self.assertDictEqual(plant_data, expected_plant_data)

    #TODO deprecated as soon as we switch plant_data
    # test instead registries_to_plant_data in task.py
    def __test_PublishOrmOneInverterRegistry(self):
        plant_name = 'SomEnergia_Alibaba'
        inverter_name = 'Mary'
        with orm.db_session:
            alcolea = self.pony.db.Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = self.pony.db.Inverter(name=inverter_name, plant=alcolea)
            metrics = ns([
                    ('daily_energy_h_wh', 0),
                    ('daily_energy_l_wh', 17556),
                    ('e_total_h_wh', 566),
                    ('e_total_l_wh', 49213),
                    ('h_total_h_h', 0),
                    ('h_total_l_h', 18827),
                    ('pac_r_w', 0),
                    ('pac_s_w', 0),
                    ('pac_t_w', 0),
                    ('powerreactive_t_v', 0),
                    ('powerreactive_r_v', 0),
                    ('powerreactive_s_v', 0),
                    ('temp_inv_dc', 320),
                    ('time', datetime.datetime.now(datetime.timezone.utc)),
                    # Sensors registers  obtained from inverters
                    ('probe1value', 443),
                    ('probe2value', 220),
                    ('probe3value', 0),
                    ('probe4value', 0),
                    ])

            self.storage.storeInverterMeasures(
                plant_name, inverter_name, metrics)

            metrics.pop('probe1value')
            metrics.pop('probe2value')
            metrics.pop('probe3value')
            metrics.pop('probe4value')

            expectedRegistry = dict(metrics)
            expectedRegistry['inverter'] = inverter.id

            inverterReadings = self.storage.inverterReadings()
            self.assertEqual(inverterReadings, [expectedRegistry])

    def __test_PublishOrmIfInverterNotExist(self):
        inverter_name = 'Alice'
        plant_name = 'SomEnergia_Alibaba'
        with orm.db_session:
            alcolea = self.pony.db.Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = self.pony.db.Inverter(name=inverter_name, plant=alcolea)
            metrics = ns([
                ('daily_energy_h_wh', 0),
                ('daily_energy_l_wh', 17556),
                ('e_total_h_wh', 566),
                ('e_total_l_wh', 49213),
                ('h_total_h_h', 0),
                ('h_total_l_h', 18827),
                ('pac_r_w', 0),
                ('pac_s_w', 0),
                ('pac_t_w', 0),
                ('powerreactive_t_v', 0),
                ('powerreactive_r_v', 0),
                ('powerreactive_s_v', 0),
                ('temp_inv_dc', 320),
                ('time', datetime.datetime.now(datetime.timezone.utc)),
                # Sensors registers  obtained from inverters
                ('probe1value', 443),
                ('probe2value', 220),
                ('probe3value', 0),
                ('probe4value', 0),
                ])
            self.storage.storeInverterMeasures(
                plant_name, "UnknownInverter", metrics)

            self.assertListEqual(self.storage.inverterReadings(), [])

    def __test_PublishOrmIfPlantNotExist(self):
        inverter_name = 'Alice'
        plant_name = 'SomEnergia_Alibaba'
        with orm.db_session:
            alcolea = self.pony.db.Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = self.pony.db.Inverter(name=inverter_name, plant=alcolea)
            metrics = ns([
                ('daily_energy_h_wh', 0),
                ('daily_energy_l_wh', 17556),
                ('e_total_h_wh', 566),
                ('e_total_l_wh', 49213),
                ('h_total_h_h', 0),
                ('h_total_l_h', 18827),
                ('pac_r_w', 0),
                ('pac_s_w', 0),
                ('pac_t_w', 0),
                ('powerreactive_t_v', 0),
                ('powerreactive_r_v', 0),
                ('powerreactive_s_v', 0),
                ('temp_inv_dc', 320),
                ('time', datetime.datetime.now(datetime.timezone.utc)),
                # Sensors registers  obtained from inverters
                ('probe1value', 443),
                ('probe2value', 220),
                ('probe3value', 0),
                ('probe4value', 0),
                ])
            self.storage.storeInverterMeasures(
                'UnknownPlant', inverter_name, metrics)

            self.assertListEqual(self.storage.inverterReadings(), [])

    def test__PonyMetricStorage_insertPlantData__storeTemperatureSensor(self):
        sensor_name = 'Alice'
        plant_name = 'SomEnergia_Alibaba'
        time = datetime.datetime.now(datetime.timezone.utc)

        with orm.db_session:
            alcolea = self.pony.db.Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            sensor = self.pony.db.SensorTemperatureAmbient(name=sensor_name, plant=alcolea)
            orm.flush()
            plant_data = {
                "plant": plant_name,
                "version": "1.0",
                "time": time.isoformat(), #consider using fastapi.jsonable_encoder
                "devices":
                [{
                    "id": "SensorTemperatureAmbient:Alice",
                    "readings":
                    [{
                        "temperature_dc": 120,
                        "time": time,
                    }]
                }]
            }
            self.storage.insertPlantData(plant_data)
            orm.flush()

            expected_plant_data = {
                'plant': plant_name,
                'devices':
                [{
                    'id': 'SensorTemperatureAmbient:Alice',
                    'readings':
                    [{
                        "temperature_dc": 120,
                        "time": time,
                    }]
                }],
            }
            self.assertDictEqual(self.storage.plantData(plant_name), expected_plant_data)

    def test__PonyMetricStorage_insertPlantData__storeTemperatureSensorWithoutTemperature(self):
        sensor_name = 'Alice'
        plant_name = 'SomEnergia_Alibaba'
        time = datetime.datetime.now(datetime.timezone.utc)

        with orm.db_session:
            alcolea = self.pony.db.Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            sensor = self.pony.db.SensorTemperatureAmbient(name=sensor_name, plant=alcolea)
            orm.flush()
            plant_data = {
                "plant": plant_name,
                "version": "1.0",
                "time": time.isoformat(), #consider using fastapi.jsonable_encoder
                "devices":
                [{
                    "id": "SensorTemperatureAmbient:Alice",
                    "readings":
                    [{
                        "temperature_dc": None,
                        "time": time,
                    }]
                }]
            }
            self.storage.insertPlantData(plant_data)
            orm.flush()

            expected_plant_data = {
                'plant': plant_name,
                'devices': [{'id': 'SensorTemperatureAmbient:Alice', 'readings':
                    [{
                        "temperature_dc": None,
                        "time": time,
                    }]
                }],
            }

            self.assertDictEqual(self.storage.plantData(plant_name), expected_plant_data)



# vim: et ts=4 sw=4
