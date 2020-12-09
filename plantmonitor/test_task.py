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
    SensorTemperature,
    SensorIrradiationRegistry,
    SensorTemperatureRegistry,
    IntegratedIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)
from plantmonitor.task import PonyMetricStorage, ApiMetricStorage

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables
from yamlns import namespace as ns
import datetime

# Test against a fully functioning api
from api_server.plantmonitor_api import api
from multiprocessing import Process
import uvicorn
import time
import requests


setupDatabase()

class ApiClient_Test(unittest.TestCase):

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

        # create api and launch local test server
        self.proc = Process(target=uvicorn.run,
                    args=(api,),
                    kwargs={
                        "host": "127.0.0.1",
                        "port": self.apiPort(),
                        "log_level": "warning"},
                    daemon=True)
        self.proc.start()
        time.sleep(0.1) 

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()
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
        plant_name = 'SomEnergia_Alcolea'
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
            ('temp_inv_c', 320),
            ('time', datetime.datetime.now(datetime.timezone.utc)),
        ])

        return plant_name, inverter_name, metrics

    def createPlant(self):
        plant_name = 'SomEnergia_Alcolea'
        inverter_name = 'Mary'
        with orm.db_session:
            alcolea = Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = Inverter(name=inverter_name, plant=alcolea)
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
                    ('temp_inv_c', 320),
                    ('time', datetime.datetime.now(datetime.timezone.utc)),
                    ])

            storage = PonyMetricStorage()
            storage.storeInverterMeasures(plant_name, inverter_name, metrics)
 
    def test__api_hello_version(self):
        response = requests.get("http://localhost:{}/version".self.apiPort())
        self.assertEqual(response.status_code, 200)
        self.assertDictEqual(response.json(),{"version":"1.0"})

    def test_Environment(self):
        #TODO will it be too late if the config is misconfigured?
        from conf import dbinfo
        self.assertEqual(dbinfo.SETTINGS_MODULE, 'conf.settings.testing')

    def test__ApiMetricStorage__storeInverterMeasures(self):
        plant_name, inverter_name, inverter_registers = self.createPlantDict()
        
        apiClient = self.createApiClient()
        result = apiClient.storeInverterMeasures(plant_name, inverter_name, inverter_registers)

        self.assertTrue(result)

    def __test__ApiMetricStorage__storeInverterMeasures(self):
        plant_name, inverter_name, inverter_registers = self.createPlantDict()
           
        apiClient = self.createApiClient()
        result = apiClient.storeInverterMeasures(plant_name, inverter_name, inverter_registers)

        readings = apiClient.getInverterMeasures(plant_name, inverter_name)

        self.assertDictEqual(inverter_registers, readings)


class ORMSetup_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

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
 
    def test_Environment(self):
        #TODO will it be too late if the config is misconfigured?
        from conf import dbinfo
        self.assertEqual(dbinfo.SETTINGS_MODULE, 'conf.settings.testing')

    def test_connection(self):
        with orm.db_session:
            self.assertEqual(database.get_connection().status, 1)

    def test_PublishOrmOneInverterRegistry(self):
        plant_name = 'SomEnergia_Alcolea'
        inverter_name = 'Mary'
        with orm.db_session:
            alcolea = Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = Inverter(name=inverter_name, plant=alcolea)
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
                    ('temp_inv_c', 320),
                    ('time', datetime.datetime.now(datetime.timezone.utc)),
                    # Sensors registers  obtained from inverters
                    ('probe1value', 443),
                    ('probe2value', 220),
                    ('probe3value', 0),
                    ('probe4value', 0),
                    ])

            storage = PonyMetricStorage()
            storage.storeInverterMeasures(
                plant_name, inverter_name, metrics)
 
            metrics.pop('probe1value')
            metrics.pop('probe2value')
            metrics.pop('probe3value')
            metrics.pop('probe4value')
 
            expectedRegistry = dict(metrics)
            expectedRegistry['inverter'] = inverter.id

            inverterReadings = storage.inverterReadings()
            self.assertEqual(inverterReadings, [expectedRegistry])

    def test_PublishOrmIfInverterNotExist(self):
        inverter_name = 'Alice'
        plant_name = 'SomEnergia_Alcolea'
        with orm.db_session:
            alcolea = Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = Inverter(name=inverter_name, plant=alcolea)
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
                ('temp_inv_c', 320),
                ('time', datetime.datetime.now(datetime.timezone.utc)),
                # Sensors registers  obtained from inverters
                ('probe1value', 443),
                ('probe2value', 220),
                ('probe3value', 0),
                ('probe4value', 0),
                ])
            storage = PonyMetricStorage()
            storage.storeInverterMeasures(
                plant_name, "UnknownInverter", metrics)

            self.assertListEqual(storage.inverterReadings(), [])
 
    def test_PublishOrmIfPlantNotExist(self):
        inverter_name = 'Alice'
        plant_name = 'SomEnergia_Alcolea'
        with orm.db_session:
            alcolea = Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = Inverter(name=inverter_name, plant=alcolea)
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
                ('temp_inv_c', 320),
                ('time', datetime.datetime.now(datetime.timezone.utc)),
                # Sensors registers  obtained from inverters
                ('probe1value', 443),
                ('probe2value', 220),
                ('probe3value', 0),
                ('probe4value', 0),
                ])
            storage = PonyMetricStorage()
            storage.storeInverterMeasures(
                'UnknownPlant', inverter_name, metrics)
 
            self.assertListEqual(storage.inverterReadings(), [])

# vim: et ts=4 sw=4
