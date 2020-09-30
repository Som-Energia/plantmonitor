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
from plantmonitor.task import publish_orm

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables
from yamlns import namespace as ns
import datetime

setupDatabase()

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
        with orm.db_session:
            plant_name = 'SomEnergia_Alcolea'
            inverter_name = 'Mary'
            alcolea = Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = Inverter(name=inverter_name, plant=alcolea)
            metrics = ([
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
                ('time', datetime.datetime.now(datetime.timezone.utc))
                # Sensors registers  obtained from inverters           
                # ('probe1value', 443),
                # ('probe2value', 220),
                # ('probe3value', 0),
                # ('probe4value', 0),
                ])
        publish_orm(plant_name, inverter_name, metrics)
        expectedRegistry = dict(metrics)
        expectedRegistry['inverter'] = 1
        with orm.db_session:
            oneRegistry = InverterRegistry[1,expectedRegistry['time']]
            oneRegistryList = oneRegistry.to_dict()
            self.assertDictEqual(expectedRegistry, oneRegistryList)

    def test_PublishOrmIfInverterNotExist(self):
        with orm.db_session:
            plant_name = 'SomEnergia_Alcolea'
            inverter_name = 'Inverter1'
            alcolea = Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = Inverter(name=inverter_name, plant=alcolea)
            metrics = ([
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
                ('time', datetime.datetime.now(datetime.timezone.utc))
                # Sensors registers  obtained from inverters           
                # ('probe1value', 443),
                # ('probe2value', 220),
                # ('probe3value', 0),
                # ('probe4value', 0),
                ])
        publish_orm(plant_name, 'UnknownInverter', metrics)
        
        with orm.db_session:
            allInverterRegistries = orm.select(c for c in InverterRegistry)
            allInverterRegistriesList = list(allInverterRegistries)
            self.assertListEqual([], allInverterRegistriesList)
        
    def test_PublishOrmIfPlantNotExist(self):
        with orm.db_session:
            plant_name = 'SomEnergia_Alcolea'
            inverter_name = 'Alice'
            alcolea = Plant(name=plant_name,  codename='SOMSC01', description='descripción de planta')
            inverter = Inverter(name=inverter_name, plant=alcolea)
            metrics = ([
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
                ('time', datetime.datetime.now(datetime.timezone.utc))
                # Sensors registers  obtained from inverters           
                # ('probe1value', 443),
                # ('probe2value', 220),
                # ('probe3value', 0),
                # ('probe4value', 0),
                ])
        publish_orm('UnknownPlant', inverter_name, metrics)
        
        with orm.db_session:
            allInverterRegistries = orm.select(c for c in InverterRegistry)
            allInverterRegistriesList = list(allInverterRegistries)
            self.assertListEqual([], allInverterRegistriesList)
