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
from plantmonitor.task import PonyMetricStorage

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables
from yamlns import namespace as ns
import datetime

from api_server.plantmonitor_api import api,app
import json
setupDatabase()

class Api_Test(unittest.TestCase):

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
        # orm.db_session.__enter__()

        self.client = app.test_client()

    def tearDown(self):
        orm.rollback()
        # orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()
    
    def test_Environment(self):
        #TODO will it be too late if the config is misconfigured?
        from conf import dbinfo
        self.assertEqual(dbinfo.SETTINGS_MODULE, 'conf.settings.testing')

    def test_connection(self):
        with orm.db_session:
            self.assertEqual(database.get_connection().status, 1)

    from yamlns.testutils import assertNsEqual

    def test_ApiPlant_Version(self):

        response = self.client.get('/version')
        
        self.assertEqual(response.status_code,200)
        self.assertNsEqual(response.json, """\
            version: '1.0'
        """)
    
    def __test_ApiPlant_MetricsInsert_Empty(self):

        yaml = ns.loads("""\
            """)
        plant_id = 1
            
        rv = self.client.put('/' + plant_id, yaml)
        self.assertEqual()

    def test_ApiPlant_MetricsInsert(self):

        data = ns.loads("""\
            plant: Alcolea
            version: "1.0"
            timestamp: 2020-09-27T14:00:00Z
            devices:
            - id: "SensorTemperature:temperature"
              reading:
                temperature_c: 16.0
            - id: "SensorIrradiation:irradiation1"
              reading:
                irradiation_w_m2: 16.0
            """)
        response = self.client.put('/plant/{}'.format(data.plant), data=data.dump())
        self.assertEqual(response.status_code,200)
        self.assertNsEqual(response.get_data(), data)



# vim: et sw=4 ts=4
