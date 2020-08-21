# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
from yamlns import namespace as ns
import datetime as dt

from .migrations import loadDump,migrateLegacyToPony

from meteologica.plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBMock,
    PlantmonitorDBError,
)

from meteologica.utils import todt

from pathlib import Path

from pony import orm

from .models import database
from .models import (
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

from .orm_util import setupDatabase, getTablesToTimescale, timescaleTables

setupDatabase()

class Migrations_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

    def createConfig(self):
        configdb = ns.load('conf/configdb_test.yaml')
        #TODO add dump database setting (if we end up not replicating it)
        configdb['psql_db'] = "testdump"
        return configdb

    def setUpORM(self):

        from conf import config
        self.assertEqual(config.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        database.drop_all_tables(with_all_data=True)

        self.maxDiff=None
        # orm.set_sql_debug(True)

        database.create_tables()

        # database.generate_mapping(create_tables=True)
        orm.db_session.__enter__()

    def tearDownORM(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()

    def setUp(self):
        # configdb = self.createConfig()
        # dumpfile = configdb['dumpfile']
        # print("dumpfile: {}".format(dumpfile))
        # PlantmonitorDB(configdb).demoDBsetupFromDump(configdb, dumpfile)
        self.setUpORM()

    def tearDown(self):
        # configdb = self.createConfig()
        # PlantmonitorDB(configdb).dropDatabase(configdb)
        self.tearDownORM()

    def createPlantmonitorDB(self):
        configdb = self.createConfig()
        return PlantmonitorDB(configdb)

    def mainFacility(self):
        return 'SomEnergia_Amapola'

    def __test_playground(self):
        self.assertTrue(False)

    def __test_loadDump(self):
        # Get data to migrate
        with self.createPlantmonitorDB() as db:
            dumpdir = "./dumpPlantmonitor"
            excluded = ['facility_meter', 'teoric_energy', 'integrated_sensors']

            loadDump(db, dumpdir, excluded)

            # TODO check data
            cur = db._client.cursor()
            cur.execute("select count(*) from facility_meter;")
            facilityMeterCountReg = cur.fetchall()
            cur.execute("select count(*) from integrated_sensors;")
            integratedSensorsCountReg = cur.fetchall()
            self.assertEqual(facilityMeterCountReg, 123)
            self.assertEqual(integratedSensorsCountReg, 123)

    def test_ormSetup(self):
        with orm.db_session:

            alcoleaPlantYAML = ns.loads("""\
                plants:
                - plant:
                    name: alcolea
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
                    temperatureSensors:
                    - temperatureSensor:
                        name: joana
                    integratedSensors:
                    - integratedSensor:
                        name: voki""")

            alcoleaPlant = alcoleaPlantYAML.plants[0].plant
            alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlantYAML)

        #TODO test the whole fixture, not just the plant data
        plantns = alcolea.exportPlant()
        self.assertNsEqual(plantns, alcoleaPlantYAML)

    def test_db_records(self):
        # Get data to migrate
        with self.createPlantmonitorDB() as db:
            curr = db._client.cursor()

            curr.execute("select current_database()")
            db_name = curr.fetchone()[0]
            self.assertEqual(db_name, "testdump")

            curr.execute("select count(*) from sistema_inversor;")
            numrecords = curr.fetchone()[0]
            print(numrecords)
            self.assertGreater(numrecords, 100)
            curr.execute('select time, inverter_name, location, "1HR", daily_energy_h, daily_energy_l from sistema_inversor;')
            records = curr.fetchmany(2)
            print(len(records))
            print(records)
            
            # WARNING fetchall does not work on timescale tables / ultrabig tables
            # we might need server-side cursors
            # https://www.psycopg.org/docs/usage.html#server-side-cursors
            #curr.fetchall()

            self.assertEqual(
                records,
                [
                    (dt.datetime(2020, 2, 27, 0, 0, 2, 958223), 'inversor1', 'Florida', None, 0, 0),
                    (dt.datetime(2020, 2, 27, 0, 0, 3, 152901), 'inversor2', 'Florida', None, 0, 0)
                ]
            )


    def test_migrateLegacyToPony_OneRecord(self):
        # Get data to migrate
        with self.createPlantmonitorDB() as db:
            curr = db._client.cursor()

            # TODO insert to ponyORM


            # TODO Check that data has been insert

            self.assertTrue(True)
