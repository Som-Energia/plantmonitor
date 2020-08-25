# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
from yamlns import namespace as ns
import datetime as dt

# from .migrations import loadDump,migrateLegacyToPony
from .migrations import migrateLegacyInverterTableToPony,migrateLegacyToPony

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

    def test_insertInverterRegistrySignature(self):
        #if the insertInvertRegistrySignature changes, the migration code has to
        # change as well, given that it relies on column order
        signature = Inverter.insertRegistry.__code__.co_varnames
        expectedSignature = (
            "self",
            "daily_energy_h_wh",
            "daily_energy_l_wh",
            "e_total_h_wh",
            "e_total_l_wh",
            "h_total_h_h",
            "h_total_l_h",
            "pac_r_w",
            "pac_s_w",
            "pac_t_w",
            "powerreactive_t_v",
            "powerreactive_r_v",
            "powerreactive_s_v",
            "temp_inv_c",
            "time",
        )
        self.assertTupleEqual(expectedSignature, signature)

    def test_tableInverterColumns(self):
        # if the insertInvertRegistrySignature changes, the migration code has to
        # change as well, given that it relies on column order
        with self.createPlantmonitorDB() as db:
            cur = db._client.cursor()
            cur.execute('select * from sistema_inversor limit 1;')
            colNames = [c[0] for c in cur.description]
            expectedColnames = [
                'time', 'inverter_name', 'location', '1HR', '1HR0', '2HR2', '3HR3',
                '4HR4', 'daily_energy_h', 'daily_energy_l', 'e_total_h', 'e_total_l',
                'h_total_h', 'h_total_l', 'pac_r', 'pac_s', 'pac_t', 'powereactive_t',
                'powerreactive_r', 'powerreactive_s', 'probe1value', 'probe2value',
                'probe3value', 'probe4value', 'temp_inv'
            ]
            self.assertListEqual(expectedColnames, colNames)

    def __test_orderbytime(self):
        # pg_dump pg_restore fails to restore the timescale time index
        with self.createPlantmonitorDB() as db:
            cur = db._client.cursor()
            cur.execute("select time from sistema_inversor order by time asc limit 1;")
            records = cur.fetchall()

            self.assertEqual(len(records), 1)

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

    def test_migrateLegacyInverterTableToPony_1000records(self):
        # Get data to migrate
        # Assumed there on db
        numrecords=1000
        # TODO insert to ponyORM
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyInverterTableToPony(db, numrecords=numrecords)

            numplants     = orm.count(p for p in Plant)
            numinverters  = orm.count(i for i in Inverter)
            numregistries = orm.count(r for r in InverterRegistry)
            print("{} plants, {} inverters, {} registries".format(numplants, numinverters, numregistries))

            self.assertEqual(numrecords, numregistries)
