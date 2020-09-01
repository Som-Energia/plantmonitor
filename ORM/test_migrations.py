# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
from yamlns import namespace as ns
import datetime as dt

# from .migrations import loadDump,migrateLegacyToPony
from .migrations import (
    createPlants,
    migrateLegacyInverterTableToPony,
    migrateLegacySensorTableToPony,
    migrateLegacyMeterTableToPony,
    migrateLegacyToPony,
)

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

        from conf import dbinfo
        self.assertEqual(dbinfo.SETTINGS_MODULE, 'conf.settings.testing')

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

    def test_registryInsert_PhantomObjectDebug(self):
        with orm.db_session:
            plant = Plant(name='alcolea', codename='alcolea')
            inverter = Inverter(name='inversor1', plant=plant)

            time = dt.datetime(2020, 2, 27, 0, 45, 57, 520238)
            # time = pytz.utc.localize(time)
            time = time.replace(tzinfo=dt.timezone.utc)
            temp_inv = 190
            values = [0, 0, 492, 29998, 0, 16166, 0, 0, 0, 0, 0, 0]
            inverter.insertRegistry(*values, temp_inv_c=temp_inv, time=time)

            orm.select(r for r in InverterRegistry).show()

    def __test_migrateLegacyTableToPony_meter(self):
        tableName = 'sistema_contador'
        plantColumnName = 'location'
        deviceColumnName = 'contador'
        deviceType = 'Meter'
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyInverterTableToPony(
                    db,
                    tableName=tableName,
                    plantColumnName=plantColumnName,
                    deviceColumnName=deviceColumnName,
                    deviceType=deviceType,
                    excerpt=True
                )

    def test_migrateLegacySensorTableToPony_sensor(self):
        tableName = 'sensors'
        dataColumnName = 'irradiation_w_m2'
        plantName = 'Alcolea'
        deviceName = 'irradiation_alcolea'
        deviceType = 'SensorIrradiation'
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacySensorTableToPony(
                    db,
                    plantName=plantName,
                    tableName=tableName,
                    deviceType=deviceType,
                    dataColumnName=dataColumnName,
                    deviceName=deviceName,
                    excerpt=True
                )

        with self.createPlantmonitorDB() as db:
            # retrieve expected
            curr = db._client.cursor()
            curr.execute("select time, {} from {} limit 1;".format(dataColumnName, tableName))
            expectedFirstRegistry = list(curr.fetchone())

        expectedTime = expectedFirstRegistry[0].replace(tzinfo=dt.timezone.utc)
        expectedValue = expectedFirstRegistry[1]
        expectedMigrateRegistryList = [expectedTime, plantName, deviceName, expectedValue]

        query = SensorIrradiationRegistry.select().order_by(SensorIrradiationRegistry.time)
        migratedRegistry = query.first()
        id, time, migratedValue = list(migratedRegistry.to_dict().values())
        migratedRegistryList = [time, migratedRegistry.sensor.plant.name, migratedRegistry.sensor.name, migratedValue]

        self.assertListEqual(migratedRegistryList, expectedMigrateRegistryList)

    # TODO fix this test
    def test_migrateLegacyMeterTableToPony_checkFacilityMeter(self):
        plantName = 'Alcolea'

        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                createPlants()
                migrateLegacyMeterTableToPony(db, excerpt=True)

        dbFacilityMeter = {}
        with self.createPlantmonitorDB() as db:
            # retrieve expected
            dbFacilityMeter = db.getFacilityMeter()

        # excluded plants
        excludedFacilities = ['SomEnergia_Exiom', 'SomEnergia_Fontivsolar', 'SomEnergia_La_Florida', 'SomEnergia_Alcolea']
        dbFacilityMeter = set((f,m) for f,m in dbFacilityMeter if f not in excludedFacilities)

        q = orm.select((m.plant.codename, m.name) for m in Meter)
        print(q[:])
        ormFacilityMeter = set(q[:])
        # fetch facilitymeter data
        self.assertSetEqual(ormFacilityMeter, dbFacilityMeter)

    def test_migrateLegacyMeterTableToPony_meters(self):
        plantName = 'Alcolea'
        meterName = '501600324'

        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                createPlants()
                migrateLegacyMeterTableToPony(db, excerpt=True)

        with self.createPlantmonitorDB() as db:
            # retrieve expected
            curr = db._client.cursor()
            curr.execute("select * from sistema_contador where name = '{}' limit 1;".format(meterName))
            expectedFirstRegistry = list(curr.fetchone())

        expectedTime = expectedFirstRegistry[0].replace(tzinfo=dt.timezone.utc)
        expectedMeterName = expectedFirstRegistry[1]
        expectedValuesList = expectedFirstRegistry[2:]
        expectedMigrateRegistryList = [expectedTime, plantName, expectedMeterName] + expectedValuesList

        query = MeterRegistry.select().order_by(MeterRegistry.time)
        migratedRegistry = query.first()
        id, time, *migratedRegistrylist = list(migratedRegistry.to_dict().values())
        migratedRegistryList = [time, migratedRegistry.meter.plant.name, migratedRegistry.meter.name] + migratedRegistrylist

        self.assertListEqual(migratedRegistryList, expectedMigrateRegistryList)

    def __test_migrateLegacyTableToPony_sensor(self):
        tableName = 'sensors'
        plantColumnName = None
        deviceColumnName = None
        deviceType = 'SensorIrradiation'
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyInverterTableToPony(
                    db,
                    tableName=tableName,
                    plantColumnName=plantColumnName,
                    deviceColumnName=deviceColumnName,
                    deviceType=deviceType,
                    excerpt=True
                )

    def test_migrateLegacyInverterTableToPony_10records(self):
        # Get data to migrate
        # Assumed there on db
        numrecords = 10

        # TODO fix order by time or extract first record plant and inverter_name from pony
        plantName = 'Alcolea'
        inversorName = 'inversor1'

        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyInverterTableToPony(db, excerpt=True)

            with self.createPlantmonitorDB() as db:
                # retrieve expected
                curr = db._client.cursor()
                #curr.execute("select * from sistema_inversor where order by location, inverter_name, time desc limit 10;")
                curr.execute("select distinct on (time, location, inverter_name) * from sistema_inversor where location = '{}' and inverter_name = '{}' limit 1;".format(plantName, inversorName))
                expectedFirstRegistry = list(curr.fetchone())

            numplants     = orm.count(p for p in Plant)
            numinverters  = orm.count(i for i in Inverter)
            numregistries = orm.count(r for r in InverterRegistry)
            print("{} plants, {} inverters, {} registries".format(numplants, numinverters, numregistries))
            self.assertEqual(numinverters*numrecords, numregistries)

            expectedTime = expectedFirstRegistry[0].replace(tzinfo=dt.timezone.utc)
            expectedPlant = expectedFirstRegistry[2]
            expectedInverter = expectedFirstRegistry[1]
            expectedList = expectedFirstRegistry[8:-5]
            expectedTempInv = expectedFirstRegistry[-1]
            expectedMigrateRegistryList = [expectedTime, expectedPlant, expectedInverter] + expectedList + [expectedTempInv]

            query = orm.select(r for r in InverterRegistry if r.inverter.name == expectedInverter and r.inverter.plant.name == expectedPlant).order_by(InverterRegistry.time)
            migratedRegistry = query.first()
            id, time, *migratedRegistryList = list(migratedRegistry.to_dict().values())
            migratedRegistryList = [time, migratedRegistry.inverter.plant.name, migratedRegistry.inverter.name] + migratedRegistryList

            self.assertListEqual(migratedRegistryList, expectedMigrateRegistryList)

    def test_fullMigration_plants(self):
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyToPony(db, excerpt=True)

        expectedPlants = [
            'Alcolea',
            'Riudarenes_SM',
            'Fontivsolar I',
            'Riudarenes_BR',
            'Riudarenes_ZE',
            'Florida',
            'Matallana'
        ]

        with orm.db_session:
            plants = list(orm.select(p.name for p in Plant))

        self.assertCountEqual(plants, expectedPlants)

    def test_fullMigration_FloridaCheck(self):
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyToPony(db, excerpt=True)

        with orm.db_session:
            plants = list(orm.select(p.name for p in Plant if p.name == "La Florida"))

        self.assertListEqual(plants, [])

    def __test_migrateLegacyInverterTableToPony_DaylightSavingTime(self):
        # TODO
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyInverterTableToPony(db, excerpt=False)


    # skipped per slow, but numbers don't match 1769262 vs 1769465
    def __test_fullMigration_InverterCount_noexcerpt(self):
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyInverterTableToPony(db, excerpt=False)

        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                curr = db._client.cursor()
                #curr.execute("select * from sistema_inversor where order by location, inverter_name, time desc limit 10;")
                curr.execute("select count(distinct (time, location, inverter_name)) from sistema_inversor")
                expectednumregistries = curr.fetchone()[0]

        numregistries = orm.count(r for r in InverterRegistry)
        self.assertEqual(numregistries, expectednumregistries)

    # TODO there are repeated records 2020-02-26 15:00:00 with different energy values
    # How does distinct choose one
    def __test_fullMigration_MeterCount_noexcerpt(self):
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                createPlants()
                migrateLegacyMeterTableToPony(db, excerpt=True)

        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                curr = db._client.cursor()
                #curr.execute("select * from sistema_inversor where order by location, inverter_name, time desc limit 10;")
                curr.execute("select count(distinct (time, name)) from sistema_contador")
                expectednumregistries = curr.fetchone()[0]

        numregistries = orm.count(r for r in MeterRegistry)
        self.assertEqual(numregistries, expectednumregistries)

    def __test_fullMigration_noexcerpt(self):
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyToPony(db, excerpt=True)

        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                curr = db._client.cursor()
                #curr.execute("select * from sistema_inversor where order by location, inverter_name, time desc limit 10;")
                curr.execute("select count(distinct (time, location, inverter_name)) from sistema_inversor")
                expectednumInverterRegistries = curr.fetchone()[0]
                curr.execute("select count(*) from sistema_contador")
                expectednumMeterRegistries = curr.fetchone()[0]

        numInverterRegistries = orm.count(r for r in InverterRegistry)
        self.assertEqual(numInverterRegistries, expectednumInverterRegistries)
        numMeterRegistries = orm.count(r for r in MeterRegistry)
        self.assertEqual(numMeterRegistries, expectednumMeterRegistries)

    def test_fullMigration_excerpt(self):
        with orm.db_session:
            with self.createPlantmonitorDB() as db:
                migrateLegacyToPony(db, excerpt=True)

        # TODO more asserts
