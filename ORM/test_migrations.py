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
    migrateLegacySensorIrradiationTableToPony,
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

from ORM.pony_manager import PonyManager


@unittest.skipIf(True, "requires legacy database")
class Migrations_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

    def createConfig(self):
        configdb = ns.load('conf/configdb_test.yaml')
        #TODO add dump database setting (if we end up not replicating it)
        configdb['psql_db'] = "testdump"
        return configdb

    def setUpORM(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        self.pony = PonyManager(envinfo.DB_CONF)

        self.pony.define_all_models()
        self.pony.binddb()

        self.pony.db.drop_all_tables(with_all_data=True)

        self.pony.db.create_tables()
        self.maxDiff=None

        # database.generate_mapping(create_tables=True)
        # orm.db_session.__enter__()

    def tearDownORM(self):
        orm.rollback()
        # orm.db_session.__exit__()
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()

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
        signature = self.pony.db.Inverter.insertRegistry.__code__.co_varnames
        expectedSignature = (
            "self",
            "power_w",
            "energy_wh",
            "intensity_cc_mA",
            "intensity_ca_mA",
            "voltage_cc_mV",
            "voltage_ca_mV",
            "uptime_h",
            "temperature_dc",
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

    def test_Plant_importExportPlant(self):
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
                    temperatureAmbientSensors:
                    - temperatureAmbientSensor:
                        name: joana
                    temperatureModuleSensors:
                    - temperatureModuleSensor:
                        name: juanpe
                    integratedSensors:
                    - integratedSensor:
                        name: voki""")

            alcoleaPlant = alcoleaPlantYAML.plants[0].plant
            alcolea = self.pony.db.Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlant)
            orm.flush()

            #TODO test the whole fixture, not just the plant data
            plantns = alcolea.exportPlant()
            self.assertNsEqual(plantns, alcoleaPlant)

    def test_db_records(self):
        # Get data to migrate
        with self.createPlantmonitorDB() as db:
            curr = db._client.cursor()

            curr.execute("select current_database()")
            db_name = curr.fetchone()[0]
            self.assertEqual(db_name, "testdump")

            curr.execute("select count(*) from sistema_inversor;")
            numrecords = curr.fetchone()[0]
            self.assertGreater(numrecords, 100)
            curr.execute('select time, inverter_name, location, "1HR", daily_energy_h, daily_energy_l from sistema_inversor;')
            records = curr.fetchmany(2)

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

    def __test_registryInsert_PhantomObjectDebug(self):
        with orm.db_session:
            plant = Plant(name='alcolea', codename='alcolea')
            inverter = Inverter(name='inversor1', plant=plant)

            time = dt.datetime(2020, 2, 27, 0, 45, 57, 520238)
            # time = pytz.utc.localize(time)
            time = time.replace(tzinfo=dt.timezone.utc)
            temp_inv = 190
            values = [0, 0, 492, 29998, 0, 16166, 0, 0, 0, 0, 0, 0]
            inverter.insertRegistry(*values, temp_inv_dc=temp_inv, time=time)

            orm.select(r for r in self.pony.db.InverterRegistry).show()

    def __test_migrateLegacyTableToPony_meter(self):
        tableName = 'sistema_contador'
        plantColumnName = 'location'
        deviceColumnName = 'contador'
        deviceType = 'Meter'
        with orm.db_session:
            migrateLegacyInverterTableToPony(
                self.createConfig(),
                tableName=tableName,
                plantColumnName=plantColumnName,
                deviceColumnName=deviceColumnName,
                deviceType=deviceType,
                excerpt=True
            )

    def test_migrateLegacySensorIrradiationTableToPony_sensor(self):
        tableName = 'sensors'
        dataColumnName = 'irradiation_w_m2'
        plantName = 'Alcolea'
        deviceName = 'irradiation_alcolea'
        deviceType = 'SensorIrradiation'

        migrateLegacySensorIrradiationTableToPony(
            self.createConfig(),
            plantName=plantName,
            deviceName=deviceName,
            excerpt=True
        )

        with self.createPlantmonitorDB() as db:
            # retrieve expected
            curr = db._client.cursor()
            curr.execute("select time, {} from {} limit 1;".format(dataColumnName, tableName))
            expectedFirstRegistry = list(curr.fetchone())

        expectedTime = expectedFirstRegistry[0].replace(tzinfo=dt.timezone.utc)
        expectedIrrValue = expectedFirstRegistry[1]
        expectedTempValue = expectedFirstRegistry[1]
        expectedMigrateRegistryList = [expectedTime, plantName, deviceName, expectedIrrValue, expectedTempValue]

        with orm.db_session:
            query = self.pony.db.SensorIrradiationRegistry.select().order_by(SensorIrradiationRegistry.time)
            migratedRegistry = query.first()
            id, time, irrValue, tempValue = list(migratedRegistry.to_dict().values())
            migratedRegistryList = [time, migratedRegistry.sensor.plant.name, migratedRegistry.sensor.name, irrValue, tempValue]

        self.assertListEqual(migratedRegistryList, expectedMigrateRegistryList)

    # @TODO deprecated integrated sensors are deprecated
    def _test_migrateLegacySensorTableToPony_sensor(self):
        tableName = 'integrated_sensors'
        dataColumnName = 'integral_irradiation_wh_m2'
        plantName = 'Alcolea'
        deviceName = 'irradiation_alcolea'
        deviceType = 'SensorIntegratedIrradiation'

        migrateLegacySensorTableToPony(
            self.createConfig(),
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

        with orm.db_session:
            query = self.pony.db.HourlySensorIrradiationRegistry.select().order_by(HourlySensorIrradiationRegistry.time)
            migratedRegistry = query.first()
            id, time, migratedValue = list(migratedRegistry.to_dict().values())
            migratedRegistryList = [time, migratedRegistry.sensor.plant.name, migratedRegistry.sensor.name, migratedValue]

        self.assertListEqual(migratedRegistryList, expectedMigrateRegistryList)

    # TODO fix this test
    def test_migrateLegacyMeterTableToPony_checkFacilityMeter(self):
        plantName = 'Alcolea'

        createPlants()
        migrateLegacyMeterTableToPony(self.createConfig(), excerpt=True)

        dbFacilityMeter = {}
        with self.createPlantmonitorDB() as db:
            # retrieve expected
            dbFacilityMeter = db.getFacilityMeter()

        # excluded plants
        excludedFacilities = ['SomEnergia_Exiom', 'SomEnergia_Fontivsolar', 'SomEnergia_La_Florida', 'SomEnergia_Alcolea']
        dbFacilityMeter = set((f,m) for f,m in dbFacilityMeter if f not in excludedFacilities)

        with orm.db_session:
            q = orm.select((m.plant.codename, m.name) for m in Meter)
            ormFacilityMeter = set(q[:])

        # fetch facilitymeter data
        self.assertSetEqual(ormFacilityMeter, dbFacilityMeter)

    def test_migrateLegacyMeterTableToPony_meters(self):
        plantName = 'Alcolea'
        meterName = '501600324'

        createPlants()
        migrateLegacyMeterTableToPony(self.createConfig(), excerpt=True)

        with self.createPlantmonitorDB() as db:
            # retrieve expected
            curr = db._client.cursor()
            curr.execute("select * from sistema_contador where name = '{}' limit 1;".format(meterName))
            expectedFirstRegistry = list(curr.fetchone())

        expectedTime = expectedFirstRegistry[0].replace(tzinfo=dt.timezone.utc)
        expectedMeterName = expectedFirstRegistry[1]
        expectedValuesList = expectedFirstRegistry[2:]
        expectedMigrateRegistryList = [expectedTime, plantName, expectedMeterName] + expectedValuesList

        with orm.db_session:
            query = self.pony.db.MeterRegistry.select().order_by(self.pony.db.MeterRegistry.time)
            migratedRegistry = query.first()

            id, time, *migratedRegistrylist = list(migratedRegistry.to_dict().values())
            migratedRegistryList = [time, migratedRegistry.meter.plant.name, migratedRegistry.meter.name] + migratedRegistrylist

        self.assertListEqual(migratedRegistryList, expectedMigrateRegistryList)

    def __test_migrateLegacyTableToPony_sensor(self):
        tableName = 'sensors'
        plantColumnName = None
        deviceColumnName = None
        deviceType = 'SensorIrradiation'

        migrateLegacyInverterTableToPony(
            self.createConfig(),
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

        migrateLegacyInverterTableToPony(self.createConfig(), excerpt=True)

        with self.createPlantmonitorDB() as db:
            # retrieve expected
            curr = db._client.cursor()
            #curr.execute("select * from sistema_inversor where order by location, inverter_name, time desc limit 10;")
            curr.execute("select distinct on (time, location, inverter_name) * from sistema_inversor where location = '{}' and inverter_name = '{}' limit 1;".format(plantName, inversorName))
            expectedFirstRegistry = list(curr.fetchone())

        with orm.db_session:
            numplants     = orm.count(p for p in self.pony.db.Plant)
            numinverters  = orm.count(i for i in self.pony.db.Inverter)
            numregistries = orm.count(r for r in self.pony.db.InverterRegistry)
            self.assertEqual(numinverters*numrecords, numregistries)

        expectedTime = expectedFirstRegistry[0].replace(tzinfo=dt.timezone.utc)
        expectedPlant = expectedFirstRegistry[2]
        expectedInverter = expectedFirstRegistry[1]
        expectedEnergy = int(round((expectedFirstRegistry[8] << 16) + expectedFirstRegistry[9]))
        expectedTempInv = expectedFirstRegistry[-1]*100
        expectedMigrateRegistryList = [expectedTime, expectedPlant, expectedInverter,expectedEnergy,expectedTempInv]

        with orm.db_session:
            query = orm.select(r for r in self.pony.db.InverterRegistry if r.inverter.name == expectedInverter and r.inverter.plant.name == expectedPlant).order_by(InverterRegistry.time)

            migratedRegistry = query.first()
            id, time, power, energy, *migratedRegistryList, temperature_dc = list(migratedRegistry.to_dict().values())
            migratedRegistryList = [time, migratedRegistry.inverter.plant.name, migratedRegistry.inverter.name, energy, temperature_dc]

        self.assertListEqual(migratedRegistryList, expectedMigrateRegistryList)

    def _test_fullMigration_plants(self):
        migrateLegacyToPony(self.createConfig(), excerpt=True)

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
            plants = list(orm.select(p.name for p in self.pony.db.Plant))

        self.assertCountEqual(plants, expectedPlants)

    def test_fullMigration_FloridaCheck(self):
        migrateLegacyToPony(self.createConfig(), excerpt=True)

        with orm.db_session:
            plants = list(orm.select(p.name for p in self.pony.db.Plant if p.name == "La Florida"))

        self.assertListEqual(plants, [])

    def __test_migrateLegacyInverterTableToPony_DaylightSavingTime(self):
        # TODO
        migrateLegacyInverterTableToPony(self.createConfig(), excerpt=False)


    # skipped per slow, but numbers don't match 1769262 vs 1769465
    def __test_fullMigration_InverterCount_noexcerpt(self):
        migrateLegacyInverterTableToPony(self.createConfig(), excerpt=False)

        with self.createPlantmonitorDB() as db:
            curr = db._client.cursor()
            #curr.execute("select * from sistema_inversor where order by location, inverter_name, time desc limit 10;")
            curr.execute("select count(distinct (time, location, inverter_name)) from sistema_inversor")
            expectednumregistries = curr.fetchone()[0]

        with orm.db_session:
            numregistries = orm.count(r for r in self.pony.db.InverterRegistry)

        self.assertEqual(numregistries, expectednumregistries)

    # TODO there are repeated records 2020-02-26 15:00:00 with different energy values
    # How does distinct choose one
    def __test_fullMigration_MeterCount_noexcerpt(self):

        migrateLegacyMeterTableToPony(self.createConfig(), excerpt=True)

        with self.createPlantmonitorDB() as db:
            curr = db._client.cursor()
            #curr.execute("select * from sistema_inversor where order by location, inverter_name, time desc limit 10;")
            curr.execute("select count(distinct (time, name)) from sistema_contador")
            expectednumregistries = curr.fetchone()[0]

        numregistries = orm.count(r for r in self.pony.db.MeterRegistry)
        self.assertEqual(numregistries, expectednumregistries)

    def __test_fullMigration_noexcerpt(self):

        migrateLegacyToPony(self.createConfig(), excerpt=False)

        with self.createPlantmonitorDB() as db:
            curr = db._client.cursor()
            #curr.execute("select * from sistema_inversor where order by location, inverter_name, time desc limit 10;")
            curr.execute("select count(distinct (time, location, inverter_name)) from sistema_inversor")
            expectednumInverterRegistries = curr.fetchone()[0]
            curr.execute("select count(*) from sistema_contador")
            expectednumMeterRegistries = curr.fetchone()[0]

        with orm.db_session:
            numInverterRegistries = orm.count(r for r in self.pony.db.InverterRegistry)
            self.assertEqual(numInverterRegistries, expectednumInverterRegistries)
            numMeterRegistries = orm.count(r for r in self.pony.db.MeterRegistry)
            self.assertEqual(numMeterRegistries, expectednumMeterRegistries)

    def test_fullMigration_excerpt(self):
        expectednumInverterRegistries = 60
        expectednumMeterRegistries = 70
        migrateLegacyToPony(self.createConfig(), excerpt=True)

        with orm.db_session:
            numInverterRegistries = orm.count(r for r in self.pony.db.InverterRegistry)
            self.assertEqual(numInverterRegistries, expectednumInverterRegistries)
            numMeterRegistries = orm.count(r for r in self.pony.db.MeterRegistry)
            self.assertEqual(numMeterRegistries, expectednumMeterRegistries)
