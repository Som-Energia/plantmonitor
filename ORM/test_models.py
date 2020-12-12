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

class Models_Test(unittest.TestCase):

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

    def samplePlantNS(self):
        alcoleaPlantNS = ns.loads("""\
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
        return alcoleaPlantNS

    def test_Plant_importExportPlant(self):
        with orm.db_session:

            alcoleaPlantNS = self.samplePlantNS()

            alcoleaPlant = alcoleaPlantNS.plants[0].plant
            alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)

            #TODO test the whole fixture, not just the plant data
            plantns = alcolea.exportPlant()
            self.assertNsEqual(plantns, alcoleaPlantNS)

    def test_Plant_importExport_ExtraFields(self):
        with orm.db_session:

            alcoleaPlantNS = self.samplePlantNS()

            extraSensor = ns()
            extraSensor['extraSensor'] = ns([('name','foosensor')])

            extraSensor2 = ns()
            extraSensor2['extraSensor'] = ns([('name','boosensor')])

            alcoleaPlantNS.plants[0].plant['extraSensors'] = [extraSensor, extraSensor2]
            
            alcoleaPlant = alcoleaPlantNS.plants[0].plant
            alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)

            #TODO test the whole fixture, not just the plant data
            plantns = alcolea.exportPlant()
            self.assertNsEqual(plantns, self.samplePlantNS())

    def test_Plant_importExport_MissingFields(self):
        with orm.db_session:

            alcoleaPlantNS = self.samplePlantNS()

            del alcoleaPlantNS.plants[0].plant['integratedSensors']
            
            alcoleaPlant = alcoleaPlantNS.plants[0].plant
            alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)

            expectedPlantNS = alcoleaPlantNS
            expectedPlantNS.plants[0].plant['integratedSensors'] = []

            #TODO test the whole fixture, not just the plant data
            plantns = alcolea.exportPlant()
            self.assertNsEqual(plantns, expectedPlantNS)

    def test_Plant_importExport_EmptyPlant(self):
        with orm.db_session:

            emptyPlantNS = ns.loads("""\
            plants:
            - plant:
                name: alcolea
                codename: SCSOM04
                description: la bonica planta
                """)
            
            expectedPlantNS = ns.loads("""\
            plants:
            - plant:
                codename: SCSOM04
                description: la bonica planta
                integratedSensors: []
                inverters: []
                irradiationSensors: []
                meters: []
                name: alcolea
                temperatureSensors: []
                """)

            emptyPlant = emptyPlantNS.plants[0].plant
            empty = Plant(name=emptyPlant.name, codename=emptyPlant.codename)
            empty = empty.importPlant(emptyPlantNS)

            #TODO test the whole fixture, not just the plant data
            plantns = empty.exportPlant()
            self.assertNsEqual(plantns, expectedPlantNS)

    def test__Meter_getRegistries__emptyRegistries(self):
        with orm.db_session:
            alcoleaPlantNS = self.samplePlantNS()
            alcoleaPlant = alcoleaPlantNS.plants[0].plant
            alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)

            registries = Meter[1].getRegistries()
            
            expectedRegistries = []

            self.assertListEqual(registries, expectedRegistries)


    def test__Meter_getRegistries__OneRegistry(self):
        with orm.db_session:
            alcoleaPlantNS = self.samplePlantNS()
            alcoleaPlant = alcoleaPlantNS.plants[0].plant
            alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)
            time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)                      
            Meter[1].insertRegistry(
                time = time,
                export_energy_wh = 10,
                import_energy_wh = 5,
                r1_w = 3,
                r2_w = 2,
                r3_w = 4,
                r4_w = 1,
            )

            registries = Meter[1].getRegistries()
            
            expectedRegistries = [{
                'time': time,
                'export_energy_wh': 10,
                'import_energy_wh': 5,
                'r1_w': 3,
                'r2_w': 2,
                'r3_w': 4, 
                'r4_w': 1,
            }]

            self.assertListEqual(registries, expectedRegistries)

    def test__Meter_getRegistries__OneRegistry_two_meters(self):
        with orm.db_session:
            alcoleaPlantNS = self.samplePlantNS()
            alcoleaPlant = alcoleaPlantNS.plants[0].plant
            alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)
            Meter(plant=alcolea, name="Albertinho")
            time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)                      
            Meter[1].insertRegistry(
                time = time,
                export_energy_wh = 10,
                import_energy_wh = 5,
                r1_w = 3,
                r2_w = 2,
                r3_w = 4,
                r4_w = 1,
            )
            Meter[2].insertRegistry(
                time = time,
                export_energy_wh = 110,
                import_energy_wh = 15,
                r1_w = 13,
                r2_w = 12,
                r3_w = 14,
                r4_w = 11,
            )
            registries = Meter[1].getRegistries()
            
            expectedRegistries = [{
                'time': time,
                'export_energy_wh': 10,
                'import_energy_wh': 5,
                'r1_w': 3,
                'r2_w': 2,
                'r3_w': 4, 
                'r4_w': 1,
            }]

            self.assertListEqual(registries, expectedRegistries)


    def test__Meter_getRegistries__OneRegistry_filter_date(self):
        with orm.db_session:
            alcoleaPlantNS = self.samplePlantNS()
            alcoleaPlant = alcoleaPlantNS.plants[0].plant
            alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)
            time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)                      
            delta = dt.timedelta(minutes=30)
            Meter[1].insertRegistry(
                time = time,
                export_energy_wh = 10,
                import_energy_wh = 5,
                r1_w = 3,
                r2_w = 2,
                r3_w = 4,
                r4_w = 1,
            )
            fromdate = time - delta
            todate = time + delta

            registries = Meter[1].getRegistries(fromdate=fromdate, todate=todate)
            
            expectedRegistries = [{
                'time': time,
                'export_energy_wh': 10,
                'import_energy_wh': 5,
                'r1_w': 3,
                'r2_w': 2,
                'r3_w': 4, 
                'r4_w': 1,
            }]

            self.assertListEqual(registries, expectedRegistries)

    def test__inverter_getRegistries__OneRegistry(self):
        with orm.db_session:
            alcoleaPlantNS = self.samplePlantNS()
            alcoleaPlant = alcoleaPlantNS.plants[0].plant
            alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)
            time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)                      
            Inverter[1].insertRegistry(
                daily_energy_h_wh = 1,
                daily_energy_l_wh =2,
                e_total_h_wh = 3,
                e_total_l_wh = 4,
                h_total_h_h = 5,
                h_total_l_h = 6,
                pac_r_w = 7,
                pac_s_w = 8,
                pac_t_w = 9,
                powerreactive_t_v = 10,
                powerreactive_r_v = 11,
                powerreactive_s_v = 12,
                temp_inv_c = 13,
                time = time,
            )

            registries = Inverter[1].getRegistries()
            
            expectedRegistries = [{
                "daily_energy_h_wh": 1,
                "daily_energy_l_wh": 2,
                "e_total_h_wh": 3,
                "e_total_l_wh": 4,
                "h_total_h_h": 5,
                "h_total_l_h": 6,
                "pac_r_w": 7,
                "pac_s_w": 8,
                "pac_t_w": 9,
                "powerreactive_t_v": 10,
                "powerreactive_r_v": 11,
                "powerreactive_s_v": 12,
                "temp_inv_c": 13,
                "time": time,
            }]

            self.assertListEqual(registries, expectedRegistries)

    def __test__forecastMetadata(self):
        pass

    def test__plantData(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcoleaPlant = alcoleaPlantNS.plants[0].plant
        alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)                      

        Meter[1].insertRegistry(
            export_energy_wh = 10,
            import_energy_wh = 5,
            r1_w = 3,
            r2_w = 2,
            r3_w = 4,
            r4_w = 1,
            time = time,
        )
        SensorIrradiation[1].insertRegistry(
            time = time,
            irradiation_w_m2 = 15,
        )
        
        plantdata = alcolea.plantData()

        expectedPlantData = {
            "plant": "alcolea",
            "devices":
            [{
                'id': 'Inverter:5555',
                'readings': []
            }, 
            {
                'id': 'Inverter:6666',
                'readings': []
            },
            {
                "id": "Meter:1234578",
                "readings":
                [{
                    "export_energy_wh": 10,
                    "import_energy_wh": 5,
                    "r1_w": 3,
                    "r2_w": 2,
                    "r3_w": 4,
                    "r4_w": 1,
                    "time": time,
                }]
            },
            {
                "id": "SensorIntegratedIrradiation:voki",
                "readings": []
            },
            {
                "id": "SensorIrradiation:alberto",
                "readings":
                [{
                    "irradiation_w_m2": 15,
                    "time": time,
                }]
            },
            {
                "id": "SensorTemperature:joana",
                "readings": []
            }]
        }

        expectedPlantData["devices"].sort(key=lambda x : x['id'])

        self.assertDictEqual(plantdata, expectedPlantData)

    def test__Plant_insertPlantData(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcoleaPlant = alcoleaPlantNS.plants[0].plant
        alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)                      

        plantData = {
                "plant": "alcolea",
                "devices":
                [{
                    'id': 'Inverter:5555',
                    'readings': []
                }, 
                {
                    'id': 'Inverter:6666',
                    'readings': []
                },
                {
                    "id": "Meter:1234578",
                    "readings":
                    [{
                        "export_energy_wh": 10,
                        "import_energy_wh": 5,
                        "r1_w": 3,
                        "r2_w": 2,
                        "r3_w": 4,
                        "r4_w": 1,
                        "time": time,
                    }]
                },
                {
                    "id": "SensorIntegratedIrradiation:voki",
                    "readings": []
                },
                {
                    "id": "SensorIrradiation:alberto",
                    "readings":
                    [{
                        "irradiation_w_m2": 15,
                        "time": time,
                    }]
                },
                {
                    "id": "SensorTemperature:joana",
                    "readings": []
                }]
            }

        alcolea.insertPlantData(plantData)

        plantDataResult = alcolea.plantData()

        self.assertDictEqual(plantData, plantDataResult)