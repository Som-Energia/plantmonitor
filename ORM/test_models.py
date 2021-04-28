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
    importPlants,
    exportPlants,
    Plant,
    Municipality,
    PlantLocation,
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

from .orm_util import setupDatabase, getTablesToTimescale, timescaleTables

setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)

class Models_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

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

    def samplePlantsNS(self):
        alcoleaPlantsNS = ns.loads("""\
            municipalities:
            - municipality:
                name: Figueres
                ineCode: '17066'
                countryCode: ES
                country: Spain
                regionCode: '09'
                region: Catalonia
                provinceCode: '17'
                province: Girona
            - municipality:
                name: Girona
                ineCode: '17079'
            plants:
            - plant:
                name: alcolea
                codename: SCSOM04
                description: la bonica planta
                municipality: '17066'
                meters:
                - meter:
                    name: '1234578'
                inverters:
                - inverter:
                    name: '5555'
            - plant:
                name: figuerea
                codename: Som_figuerea
                description: la bonica planta
                municipality: '17079'
                meters:
                - meter:
                    name: '9876'
                - meter:
                    name: '5432'
                inverters:
                - inverter:
                    name: '4444'
                - inverter:
                    name: '2222'
                irradiationSensors:
                - irradiationSensor:
                    name: oriol
                temperatureModuleSensors:
                - temperatureModuleSensor:
                    name: joan
                temperatureAmbientSensors:
                - temperatureAmbientSensor:
                    name: benjami
                integratedSensors:
                - integratedSensor:
                    name: david""")
        return alcoleaPlantsNS

    def samplePlantNS(self):
        alcoleaPlantNS = ns.loads("""\
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
            temperatureModuleSensors:
            - temperatureModuleSensor:
                name: pol
            temperatureAmbientSensors:
            - temperatureAmbientSensor:
                name: joana
            integratedSensors:
            - integratedSensor:
                name: voki""")
        return alcoleaPlantNS

    def samplePlantsData(self, time, dt):
        plantsData = [
              {
              "plant": "alcolea",
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
                    "id": "Inverter:5555",
                    "readings": [{
                        "time": time,
                        "power_w": 156,
                        "energy_wh": 154,
                        "intensity_cc_mA": 222,
                        "intensity_ca_mA": 500,
                        "voltage_cc_mV": 200,
                        "voltage_ca_mV": 100,
                        "uptime_h": 15,
                        "temperature_dc": 170,
                    }]
                }],key=lambda d: d['id']),
              },
              {
              "plant": "figuerea",
              "devices":
                sorted([{
                    "id": "Meter:9876",
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
                    "id": "Meter:5432",
                    "readings": [{
                        "time": time,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                }],key=lambda d: d['id']),
            }]
        return plantsData

    def test_Plant_importExportPlant(self):

        alcoleaPlantNS = self.samplePlantNS()

        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()

        #TODO test the whole fixture, not just the plant data
        plantns = alcolea.exportPlant()
        self.assertNsEqual(plantns, alcoleaPlantNS)

    def test_Plant_importExport_ExtraFields(self):

        alcoleaPlantNS = self.samplePlantNS()

        extraSensor = ns()
        extraSensor['extraSensor'] = ns([('name','foosensor')])

        extraSensor2 = ns()
        extraSensor2['extraSensor'] = ns([('name','boosensor')])

        alcoleaPlantNS['extraSensors'] = [extraSensor, extraSensor2]

        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()

        #TODO test the whole fixture, not just the plant data
        plantns = alcolea.exportPlant()
        self.assertNsEqual(plantns, self.samplePlantNS())

    def test_Plant_importExport_MissingFields(self):

        alcoleaPlantNS = self.samplePlantNS()

        del alcoleaPlantNS['integratedSensors']

        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()

        expectedPlantNS = alcoleaPlantNS
        expectedPlantNS['integratedSensors'] = []

        #TODO test the whole fixture, not just the plant data
        plantns = alcolea.exportPlant()
        self.assertNsEqual(plantns, expectedPlantNS)

    def test_Plant_importExport_EmptyPlant(self):
        emptyPlantNS = ns.loads("""\
            name: alcolea
            codename: SCSOM04
            description: la bonica planta
            """)

        expectedPlantNS = ns.loads("""\
            codename: SCSOM04
            description: la bonica planta
            integratedSensors: []
            inverters: []
            irradiationSensors: []
            meters: []
            name: alcolea
            temperatureAmbientSensors: []
            temperatureModuleSensors: []
            """)

        empty = Plant(name=emptyPlantNS.name, codename=emptyPlantNS.codename)
        empty = empty.importPlant(emptyPlantNS)

        #TODO test the whole fixture, not just the plant data
        plantns = empty.exportPlant()
        self.assertNsEqual(plantns, expectedPlantNS)

    def test_Plant_importPlants__manyPlants(self):
        plantsns = self.samplePlantsNS()

        importPlants(plantsns)

        resultPlantns = exportPlants(skipEmpty=True)

        self.assertNsEqual(plantsns, resultPlantns)

    def test_Municipality__importMunicipality__OneMunicipality(self):
        plantsns = ns()

        municipalitiesns = ns.loads("""\
        - municipality:
            name: Figueres
            ineCode: '17066'
            countryCode: ES
            country: Spain
            regionCode: '08'
            region: Catalonia
            provinceCode: '17'
            province: Girona
            """)

        plantsns['municipalities'] = municipalitiesns

        importPlants(plantsns)

        storedMunicipalitiesns = exportPlants()
        plantsns['plants'] = []
        self.assertNsEqual(plantsns, storedMunicipalitiesns)

    def test_Plant_importPlants__ManyMunicipalities(self):
        plantsns = ns()

        municipalitiesns = ns.loads("""\
        - municipality:
            name: Figueres
            ineCode: '17066'
        - municipality:
            name: Girona
            ineCode: '17003'
            """)

        plantsns['municipalities'] = municipalitiesns

        importPlants(plantsns)

        storedMunicipalitiesns = exportPlants()
        plantsns['plants'] = []
        self.assertNsEqual(plantsns, storedMunicipalitiesns)

    def test_Plant_importPlants__manyPlants_ManyMunicipalities(self):

        plantsns = self.samplePlantsNS()

        # municipalitiesns = ns.loads("""\
        # - municipality:
        #     name: Figueres
        #     ineCode: '17066'
        # - municipality:
        #     name: Girona
        #     ineCode: '17003'
        #     """)

        # plantsns['municipalities'] = municipalitiesns

        importPlants(plantsns)
        # orm.flush()

        resultPlantns = exportPlants(skipEmpty=True)
        self.assertNsEqual(plantsns, resultPlantns)

    def test__Plant_importPlantsData__many(self):
        plantsns = self.samplePlantsNS()
        importPlants(plantsns)

        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        delta = dt.timedelta(minutes=30)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)
        # orm.flush()

        for plantData in plantsData:
            self.assertDictEqual(dict(plantData), Plant.get(name=plantData['plant']).plantData(skipEmpty=True))

    def test__Plant_str2model__sameNameDifferentPlant(self):
        plantsns = self.samplePlantsNS()
        plantsns.plants[0].plant.inverters[0].inverter.name = '4444'
        importPlants(plantsns)
        orm.flush()

        plant = Plant.get(name='alcolea')

        alcoleainverter = plant.str2model('Inverter', '4444')

        self.assertNotEqual(alcoleainverter, None)

        plant = Plant.get(name='figuerea')

        figuereainverter = plant.str2model('Inverter', '4444')

        self.assertNotEqual(figuereainverter, None)

        self.assertNotEqual(alcoleainverter.plant.name, figuereainverter.plant.name)


    def test__Meter_getRegistries__emptyRegistries(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)

        registries = Meter[1].getRegistries()

        expectedRegistries = []

        self.assertListEqual(registries, expectedRegistries)

    def test__Meter_getRegistries__OneRegistry(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        Meter[1].insertRegistry(
            time = time,
            export_energy_wh = 10,
            import_energy_wh = 5,
            r1_VArh = 3,
            r2_VArh = 2,
            r3_VArh = 4,
            r4_VArh = 1,
        )

        registries = Meter[1].getRegistries()

        expectedRegistries = [{
            'time': time,
            'export_energy_wh': 10,
            'import_energy_wh': 5,
            'r1_VArh': 3,
            'r2_VArh': 2,
            'r3_VArh': 4,
            'r4_VArh': 1,
        }]

        self.assertListEqual(registries, expectedRegistries)

    def test__Meter_getRegistries__OneRegistry_two_meters(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        Meter(plant=alcolea, name="Albertinho")
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        Meter[1].insertRegistry(
            time = time,
            export_energy_wh = 10,
            import_energy_wh = 5,
            r1_VArh = 3,
            r2_VArh = 2,
            r3_VArh = 4,
            r4_VArh = 1,
        )
        Meter[2].insertRegistry(
            time = time,
            export_energy_wh = 110,
            import_energy_wh = 15,
            r1_VArh = 13,
            r2_VArh = 12,
            r3_VArh = 14,
            r4_VArh = 11,
        )
        registries = Meter[1].getRegistries()

        expectedRegistries = [{
            'time': time,
            'export_energy_wh': 10,
            'import_energy_wh': 5,
            'r1_VArh': 3,
            'r2_VArh': 2,
            'r3_VArh': 4,
            'r4_VArh': 1,
        }]

        self.assertListEqual(registries, expectedRegistries)


    def test__Meter_getRegistries__OneRegistry_filter_date(self):
        with orm.db_session:
            alcoleaPlantNS = self.samplePlantNS()
            alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)
            time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
            delta = dt.timedelta(minutes=30)
            Meter[1].insertRegistry(
                time = time,
                export_energy_wh = 10,
                import_energy_wh = 5,
                r1_VArh = 3,
                r2_VArh = 2,
                r3_VArh = 4,
                r4_VArh = 1,
            )
            fromdate = time - delta
            todate = time + delta

            registries = Meter[1].getRegistries(fromdate=fromdate, todate=todate)

            expectedRegistries = [{
                'time': time,
                'export_energy_wh': 10,
                'import_energy_wh': 5,
                'r1_VArh': 3,
                'r2_VArh': 2,
                'r3_VArh': 4,
                'r4_VArh': 1,
            }]

            self.assertListEqual(registries, expectedRegistries)

    def test__Meter_getLastReadingsDate__empty(self):
        result = Meter.getLastReadingDatesOfAllMeters()

        expectedResult = []

        self.assertListEqual(result, expectedResult)


    def test__Meter_getLastReadingsDate__oneMeterOnePlantEmptyReadings(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)

        result = Meter.getLastReadingDatesOfAllMeters()

        expectedResult = [{
            "plant": alcoleaPlantNS.name,
            "devices":
            [{
                "id": "Meter:1234578",
                "time": None,
            }]

        }]

        self.assertListEqual(result, expectedResult)


    def test__Meter_getLastReadingsDate__oneMeterOnePlantOneReading(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        delta = dt.timedelta(minutes=30)
        Meter[1].insertRegistry(
            time = time,
            export_energy_wh = 10,
            import_energy_wh = 5,
            r1_VArh = 3,
            r2_VArh = 2,
            r3_VArh = 4,
            r4_VArh = 1,
        )

        result = Meter.getLastReadingDatesOfAllMeters()

        expectedResult = [{
            "plant": alcoleaPlantNS.name,
            "devices":
            [{
                "id": "Meter:1234578",
                "time": time,
            }]

        }]

        self.assertListEqual(result, expectedResult)

    def _test__Meter_getLastReadingsDate__OneMeterOnePlantOneReading(self):
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        delta = dt.timedelta(minutes=30)
        plantsns = self.samplePlantsNS()
        importPlants(plantsns)
        plantsData = self.samplePlantsData(time, delta)
        Plant.insertPlantsData(plantsData)

        result = Meter.getLastReadingDatesOfAllMeters()

        expectedResult = [{
            "plant": "alcolea",
            "devices":
            [{
                "id": "Meter:1234578",
                "time": time,
            }]

        }]

        self.assertListEqual(result, expectedResult)

    def test__Meter_getLastReadingsDate__manyMetersManyPlantsManyReadings(self):
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        delta = dt.timedelta(minutes=30)
        plantsns = self.samplePlantsNS()
        importPlants(plantsns)
        plantsData = self.samplePlantsData(time, delta)

        extraReadings = [{
                        "time": time + i*delta,
                        "export_energy_wh": 1 + i*50,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    } for i in range(5)]

        plantsData[1]["devices"][0]["readings"] = extraReadings
        Plant.insertPlantsData(plantsData)
        extratime = time+4*delta

        result = Meter.getLastReadingDatesOfAllMeters()

        result[1]["devices"].sort(key=lambda d: d['id'])
        expectedResult = [
            {
                "plant": "alcolea",
                "devices":
                    [{
                        "id": "Meter:1234578",
                        "time": time,
                    }],
            },
            {
                "plant": "figuerea",
                "devices":
                    [{
                        "id": "Meter:5432",
                        "time": extratime,
                    },
                    {
                        "id": "Meter:9876",
                        "time": time,
                    }]
            }
        ]

        self.assertListEqual(result, expectedResult)

    def test__inverter_getRegistries__OneRegistry(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        Inverter[1].insertRegistry(
            power_w = 1,
            energy_wh =2,
            intensity_cc_mA = 3,
            intensity_ca_mA = 4,
            voltage_cc_mV = 5,
            voltage_ca_mV = 6,
            uptime_h = 7,
            temperature_dc = 8,
            time = time,
        )

        registries = Inverter[1].getRegistries()

        expectedRegistries = [{
            "power_w": 1,
            "energy_wh": 2,
            "intensity_cc_mA": 3,
            "intensity_ca_mA": 4,
            "voltage_cc_mV": 5,
            "voltage_ca_mV": 6,
            "uptime_h": 7,
            "temperature_dc": 8,
            "time": time,
        }]

        self.assertListEqual(registries, expectedRegistries)

    def __test__forecastMetadata(self):
        pass

    def test__createDevice(self):
        oneplant = Plant(name="Alice", codename="LaSuisse")
        oneplant.createDevice(oneplant, "Inverter", "Bob")

        plantData = oneplant.exportPlant()

        expectedPlant = {"name": "Alice",
            "codename": "LaSuisse",
            "description": '',
            "meters": [],
            "inverters": [{
                "inverter": { "name": "Bob" },
            }],
            "irradiationSensors": [],
            "temperatureAmbientSensors": [],
            "temperatureModuleSensors": [],
            "integratedSensors": [],
        }

        self.assertDictEqual(plantData, expectedPlant)


    def test__plantData(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        Meter[1].insertRegistry(
            export_energy_wh = 10,
            import_energy_wh = 5,
            r1_VArh = 3,
            r2_VArh = 2,
            r3_VArh = 4,
            r4_VArh = 1,
            time = time,
        )
        SensorIrradiation[1].insertRegistry(
            time = time,
            irradiation_w_m2 = 15,
            temperature_dc = 250,
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
                    "r1_VArh": 3,
                    "r2_VArh": 2,
                    "r3_VArh": 4,
                    "r4_VArh": 1,
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
                    'temperature_dc': 250,
                    "time": time,
                }]
            },
            {
                "id": "SensorTemperatureAmbient:joana",
                "readings": []
            },
            {
                "id": "SensorTemperatureModule:pol",
                "readings": []
            }]
        }

        expectedPlantData["devices"].sort(key=lambda x : x['id'])

        self.assertDictEqual(plantdata, expectedPlantData)

    def test__Plant_insertPlantData(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()
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
                        "r1_VArh": 3,
                        "r2_VArh": 2,
                        "r3_VArh": 4,
                        "r4_VArh": 1,
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
                        "temperature_dc": 250,
                        "time": time,
                    }]
                },
                {
                    "id": "SensorTemperatureAmbient:joana",
                    "readings": []
                },
                {
                    "id": "SensorTemperatureModule:pol",
                    "readings": []
                }]
            }

        alcolea.insertPlantData(plantData)

        plantDataResult = alcolea.plantData()

        self.assertDictEqual(plantData, plantDataResult)

    def test__municipality_insert(self):
        figueres = Municipality(
            countryCode = "ES",
            country = "Spain",
            regionCode = "09",
            region = "Catalonia",
            provinceCode = "17",
            province = "Girona",
            ineCode = "17066",
            name = "Figueres",
        )

        alcolea = Plant(name="Alcolea", codename="Som_Alcolea", municipality=figueres)

        municipality = Plant.get(name="Alcolea").municipality

        self.assertDictEqual(figueres.to_dict(), municipality.to_dict())

    def test__location_insert(self):
        alcolea = Plant(name="Alcolea", codename="Som_Alcolea")
        alcoleaLatLong = (42.26610810248693, 2.958334450785599)
        alcoleaLocation = PlantLocation(
            plant=alcolea,
            latitude=alcoleaLatLong[0],
            longitude=alcoleaLatLong[1]
        )
        alcolea.location = alcoleaLocation

        location = Plant.get(name="Alcolea").location

        self.assertEqual(alcoleaLatLong, location.getLatLong())

    def test__importPlants__oneplant(self):
        pass
