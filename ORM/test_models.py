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
from ORM.pony_manager import PonyManager
from ORM.models import importPlants, exportPlants


class Models_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()

        self.pony = PonyManager(envinfo.DB_CONF)

        self.pony.define_all_models()
        self.pony.binddb(create_tables=True)

        self.pony.db.drop_all_tables(with_all_data=True)

        self.pony.db.create_tables()

        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()

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
        """)
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
        """)
        return alcoleaPlantNS

    def samplePlantNSWithStrings(self):
        alcoleaPlantNS = ns.loads("""\
            name: alcolea
            codename: SCSOM04
            description: la bonica planta
            inverters:
            - inverter:
                name: '5555'
                strings:
                - string1
                - string2
            - inverter:
                name: '6666'
        """)
        return alcoleaPlantNS

    def samplePlantNSWithModuleParameters(self):
        alcoleaPlantNS = ns.loads("""\
            name: alcolea
            codename: SCSOM04
            description: la bonica planta
            moduleParameters:
                nominalPowerMWp: 2.16
                efficiency: 15.5
                nModules: 4878
                Imp: 9.07
                Vmp: 37.5
                temperatureCoefficientI: 0.05
                temperatureCoefficientV: -0.31
                temperatureCoefficientPmax: -0.442
                irradiationSTC: 1000.0
                temperatureSTC: 25
                degradation: 97.5
                Voc: 46.1
                Isc: 9.5
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
        """)
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
                        "create_date": time
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
                        "create_date": time
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
                        "create_date": time
                    }],
                }],key=lambda d: d['id']),
            }]
        return plantsData

    def test_Plant_importExportPlant(self):

        alcoleaPlantNS = self.samplePlantNS()

        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
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

        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()

        #TODO test the whole fixture, not just the plant data
        plantns = alcolea.exportPlant()
        self.assertNsEqual(plantns, self.samplePlantNS())

    def test_Plant_importExport_MissingFields(self):

        alcoleaPlantNS = self.samplePlantNS()

        del alcoleaPlantNS['temperatureAmbientSensors']

        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()

        expectedPlantNS = alcoleaPlantNS
        expectedPlantNS['temperatureAmbientSensors'] = []

        #TODO test the whole fixture, not just the plant data
        plantns = alcolea.exportPlant()
        self.assertNsEqual(plantns, expectedPlantNS)

    def test__Plant_importExport__ModuleParameters(self):

        alcoleaPlantNS = self.samplePlantNSWithModuleParameters()

        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()

        expectedPlantNS = alcoleaPlantNS
        expectedPlantNS['moduleParameters'] = {
            'nModules': 4878,
            'Imp': 9070,
            'Vmp': 37500,
            'temperatureCoefficientI': 50,
            'temperatureCoefficientV': -310,
            'irradiationSTC': 1000,
            'temperatureSTC': 250,
            'degradation': 9750,
            'Voc': 46100,
            'Isc': 9500,
        }

        #TODO test the whole fixture, not just the plant data
        plantns = alcolea.exportPlant()
        self.assertNsEqual(plantns, expectedPlantNS)


    def test__Plant_importExport__Strings(self):

        alcoleaPlantNS = self.samplePlantNSWithStrings()

        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()

        expectedPlantNS = alcoleaPlantNS
        # expectedPlantNS.inverters[1].inverter['strings'] = []

        #TODO test the whole fixture, not just the plant data
        plantns = alcolea.exportPlant(skipEmpty=True)

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
            inverters: []
            irradiationSensors: []
            meters: []
            name: alcolea
            temperatureAmbientSensors: []
            temperatureModuleSensors: []
            """)

        empty = self.pony.db.Plant(name=emptyPlantNS.name, codename=emptyPlantNS.codename)
        empty = empty.importPlant(emptyPlantNS)

        #TODO test the whole fixture, not just the plant data
        plantns = empty.exportPlant()
        self.assertNsEqual(plantns, expectedPlantNS)

    def test_Plant_importPlants__manyPlants(self):
        plantsns = self.samplePlantsNS()

        importPlants(self.pony.db, plantsns)

        resultPlantns = exportPlants(self.pony.db, skipEmpty=True)

        self.assertNsEqual(plantsns, resultPlantns)

    def test__Plant__getMeter(self):
        plantName = 'alibaba'
        alibaba = self.pony.db.Plant(name=plantName, codename='SomEnergia_{}'.format(plantName))
        meterName = '1234'
        meter = self.pony.db.Meter(plant=alibaba, name=meterName)
        orm.flush()

        resultMeter = alibaba.getMeter()

        self.assertEqual(resultMeter, meter)

    def test__Plant__getMeter__ManyGivesNewest(self):
        plantName = 'alibaba'
        alibaba = self.pony.db.Plant(name=plantName, codename='SomEnergia_{}'.format(plantName))
        oldmeter = self.pony.db.Meter(plant=alibaba, name='oldmeter')
        newmeter = self.pony.db.Meter(plant=alibaba, name='newmeter')
        orm.flush()

        resultMeter = alibaba.getMeter()

        self.assertEqual(resultMeter, newmeter)

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

        importPlants(self.pony.db, plantsns)

        storedMunicipalitiesns = exportPlants(self.pony.db)
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

        importPlants(self.pony.db, plantsns)

        storedMunicipalitiesns = exportPlants(self.pony.db)
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

        importPlants(self.pony.db, plantsns)
        # orm.flush()

        resultPlantns = exportPlants(self.pony.db, skipEmpty=True)
        self.assertNsEqual(plantsns, resultPlantns)

    def test__Plant_importPlantsData__many(self):
        plantsns = self.samplePlantsNS()
        importPlants(self.pony.db, plantsns)

        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        delta = dt.timedelta(minutes=30)
        plantsData = self.samplePlantsData(time, delta)
        self.pony.db.Plant.insertPlantsData(plantsData)
        # orm.flush()

        for plantData in plantsData:
            self.assertDictEqual(dict(plantData), self.pony.db.Plant.get(name=plantData['plant']).plantData(skipEmpty=True))

    def test__Plant_str2model__sameNameDifferentPlant(self):
        plantsns = self.samplePlantsNS()
        plantsns.plants[0].plant.inverters[0].inverter.name = '4444'
        importPlants(self.pony.db, plantsns)
        orm.flush()

        plant = self.pony.db.Plant.get(name='alcolea')

        alcoleainverter = self.pony.db.Plant.str2device(plant, 'Inverter', '4444')

        self.assertNotEqual(alcoleainverter, None)

        plant = self.pony.db.Plant.get(name='figuerea')

        figuereainverter = self.pony.db.Plant.str2device(plant, 'Inverter', '4444')

        self.assertNotEqual(figuereainverter, None)

        self.assertNotEqual(alcoleainverter.plant.name, figuereainverter.plant.name)


    def test__Meter_getRegistries__emptyRegistries(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)

        registries = self.pony.db.Meter[1].getRegistries()

        expectedRegistries = []

        self.assertListEqual(registries, expectedRegistries)

    def test__Meter_getRegistries__OneRegistry(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        self.pony.db.Meter[1].insertRegistry(
            time = time,
            export_energy_wh = 10,
            import_energy_wh = 5,
            r1_VArh = 3,
            r2_VArh = 2,
            r3_VArh = 4,
            r4_VArh = 1,
        )

        registries = self.pony.db.Meter[1].getRegistries()

        expectedRegistries = [{
            'time': time,
            'export_energy_wh': 10,
            'import_energy_wh': 5,
            'r1_VArh': 3,
            'r2_VArh': 2,
            'r3_VArh': 4,
            'r4_VArh': 1,
            'create_date': unittest.mock.ANY
        }]

        self.assertListEqual(registries, expectedRegistries)

    def test__Meter_getRegistries__OneRegistry_two_meters(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        self.pony.db.Meter(plant=alcolea, name="Albertinho")
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        self.pony.db.Meter[1].insertRegistry(
            time = time,
            export_energy_wh = 10,
            import_energy_wh = 5,
            r1_VArh = 3,
            r2_VArh = 2,
            r3_VArh = 4,
            r4_VArh = 1,
        )
        self.pony.db.Meter[2].insertRegistry(
            time = time,
            export_energy_wh = 110,
            import_energy_wh = 15,
            r1_VArh = 13,
            r2_VArh = 12,
            r3_VArh = 14,
            r4_VArh = 11,
        )
        registries = self.pony.db.Meter[1].getRegistries()

        expectedRegistries = [{
            'time': time,
            'export_energy_wh': 10,
            'import_energy_wh': 5,
            'r1_VArh': 3,
            'r2_VArh': 2,
            'r3_VArh': 4,
            'r4_VArh': 1,
            'create_date': unittest.mock.ANY
        }]

        self.assertListEqual(registries, expectedRegistries)


    def test__Meter_getRegistries__OneRegistry_filter_date(self):
        with orm.db_session:
            alcoleaPlantNS = self.samplePlantNS()
            alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
            alcolea = alcolea.importPlant(alcoleaPlantNS)
            time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
            delta = dt.timedelta(minutes=30)
            self.pony.db.Meter[1].insertRegistry(
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

            registries = self.pony.db.Meter[1].getRegistries(fromdate=fromdate, todate=todate)

            expectedRegistries = [{
                'time': time,
                'export_energy_wh': 10,
                'import_energy_wh': 5,
                'r1_VArh': 3,
                'r2_VArh': 2,
                'r3_VArh': 4,
                'r4_VArh': 1,
                'create_date': unittest.mock.ANY
            }]

            self.assertListEqual(registries, expectedRegistries)

    def test__Meter_getLastReadingsDate__oneMeterOnePlantEmptyReadings(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)

        result = self.pony.db.Meter[1].getLastReadingDate()

        self.assertIsNone(result)


    def test__Meter_getLastReadingsDate__oneMeterOnePlantOneReading(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        delta = dt.timedelta(minutes=30)
        meter = self.pony.db.Meter[1]
        meter.insertRegistry(
            time = time,
            export_energy_wh = 10,
            import_energy_wh = 5,
            r1_VArh = 3,
            r2_VArh = 2,
            r3_VArh = 4,
            r4_VArh = 1,
        )

        result = meter.getLastReadingDate()

        expectedResult = time

        self.assertEqual(result, expectedResult)

    def test__inverter_getRegistries__OneRegistry(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        self.pony.db.Inverter[1].insertRegistry(
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

        registries = self.pony.db.Inverter[1].getRegistries()

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

    def test__string_getRegistries__OneRegistry(self):
        alcoleaPlantNS = self.samplePlantNSWithStrings()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        self.pony.db.String[1].insertRegistry(
            intensity_mA = 3,
            time = time,
        )

        registries = self.pony.db.String[1].getRegistries()

        expectedRegistries = [{
            "intensity_mA": 3,
            "time": time,
        }]

        self.assertListEqual(registries, expectedRegistries)

    def __test__forecastMetadata(self):
        pass

    def test__createDevice(self):
        oneplant = self.pony.db.Plant(name="Alice", codename="LaSuisse")
        oneplant.createDevice("Inverter", "Bob")

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
        }

        self.assertDictEqual(plantData, expectedPlant)

    def test__Plant__plantData__InverterWithStrings(self):
        alcoleaPlantNS = self.samplePlantNSWithStrings()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)

        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        registry = {
            'energy_wh': 100,
            'intensity_ca_mA': 1,
            'intensity_cc_mA': 1,
            'power_w': 100,
            'temperature_dc': 1,
            'time': time,
            'uptime_h': 1,
            'voltage_ca_mV': 1,
            'voltage_cc_mV': 1
        }

        self.pony.db.Inverter[1].insertRegistry(**registry)

        plantdata = alcolea.plantData()

        expectedPlantData = {
            "plant": "alcolea",
            "devices":
            [{
                'id': 'Inverter:5555',
                'readings': [registry]
            },
            {
                'id': 'Inverter:6666',
                'readings': []
            }]
        }

        expectedPlantData["devices"].sort(key=lambda x : x['id'])

        self.assertDictEqual(plantdata, expectedPlantData)

    # TODO fix plantData construction of an inverter with strings
    def _test__Inverter__plantData__withStrings(self):
        alcoleaPlantNS = self.samplePlantNSWithStrings()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)

        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        inverterRegistry = {
            'energy_wh': 100,
            'intensity_ca_mA': 1,
            'intensity_cc_mA': 1,
            'power_w': 100,
            'temperature_dc': 1,
            'time': time,
            'uptime_h': 1,
            'voltage_ca_mV': 1,
            'voltage_cc_mV': 1
        }

        stringsRegistry = [100,200]

        self.pony.db.Inverter[1].insertRegistry(**inverterRegistry)
        self.pony.db.String[1].insertRegistry(intensity_mA=stringsRegistry[0], time=time)
        self.pony.db.String[2].insertRegistry(intensity_mA=stringsRegistry[1], time=time)

        inverterPlantdata = self.pony.db.Inverter[1].plantData()

        expected = {
                'id': 'Inverter:5555',
                'readings': [{
                    **inverterRegistry,
                    'String_intensity_mA:string1': 100,
                    'String_intensity_mA:string2': 200
                }]
        }

        self.assertDictEqual(plantdata, expected)

    def _test__String__plantData(self):
        alcoleaPlantNS = self.samplePlantNSWithStrings()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)

        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        inverterRegistry = {
            'energy_wh': 100,
            'intensity_ca_mA': 1,
            'intensity_cc_mA': 1,
            'power_w': 100,
            'temperature_dc': 1,
            'time': time,
            'uptime_h': 1,
            'voltage_ca_mV': 1,
            'voltage_cc_mV': 1
        }

        stringsRegistry = [100,200]

        self.pony.db.Inverter[1].insertRegistry(**inverterRegistry)
        self.pony.db.String[1].insertRegistry(intensity_mA=stringsRegistry[0], time=time)
        self.pony.db.String[2].insertRegistry(intensity_mA=stringsRegistry[1], time=time)

        plantdata = alcolea.plantData()

        # option 1b
        expectedPlantData = {
            "plant": "alcolea",
            "devices":
            [{
                'id': 'Inverter:5555',
                'readings': [{
                    **inverterRegistry,
                    'String_intensity_mA:string1': 100,
                    'String_intensity_mA:string2': 200
                }]
            },
            {
                'id': 'Inverter:6666',
                'readings': []
            }]
        }

        expectedPlantData["devices"].sort(key=lambda x : x['id'])

        self.assertDictEqual(plantdata, expectedPlantData)

    def test__Plant__plantData(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        mRegistry = {
            "export_energy_wh": 10,
            "import_energy_wh": 5,
            "r1_VArh": 3,
            "r2_VArh": 2,
            "r3_VArh": 4,
            "r4_VArh": 1,
            "time": time,
        }

        self.pony.db.Meter[1].insertRegistry(**mRegistry)

        self.pony.db.SensorIrradiation[1].insertRegistry(
            time = time,
            irradiation_w_m2 = 15,
            temperature_dc = 250,
        )

        plantdata = alcolea.plantData()

        mRegistry["create_date"] = unittest.mock.ANY

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
                [mRegistry]
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
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
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
                        "create_date": time
                    }]
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

    def test__Plant_insertPlantData__without_create_date(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        plantData = {
                "plant": "alcolea",
                "devices":
                [
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
                        "time": time
                    }]
                }]
            }

        alcolea.insertPlantData(plantData)

        plantDataResult = alcolea.plantData()

        self.assertIsNotNone(plantDataResult['devices'][2]['readings'][0]['create_date'])
        self.assertIsInstance(plantDataResult['devices'][2]['readings'][0]['create_date'], dt.datetime)

    def _test__Plant_insertPlantData__Strings__Empty(self):
        alcoleaPlantNS = self.samplePlantNSWithStrings()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
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
                }]
            }

        alcolea.insertPlantData(plantData)

        plantDataResult = alcolea.plantData()

        self.assertDictEqual(plantData, plantDataResult)

    def test__Plant_insertPlantData__NewStrings__OneInverter(self):
        alcoleaPlantNS = self.samplePlantNSWithStrings()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        plantData = {
                "plant": "alcolea",
                "devices":
                [{
                    'id': 'Inverter:5555',
                    'readings': [{
                        'energy_wh': 100,
                        'intensity_ca_mA': 1,
                        'intensity_cc_mA': 1,
                        'power_w': 100,
                        'temperature_dc': 1,
                        'time': time,
                        'uptime_h': 1,
                        'voltage_ca_mV': 1,
                        'voltage_cc_mV': 1,
                        'String:string1:intensity_mA': 100,
                        'String:string2:intensity_mA': 200
                    }]
                },
                {
                    'id': 'Inverter:6666',
                    'readings': []
                }]
            }

        alcolea.insertPlantData(plantData)

        # TODO build a plantData with data with strings

        plantdata_result = alcolea.plantData()

        del plantData['devices'][0]['readings'][0]['String:string1:intensity_mA']
        del plantData['devices'][0]['readings'][0]['String:string2:intensity_mA']

        self.assertDictEqual(plantData, plantdata_result)

        expected_strings = [{'time': time, 'intensity_mA': 100}, {'time': time, 'intensity_mA': 200}]

        string_readings = [{'time': r.time, 'intensity_mA': r.intensity_mA} for r in self.pony.db.StringRegistry.select()]

        self.assertListEqual(string_readings, expected_strings)

    def test__municipality_insert(self):
        figueres = self.pony.db.Municipality(
            countryCode = "ES",
            country = "Spain",
            regionCode = "09",
            region = "Catalonia",
            provinceCode = "17",
            province = "Girona",
            ineCode = "17066",
            name = "Figueres",
        )

        alcolea = self.pony.db.Plant(name="Alcolea", codename="Som_Alcolea", municipality=figueres)

        municipality = self.pony.db.Plant.get(name="Alcolea").municipality

        self.assertDictEqual(figueres.to_dict(), municipality.to_dict())

    def test__location_insert(self):
        alcolea = self.pony.db.Plant(name="Alcolea", codename="Som_Alcolea")
        alcoleaLatLong = (42.26610810248693, 2.958334450785599)
        alcoleaLocation = self.pony.db.PlantLocation(
            plant=alcolea,
            latitude=alcoleaLatLong[0],
            longitude=alcoleaLatLong[1]
        )
        alcolea.location = alcoleaLocation

        location = self.pony.db.Plant.get(name="Alcolea").location

        self.assertEqual(alcoleaLatLong, location.getLatLong())

    def test__importPlants__oneplant(self):
        pass

    def test__Meter_updateMeterProtocol_byDefault(self):

        alcoleaPlantNS = self.samplePlantNSWithModuleParameters()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()

        meter = self.pony.db.Meter.get(name='1234578')
        self.assertEqual(meter.connection_protocol, 'ip')

    def test__Meter_updateMeterProtocol_afterUpdate(self):

        alcoleaPlantNS = self.samplePlantNSWithModuleParameters()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        orm.flush()

        self.pony.db.Meter.updateMeterProtocol({
            '1234578': 'moxa',
        })
        meter = self.pony.db.Meter.get(name='1234578')
        self.assertEqual(meter.connection_protocol, 'moxa')

    def test__Meter_updateMeterProtocol_unknownMetersIgnored(self):
        # This adds many more meters such as 9876 which we are not updating
        plantsns = self.samplePlantsNS()
        importPlants(self.pony.db, plantsns)

        self.pony.db.Meter.updateMeterProtocol({
            '1234578': 'moxa',
        })
        meter = self.pony.db.Meter.get(name='1234578')
        self.assertEqual(meter.connection_protocol, 'moxa')
        meter = self.pony.db.Meter.get(name='9876')
        self.assertEqual(meter.connection_protocol, 'ip')

    def test__Meter_updateMeterProtocol_deprecatedMetersIgnored(self):
        plantsns = self.samplePlantsNS()
        importPlants(self.pony.db, plantsns)

        # Should not fail
        self.pony.db.Meter.updateMeterProtocol({
            '1234578': 'moxa',
            'OLDMETER': 'moxa',
        })
        meter = self.pony.db.Meter.get(name='OLDMETER')
        self.assertEqual(meter, None)

    @unittest.skipIf(True, "we don't support String creation directly, only as inverter registry")
    def test__String_createDevice(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        dev = alcolea.createDevice(classname='String', devicename='5555-string1')

        expected = {'id': 1, 'inverter': 1, 'name': 'string1'}
        self.assertIsNotNone(dev)
        self.assertDictEqual(expected, dev.to_dict())


    def test__String_insertRegistry__new(self):
        alcoleaPlantNS = self.samplePlantNS()
        alcolea = self.pony.db.Plant(name=alcoleaPlantNS.name, codename=alcoleaPlantNS.codename)
        alcolea = alcolea.importPlant(alcoleaPlantNS)
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        stringIntensities = [400,400,300,500,2000,300,400,1000,300]

        self.pony.db.Inverter[1].insertRegistry(
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

        registries = self.pony.db.Inverter[1].getRegistries()

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


# vim: et sw=4 ts=4
