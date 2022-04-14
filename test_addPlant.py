# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
from yamlns import namespace as ns
import datetime as dt

from click.testing import CliRunner

from meteologica.plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBMock,
    PlantmonitorDBError,
)

from meteologica.utils import todt

from pathlib import Path

from pony import orm

from ORM.pony_manager import PonyManager
from ORM.models import (
    importPlants,
    exportPlants,
)

from ORM.db_utils import setupDatabase, getTablesToTimescale, timescaleTables

from addPlant import importPlantCLI, importPlantsFromFile

class ImportPlant_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

    def setUpORM(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()

        self.pony = PonyManager(envinfo.DB_CONF)

        self.pony.define_all_models()
        self.pony.binddb(create_tables=True)

        self.pony.db.drop_all_tables(with_all_data=True)

        #orm.set_sql_debug(True)
        self.pony.db.create_tables()


        # database.generate_mapping(create_tables=True)
        # orm.db_session.__enter__()

    def tearDownORM(self):
        orm.rollback()
        # orm.db_session.__exit__()
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()

    def setUp(self):
        self.setUpORM()

    def tearDown(self):
        self.tearDownORM()

    def test_importExportPlant__base(self):
        with orm.db_session:

            nsplants = ns.loads("""\
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
                        name: pol""")

            importPlants(self.pony.db, nsplants)

            #TODO test the whole fixture, not just the plant data
            expectedPlantsNS = exportPlants(self.pony.db)
            self.assertNsEqual(expectedPlantsNS, nsplants)

    def test__importPlantCLI_NoFile(self):
        runner = CliRunner()
        result = runner.invoke(importPlantCLI, [])
        self.assertEqual(2, result.exit_code)

    def test__importPlantCLI_File(self):
        fakePlantYaml = 'fakeplant.yaml'

        content = """\
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
                    name: pol"""

        p = Path(fakePlantYaml)
        with p.open("w", encoding="utf-8") as f:
            f.write(content)

        runner = CliRunner()
        result = runner.invoke(importPlantCLI, [fakePlantYaml])
        print(result)
        self.assertEqual(0, result.exit_code)

        p.unlink()

        with orm.db_session:
            plantns = exportPlants(self.pony.db)

        self.assertNsEqual(plantns, content)


    def test__importPlant_File(self):
        fakePlantsYaml = 'fakeplant.yaml'

        content = """\
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
                    name: pol"""

        p = Path(fakePlantsYaml)
        with p.open("w", encoding="utf-8") as f:
            f.write(content)

        importPlantsFromFile(self.pony.db, fakePlantsYaml)

        p.unlink()

        with orm.db_session:
            plantns = exportPlants(self.pony.db)

        self.assertNsEqual(plantns, content)

    def test__importPlant_File__withMunicipalities(self):
        fakePlantsYaml = 'fakeplant.yaml'

        content = """\
            municipalities:
            - municipality:
                name: Figueres
                ineCode: '17066'
                countryCode: ES
                country: Spain
                regionCode: '08'
                region: Catalonia
                provinceCode: '17'
                province: Girona
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
                    name: pol"""

        p = Path(fakePlantsYaml)
        with p.open("w", encoding="utf-8") as f:
            f.write(content)

        importPlantsFromFile(self.pony.db, fakePlantsYaml)

        p.unlink()

        with orm.db_session:
            plantns = exportPlants(self.pony.db)

        self.assertNsEqual(plantns, content)

    def test_importExportPlant__with_details(self):
        self.maxDiff=None

        with orm.db_session:

            nsplants = ns.loads("""\
                plants:
                - plant:
                    name: alcolea
                    codename: SCSOM04
                    description: la bonica planta
                    location:
                        lat: 40.02
                        long: -2.11
                    plantParameters:
                        peakPowerMWp: 100000
                        nominalPowerMW: 80000
                        connectionDate: 2022-01-01
                        targetMonthlyEnergyGWh: 0
                    meters:
                    - meter:
                        name: '1234578'
            """)

            importPlants(self.pony.db, nsplants)

            orm.flush()

            resultPlantsNS = exportPlants(self.pony.db, skipEmpty=True)
            self.assertNsEqual(nsplants, resultPlantsNS)

    def test_importExportPlant__without_description(self):
        self.maxDiff=None

        with orm.db_session:

            nsplants = ns.loads("""\
                plants:
                - plant:
                    name: alcolea
            """)

            importPlants(self.pony.db, nsplants)

            orm.flush()

            #TODO test the whole fixture, not just the plant data
            resultPlantNs = exportPlants(self.pony.db, skipEmpty=True)
            nsplants.plants[0].plant.description = ''
            nsplants.plants[0].plant.codename = 'SomEnergia_alcolea'

            self.assertNsEqual(nsplants, resultPlantNs)

    def test_importExportPlant__new_plant(self):
        self.maxDiff=None

        with orm.db_session:

            nsplants = ns.loads("""\
                plants:
                - plant:
                    name: new_plant
            """)

            importPlants(self.pony.db, nsplants)

            orm.flush()

            #TODO test the whole fixture, not just the plant data
            resultPlantNs = exportPlants(self.pony.db, skipEmpty=True)
            nsplants.plants[0].plant.description = ''
            nsplants.plants[0].plant.codename = 'SomEnergia_new_plant'

            self.assertNsEqual(nsplants, resultPlantNs)

    def test__importExportPlant__keep_old_meters(self):
        #orm.set_sql_debug(debug=True, show_values=True)

        nsplants_original = ns.loads("""\
            plants:
            - plant:
                name: Le_Roger
                codename: SomEnergia_Le_Roger_Meteo
                description: ohlala
                meters:
                - meter:
                    name: '1234578'
        """)

        importPlants(self.pony.db, nsplants_original)

        orm.flush()

        nsplants = ns.loads("""\
            plants:
            - plant:
                name: Le_Roger
                description: sacrebleu
        """)

        importPlants(self.pony.db, nsplants)

        orm.flush()

        #TODO test the whole fixture, not just the plant data
        resultPlantNs = exportPlants(self.pony.db, skipEmpty=True)

        nsplants_original.plants[0].plant.description = 'sacrebleu'

        self.assertNsEqual(nsplants_original.plants[0].plant.meters, resultPlantNs.plants[0].plant.meters)

    #TODO currently we do not support updating plant's name, codenames or description, it will only get the name
    def _test__importExportPlant__update_plant_data(self):
        pass

    def _test__importExportPlant__update_plant_keep_codename(self):
        self.maxDiff=None

        with orm.db_session:
            nsplants_original = ns.loads("""\
                plants:
                - plant:
                    name: Le_Roger
                    codename: SomEnergia_Le_Roger_Meteo
                    description: ohlala
            """)

            importPlants(self.pony.db, nsplants_original)

            orm.flush()

            nsplants = ns.loads("""\
                plants:
                - plant:
                    name: Le_Roger
                    description: sacrebleu
            """)

            importPlants(self.pony.db, nsplants)

            orm.flush()

            #TODO test the whole fixture, not just the plant data
            resultPlantNs = exportPlants(self.pony.db, skipEmpty=True)

            nsplants_original.plants[0].plant.description = 'sacrebleu'

            self.assertNsEqual(nsplants_original, resultPlantNs)

    def _test__importExportPlant__update_plant_keep_description(self):
        self.maxDiff=None

        with orm.db_session:
            nsplants_original = ns.loads("""\
                plants:
                - plant:
                    name: Le_Roger
                    codename: SomEnergia_Le_Roger_Meteo
                    description: ohlala
            """)

            importPlants(self.pony.db, nsplants_original)

            orm.flush()

            nsplants = ns.loads("""\
                plants:
                - plant:
                    name: Le_Roger
                    codename: SomEnergia_Le_Roger_Nouveau_Meteo
            """)

            importPlants(self.pony.db, nsplants)

            orm.flush()

            #TODO test the whole fixture, not just the plant data
            resultPlantNs = exportPlants(self.pony.db, skipEmpty=True)

            nsplants_original.plants[0].plant.codename = 'SomEnergia_Le_Roger_Nouveau_Meteo'

            self.assertNsEqual(nsplants_original, resultPlantNs)