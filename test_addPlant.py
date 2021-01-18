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

from ORM.models import database
from ORM.models import (
    importPlants,
    exportPlants,
    Plant,
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

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables

from addPlant import importPlantCLI, importPlantsFromFile

setupDatabase(create_tables=True, timescale_tables=True, drop_tables=True)

class ImportPlant_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

    def setUpORM(self):

        from conf import dbinfo
        self.assertEqual(dbinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        database.drop_all_tables(with_all_data=True)

        self.maxDiff=None
        # orm.set_sql_debug(True)

        database.create_tables()

        # database.generate_mapping(create_tables=True)
        # orm.db_session.__enter__()

    def tearDownORM(self):
        orm.rollback()
        # orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()

    def setUp(self):
        self.setUpORM()

    def tearDown(self):
        self.tearDownORM()

    def test_importExportPlant(self):
        with orm.db_session:

            plantsns = ns.loads("""\
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
                        name: pol
                    integratedSensors:
                    - integratedSensor:
                        name: voki""")

            importPlants(plantsns)

            #TODO test the whole fixture, not just the plant data
            expectedPlantsNS = exportPlants()
            self.assertNsEqual(expectedPlantsNS, plantsns)

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
                    name: pol
                integratedSensors:
                - integratedSensor:
                    name: voki"""

        p = Path(fakePlantYaml)
        with p.open("w", encoding="utf-8") as f:
            f.write(content)

        runner = CliRunner()
        result = runner.invoke(importPlantCLI, [fakePlantYaml])
        self.assertEqual(0, result.exit_code)

        p.unlink()

        with orm.db_session:
            plantns = exportPlants()
        
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
                    name: pol
                integratedSensors:
                - integratedSensor:
                    name: voki"""

        p = Path(fakePlantsYaml)
        with p.open("w", encoding="utf-8") as f:
            f.write(content)

        importPlantsFromFile(fakePlantsYaml)

        p.unlink()

        with orm.db_session:
            plantns = exportPlants()
        
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
                    name: pol
                integratedSensors:
                - integratedSensor:
                    name: voki"""

        p = Path(fakePlantsYaml)
        with p.open("w", encoding="utf-8") as f:
            f.write(content)

        importPlantsFromFile(fakePlantsYaml)

        p.unlink()

        with orm.db_session:
            plantns = exportPlants()
        
        self.assertNsEqual(plantns, content)