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

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables

from addPlant import importPlantCLI, importPlant

setupDatabase()

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
                temperatureSensors:
                - temperatureSensor:
                    name: joana
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
            alcolea = Plant.get(name='alcolea')
            self.assertIsNotNone(alcolea)
            plantns = alcolea.exportPlant()
        
        self.assertNsEqual(plantns, content)


    def test__importPlant_File(self):
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
                temperatureSensors:
                - temperatureSensor:
                    name: joana
                integratedSensors:
                - integratedSensor:
                    name: voki"""

        p = Path(fakePlantYaml)
        with p.open("w", encoding="utf-8") as f:
            f.write(content)

        importPlant(fakePlantYaml)

        p.unlink()

        with orm.db_session:
            alcolea = Plant.get(name='alcolea')
            self.assertIsNotNone(alcolea)
            plantns = alcolea.exportPlant()
        
        self.assertNsEqual(plantns, content)