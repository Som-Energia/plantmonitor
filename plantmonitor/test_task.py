# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

from yamlns import namespace as ns

from plantmonitor.resource import ProductionPlant

from conf import envinfo

from .task import get_plant_reading

from unittest.mock import MagicMock, Mock


class Task_Test(unittest.TestCase):

    def test_config(self):

        plant = ProductionPlant()

        self.assertTrue(envinfo)
        self.assertTrue('activeplant' in envinfo.ACTIVEPLANT_CONF)

        plantname = envinfo.ACTIVEPLANT_CONF['activeplant']

        apiconfig = envinfo.API_CONFIG

        expected_apiconfig = {"api_url":"http://localhost:5000","version":"1.0"}
        self.assertDictEqual(apiconfig, expected_apiconfig)

        result = plant.load('test_data/modmap_testing.yaml', plantname)

        self.assertTrue(result)


    def test__task__get_plant_reading(self):

        plantname = 'Alibaba'

        plant = ProductionPlant()
        plant.load('test_data/modmap_testing.yaml', plantname)

        plant.get_registers = MagicMock(return_value={plantname: []})

        plant_data = get_plant_reading(plant)

        self.assertIsNone(plant_data)
