# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

from yamlns import namespace as ns

from plantmonitor.resource import ProductionPlant

from conf import envinfo


class Task_Test(unittest.TestCase):

    def test_config(self):

        plant = ProductionPlant()

        self.assertTrue(envinfo)
        self.assertTrue('activeplant' in envinfo.ACTIVEPLANT_CONF)

        plantname = envinfo.ACTIVEPLANT_CONF['activeplant']

        result = plant.load('conf/modmap_testing.yaml', plantname)

        self.assertTrue(result)


