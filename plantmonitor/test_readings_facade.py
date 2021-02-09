# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

import datetime as dt

from yamlns import namespace as ns

from .readings_facade import ReadingsFacade

class ReadingsFacade_Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_transfer_ERP_readings_to_model(self):
        r = ReadingsFacade()

        # TODO mock measures or fake meters
        r.transfer_ERP_readings_to_model()