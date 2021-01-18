# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

from standardization import alcolea_registers_to_plantdata

class Standardization_Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def alcolea_registers():
        pass

    def test__alcolea_registers_to_plantdata(self):
        

        
        plant_data = alcolea_registers_to_plantdata(registers)
        
        pass