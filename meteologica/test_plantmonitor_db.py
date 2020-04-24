#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import unittest

from yamlns import namespace as ns

from .plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBMock,
)

class PlantmonitorDBMock_Test(unittest.TestCase):

    def createPlantmonitorDB(self):
        return PlantmonitorDBMock()

    def mainFacility(self):
        return "SomEnergia_Fontivsolar"

    def otherFacility(slf):
        return "SomEnergia_Alcolea"

    def test_write_meter(self):
        db = self.createPlantmonitorDB()
        facility = self.mainFacility()
        result = db.add([facility, [("2020-01-01 00:00:00",10)]])
        self.assertEqual(result, [facility, [("2020-01-01 00:00:00",10)]])

    def __test_read_meter(self):
        db = self.createPlantmonitorDB()
        facility = self.mainFacility()
        db.add([facility, [("2020-01-01 00:00:00",10)]])
        meter = db.getMeterData()
        self.assertEqual(meter, [facility, [("2020-01-01 00:00:00",10)]])

# tests that can't be mocked and require a real db
class PlantmonitorDB_Test(PlantmonitorDBMock_Test):

    def createPlantmonitorDB(self):
        return PlantmonitorDB()
