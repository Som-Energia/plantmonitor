#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import unittest

from yamlns import namespace as ns

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from .plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBMock,
    PlantmonitorDBError,
)


class PlantmonitorDB_Test(unittest.TestCase):

    # tests that can't be mocked and require a real db

    def createConfig(self):
        return ns.load('conf/configdb_test.yaml')

    def setUp(self):
        configdb = self.createConfig()
        PlantmonitorDB(configdb).demoDBsetup(configdb)

    def tearDown(self):
        configdb = self.createConfig()
        PlantmonitorDB(configdb).dropDatabase(configdb)

    def mainFacility(self):
        return 'SomEnergia_Amapola'

    def secondaryFacility(self):
        return 'SomEnergia_Tulipan'

    def createPlantmonitorDB(self):
        configdb = ns.load('conf/configdb_test.yaml')
        return PlantmonitorDB(configdb)

    def __test_contextManagers(self):
        pass

    def test_InvalidLogin(self):
        db = self.createPlantmonitorDB()
        db._config['psql_user'] = 'BlackHat'
        self.assertRaises(PlantmonitorDBError, db.login)

    def test_notLoggedIn(self):
        db = self.createPlantmonitorDB()
        # self.assertRaises(AttributeError, db.getMeterData)
        # self.assertRaises(PlantmonitorDBError, db.getMeterData)
        self.assertRaises(Exception, db.getMeterData)

    def test_LogIn(self):
        db = self.createPlantmonitorDB()
        try:
            db.login()
        except PlantmonitorDBError:
            self.fail("Login test failed")
        finally:
            db.close()

    def test_getEmpty(self):
        with self.createPlantmonitorDB() as db:
            result = db.getMeterData()
            self.assertEqual(result, {})

    def test_addOneFromMissingFacility(self):
        with self.createPlantmonitorDB() as db:
            oneRow = {self.mainFacility(): [('2020-01-01 00:00:00', 10)]}
            self.assertRaises(PlantmonitorDBError, db.addMeterData, oneRow)

    def test_addFirstFacilityMeterRelation(self):
        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            result = set([(self.mainFacility(), '123401234')])
            facility_meter = db.getFacilityMeter()
            self.assertEqual(facility_meter, result)

    def test_addAnotherFacilityMeterRelation(self):
        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.secondaryFacility(), '432104321')
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            self.assertTrue(
                db.facilityMeterRelationExists(self.mainFacility(), '123401234')
            )

    def test_addOneMeterData(self):
        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            oneRow = {self.mainFacility(): [('2020-01-01 00:00:00', 10)]}
            db.addMeterData(oneRow)
            result = db.getMeterData()
            self.assertEqual(result, oneRow)

    def test_getOneMeterData(self):
        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.secondaryFacility(), '432104321')
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            data = {self.mainFacility(): [('2020-01-01 00:00:00', 10)]}
            db.addMeterData(data)
            result = db.getMeterData()
            self.assertEqual(result, data)

    def test_getMeterData(self):
        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.secondaryFacility(), '432104321')
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            data = {
                self.mainFacility(): [
                    ("2020-01-01 00:00:00", 10),
                    ("2020-01-01 01:00:00", 20),
                    ("2020-01-01 02:00:00", 30),
                    ("2020-01-01 03:00:00", 40),
                ],
                self.secondaryFacility(): [
                    ("2020-01-01 00:00:00", 210),
                    ("2020-01-01 01:00:00", 320),
                    ("2020-01-01 02:00:00", 230),
                    ("2020-01-01 03:00:00", 340),
                ]
            }
            db.addMeterData(data)
            meter = db.getMeterData()
            self.assertEqual(meter, data)


class PlantmonitorDBMock_Test(PlantmonitorDB_Test):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def createPlantmonitorDB(self):
        config = ns({'psql_user': 'alberto', 'psql_password': '1234'})
        return PlantmonitorDBMock(config)

    def mainFacility(self):
        return "SomEnergia_Fontivsolar"

    def secondaryFacility(self):
        return "SomEnergia_Alcolea"

    def __test_write_meter(self):
        db = self.createPlantmonitorDB()
        facility = self.mainFacility()
        expected = {facility: [("2020-01-01 00:00:00", 10)]}
        result = db.addMeterData(facility, [("2020-01-01 00:00:00", 10)])
        self.assertEqual(result, expected)
        allMeterData = db.getMeterData()
        self.assertEqual(allMeterData.get(facility, None), expected[facility])


unittest.TestCase.__str__ = unittest.TestCase.id
