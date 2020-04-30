#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import unittest

from yamlns import namespace as ns

import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from .plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBMock,
    PlantmonitorDBError,
)

# tests that can't be mocked and require a real db
class PlantmonitorDB_Test(unittest.TestCase):

    def setUp(self):
        configdb = self.createConfig()

        #PlantmonitorDB.demoDb(configdb)

        with psycopg2.connect(
            user = configdb['psql_user'], 
            password = configdb['psql_password'],
            host = configdb['psql_host'], 
            port = configdb['psql_port']
        ) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                cursor.execute("DROP DATABASE IF EXISTS {}".format(configdb['psql_db']))
                cursor.execute("CREATE DATABASE {};".format(configdb['psql_db']))

        with psycopg2.connect(
            user = configdb['psql_user'],
            password = configdb['psql_password'],
            host = configdb['psql_host'], 
            port = configdb['psql_port'], 
            database = configdb['psql_db']
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE TABLE sistema_contador(time TIMESTAMP NOT NULL, \
                name VARCHAR(50), export_energy bigint);")
                cursor.execute("CREATE TABLE facility_meter (id serial primary key, \
                facilityid character varying(200), meter character varying(200));")

    def tearDown(self):
        configdb = self.createConfig()

        #dropDemoDB(configdb)

        with psycopg2.connect(
            user = configdb['psql_user'], 
            password = configdb['psql_password'],
            host = configdb['psql_host'], 
            port = configdb['psql_port']
        ) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                cursor.execute("DROP DATABASE {}".format(configdb['psql_db']))

    def createPlantmonitorDB(self):
        configdb = ns.load('conf/configdb_test.yaml')
        return PlantmonitorDB(configdb)

    def createConfig(self):
        return ns.load('conf/configdb_test.yaml')

    def __test_InvalidLogin(self):
        with PlantmonitorDB(self.createConfig()) as db:
            db._config['psql_user'] = ''
            self.assertEquals(db.client, {'errorCode': 'DISCONNECTED'})

    def __test_notLoggedIn(self):
        db = self.createPlantmonitorDB()
        self.assertEquals(db.client, {'errorCode': 'DISCONNECTED'})

    def __test_getNotLoggedIn(self):
        db = self.createPlantmonitorDB()
        db.close()
        self.assertEquals(None, db.getMeterData())

    def test_LogIn(self):
        db = self.createPlantmonitorDB()
        try:
            login = db.login()
        except:
            self.fail("Login test failed")
        finally:
            db.close()

    def test_getEmpty(self):
        with self.createPlantmonitorDB() as db:
            result = db.getMeterData()
            self.assertEqual(result, {})

    def test_addOneFromMissingFacility(self):
        with self.createPlantmonitorDB() as db:
            oneRow = {'SomEnergia_Amapola': [('2020-01-01 00:00:00',10)]}
            self.assertRaises(PlantmonitorDBError,db.addMeterData,oneRow)
    
    def test_addFirstFacilityMeterRelation(self):
        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation('SomEnergia_Amapola','1234')
            result = set([('SomEnergia_Amapola','1234')])
            facility_meter = db.getFacilityMeter()
            self.assertEqual(facility_meter,result)

    def test_addAnotherFacilityMeterRelation(self):
        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation('SomEnergia_Tulipan','4321')
            db.addFacilityMeterRelation('SomEnergia_Amapola','1234')
            self.assertTrue(db.facilityMeterRelationExists('SomEnergia_Amapola','1234'))

    def test_addOneMeterData(self):
        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation('SomEnergia_Amapola','1234')
            oneRow = {'SomEnergia_Amapola': [('2020-01-01 00:00:00',10)]}
            db.addMeterData(oneRow)
            result = db.getMeterData()
            facility_meter = db.getFacilityMeter()
            self.assertEqual(result, oneRow)

    def __test_getOneMeterData(self):
        with self.createPlantmonitorDB() as db:
            db.addMeterData({'SomEnergia_Amapola': [('2020-01-01 00:00:00',10)]})
            result = db.getMeterData()
            self.assertEqual(result, {})

    def __test_getMeterData(self):
        with self.createPlantmonitorDB() as db:
            facility = self.mainFacility()
            db.addMeterData({facility: [("2020-01-01 00:00:00",10)]})
            meter = db.getMeterData()
            self.assertEqual(meter, [facility, [("2020-01-01 00:00:00",10)]])


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

    def otherFacility(self):
        return "SomEnergia_Alcolea"

    def __test_write_meter(self):
        db = self.createPlantmonitorDB()
        facility = self.mainFacility()
        expected = {facility: [("2020-01-01 00:00:00",10)]}
        result = db.addMeterData(facility, [("2020-01-01 00:00:00",10)])
        self.assertEqual(result, expected)
        allMeterData = db.getMeterData()
        self.assertEqual(allMeterData.get(facility, None), expected[facility])



unittest.TestCase.__str__ = unittest.TestCase.id
