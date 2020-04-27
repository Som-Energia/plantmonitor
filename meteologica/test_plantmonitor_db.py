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
)

# tests that can't be mocked and require a real db
class PlantmonitorDB_Test(unittest.TestCase):
    
    def setUp(self):
        configdb = ns.load('conf/configdb_test.yaml')

        with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
        host = configdb['psql_host'], port = configdb['psql_port']) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:     
                cursor.execute("CREATE DATABASE {};".format(configdb['psql_db']))

                # cursor.execute("CREATE TABLE if not exists sistema_contador(id SERIAL NOT NULL, errorCode VARCHAR(50), facilityId VARCHAR(50), \
                # variableId VARCHAR(50), predictorId VARCHAR(20), forecastDate TIMESTAMPTZ, granularity INTEGER, PRIMARY KEY(id));")
                # cursor.execute("CREATE TABLE forecastData(idForecastHead SERIAL REFERENCES forecastHead(id), time TIMESTAMPTZ, percentil10 INTEGER, percentil50 INTEGER, \
                # percentil90 INTEGER, PRIMARY KEY(idForecastHead,time));")
                # cursor.execute("SELECT create_hypertable('forecastData', 'time');")
        conn.close()

    def tearDown(self):
        configdb = ns.load('conf/configdb_test.yaml')

        with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
        host = configdb['psql_host'], port = configdb['psql_port']) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute("DROP DATABASE {}".format(configdb['psql_db']))
        conn.close()

    def createPlantmonitorDB(self):
        configdb = ns.load('conf/configdb_test.yaml')
        return PlantmonitorDB(configdb)

    def __test_InvalidLogin(self):
        db = self.createPlantmonitorDB()
        db._config['psql_user'] = ''
        self.assertEquals(db.client, {'errorCode': 'DISCONNECTED'})

    def __test_notLoggedIn(self):
        db = self.createPlantmonitorDB()
        self.assertEquals(db.client, {'errorCode': 'DISCONNECTED'})

    def __test_getNotLoggedIn(self):
        db = self.createPlantmonitorDB()
        self.assertEquals(None, db.getMeterData())

    def test_LogIn(self):
        db = self.createPlantmonitorDB()
        login = db.login()
        self.assertEquals(login, {'errorCode': 'OK'})

    def __test_getEmpty(self):
        db = self.createPlantmonitorDB()
        db.login()
        result = db.getMeterData()
        self.assertEqual(result, {})

    def __test_getMeterData(self):
        db = self.createPlantmonitorDB()
        facility = self.mainFacility()
        db.add(facility, [("2020-01-01 00:00:00",10)])
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
        result = db.add(facility, [("2020-01-01 00:00:00",10)])
        self.assertEqual(result, expected)
        allMeterData = db.getMeterData()
        self.assertEqual(allMeterData.get(facility, None), expected[facility])



unittest.TestCase.__str__ = unittest.TestCase.id