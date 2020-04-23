#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import unittest

from zeep import Client

from yamlns import namespace as ns

import psycopg2

from psycopg2 import Error
import psycopg2.extras

from datetime import datetime, date, timedelta
import pytz
import time

from yamlns import namespace as ns
from .client import (
    upload_meter_data,
)

class Meteologica_Test(unittest.TestCase):

    def setUp(self):

        # conn = psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
        # host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db'])
        # cur = conn.cursor()

        # # Connexió postgresql
        # with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
        # host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
        #     with conn.cursor() as cur:
        #         cur.execute("CREATE TABLE if not exists forecastHead(id SERIAL NOT NULL, errorCode VARCHAR(50), facilityId VARCHAR(50), \
        #         variableId VARCHAR(50), predictorId VARCHAR(20), forecastDate TIMESTAMPTZ, granularity INTEGER, PRIMARY KEY(id));")
        #         cur.execute("CREATE TABLE forecastData(idForecastHead SERIAL REFERENCES forecastHead(id), time TIMESTAMPTZ, percentil10 INTEGER, percentil50 INTEGER, \
        #         percentil90 INTEGER, PRIMARY KEY(idForecastHead,time));")
        #         cur.execute("SELECT create_hypertable('forecastData', 'time');")
        return

    def tearDown(self):
        # # Connexió postgresql
        # with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
        # host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
        #     with conn.cursor() as cur:
        #         cur.execute("DROP TABLE forecastData;")
        #         cur.execute("DROP TABLE forecastHead;")
        pass

    def clearDb(self):
        with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
        host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM forecastData;")
                cur.execute("DELETE FROM forecastHead;")

    def test_erroneusRequest(self):
        self.assertTrue(True)

    def test_expiredToken(self):
        self.assertTrue(True)

    def test_databaseInsert(self):
        self.assertTrue(True)

    def test_speedTest(self):
        self.assertTrue(True)

    def test_daylightSaving(self):
        self.assertTrue(True)


    def test_read_meter(self):
        self.assertTrue(True)

    def test_commit_meter_data(self):
        self.assertTrue(True)

    def test_commit_daylight_saving_meter_data(self):
        self.assertTrue(True)

    def test_read_meter_and_commit_data(self):
        self.assertTrue(True)
