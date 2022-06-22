#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from pathlib import Path
from unittest import TestCase
from maintenance.db_test_factory import DbTestFactory, DbPlantFactory
import datetime

from .db_manager import DBManager

class TestDbTestFactory(TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF

        debug = False

        cls.dbmanager = DBManager(**database_info, timezone_str='utc', echo=debug)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.dbmanager.close_db()

    def setUp(self):
        self.session = self.dbmanager.db_con.begin()

    def tearDown(self):
        self.session.rollback()
        self.session.close()

    def test_create(self):
        factory = DbTestFactory(self.dbmanager)
        factory.create('inverterregistry_factory.csv', 'inverterregistry')
        factory.delete('inverterregistry')

    def test__create_solargis(self):
        factory = DbPlantFactory(self.dbmanager)
        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        factory.create_inverter_sensor_plant(sunrise, sunset)

        factory.create_solargis('input__solargis_readings__base.csv')

        readings = self.dbmanager.db_con.execute("select * from satellite_readings;").fetchall()
        expected = [
            (datetime.datetime(2022,3,1,10,30,tzinfo=datetime.timezone.utc), 1, 555, 860, 440, 694, 'solargis', datetime.datetime(2022,3,20,14,48,21,566310,tzinfo=datetime.timezone.utc)),
            (datetime.datetime(2022,3,1,11,30,tzinfo=datetime.timezone.utc), 1, 733,1008, 507, 783, 'solargis', datetime.datetime(2022,3,20,14,48,21,566310,tzinfo=datetime.timezone.utc)),
            (datetime.datetime(2022,3,1,12,30,tzinfo=datetime.timezone.utc), 1, 862,1065, 542, 792, 'solargis', datetime.datetime(2022,3,20,14,48,21,566310,tzinfo=datetime.timezone.utc)),
        ]
        self.assertListEqual(readings, expected)

    def test_create_meter_plant(self):
        factory = DbPlantFactory(self.dbmanager)
        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        factory.create_meter_plant(sunrise, sunset)

        meters = self.dbmanager.db_con.execute("select * from meter;").fetchall()
        expected = [
                (2, 'Alibaba_meter', 1, 'ip'),
                (7, 'Meravelles_meter', 2, 'moxa'),
                (34, 'Verne_meter', 3, 'moxa',),
                (36, 'Lupin_meter', 4, 'moxa')
        ]
        self.assertListEqual(meters, expected)

