#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from pathlib import Path
from unittest import TestCase, skipIf

from .maintenance import (
    get_latest_reading,
    update_bucketed_inverter_registry,
    update_bucketed_string_registry,
    alarm_maintenance
)
from .db_manager import DBManager

import pandas as pd
from pandas.testing import assert_frame_equal

import datetime

from .db_test_factory import DbTestFactory, DbPlantFactory

from re import search

class IrradiationDBConnectionTest(TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF

        debug = False

        cls.dbmanager = DBManager(**database_info, echo=debug)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.dbmanager.close_db()

    def setUp(self):
        self.session = self.dbmanager.db_con.begin()

    def tearDown(self):
        self.session.rollback()
        self.session.close()

    def test__write_table(self):

        sometime = datetime.datetime(2021,1,1,tzinfo=datetime.timezone.utc)
        df = pd.DataFrame({'time' : sometime, 'value': 100}, index=[0])

        df.to_sql('test_table', self.dbmanager.db_con)

    def _test__alarm_maintenance_via_pandas(self):

        sometime = datetime.datetime(2021,1,1,tzinfo=datetime.timezone.utc)
        df = pd.DataFrame({'time' : sometime, 'value': 100}, index=[0])
        src_table_name = 'test_alarm_source'

        df.to_sql(src_table_name, self.dbmanager.db_con, if_exists='replace')

    def test__execute_sql(self):

        # setup
        # create source table
        readingtime = datetime.datetime(2021,1,1,tzinfo=datetime.timezone.utc)
        src_table_name = 'test_alarm_source'
        self.dbmanager.db_con.execute('create table {} (time timestamptz, value integer)'.format(src_table_name))
        self.dbmanager.db_con.execute(
            "insert into {}(time, value) values ('{}', {}), ('{}', {})".format(
                src_table_name,
                readingtime.strftime('%Y-%m-%d %H:%M:%S%z'), 0,
                (readingtime+datetime.timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S%z'), 10
            )
        )

        # create destination table
        dst_table_name = 'test_alarm_destination'
        self.dbmanager.db_con.execute('create table {} (like {}, alarm bool)'.format(dst_table_name, src_table_name))

        # execute
        # load query and run maintenance
        query = 'insert into {} select *, TRUE as alarm from {} where value = 0'.format(dst_table_name, src_table_name)
        self.dbmanager.db_con.execute(query)

        results = self.dbmanager.db_con.execute('select * from {}'.format(dst_table_name)).fetchall()

        # check
        expected = [(readingtime, 0, True)]
        # check inserted results
        self.assertListEqual(results, expected)

    def test__get_latest_reading(self):
        readingtime = datetime.datetime(2021,1,1,12,tzinfo=datetime.timezone.utc)
        table_name = 'test_alarm_source'
        self.dbmanager.db_con.execute('create table {} (time timestamptz, power_w integer)'.format(table_name))
        self.dbmanager.db_con.execute(
            "insert into {}(time, power_w) values ('{}', {}), ('{}', {})".format(
                table_name,
                readingtime.strftime('%Y-%m-%d %H:%M:%S%z'), 0,
                (readingtime+datetime.timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S%z'), 10
            )
        )

        result = get_latest_reading(self.dbmanager.db_con, table_name)

        self.assertEqual(result, readingtime+datetime.timedelta(minutes=5))

    def test__get_latest_reading__empty_table(self):
        readingtime = datetime.datetime(2021,1,1,13,5, tzinfo=datetime.timezone.utc)
        target_table = 'test_alarm_target'
        source_table = 'test_alarm_source'
        self.dbmanager.db_con.execute('create table {} (time timestamptz, power_w integer)'.format(target_table))
        self.dbmanager.db_con.execute('create table {} (time timestamptz, power_w integer)'.format(source_table))
        self.dbmanager.db_con.execute(
            "insert into {}(time, power_w) values ('{}', {}), ('{}', {})".format(
                source_table,
                readingtime.strftime('%Y-%m-%d %H:%M:%S%z'), 0,
                (readingtime+datetime.timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S%z'), 10
            )
        )

        result = get_latest_reading(self.dbmanager.db_con, target_table, source_table)
        result = result.astimezone(datetime.timezone.utc)
        self.assertEqual(result, readingtime)

    def create_plant(self, sunrise, sunset):
        # TODO tables already exist, why?
        self.dbmanager.db_con.execute('create table if not exists plant (id serial primary key, name text, codename text, description text)')
        self.dbmanager.db_con.execute(
            "insert into plant(id, name, codename, description) values ({}, '{}', '{}', '{}')".format(
                1, 'Alibaba', 'SomEnergia_Alibaba', ''
            )
        )
        self.dbmanager.db_con.execute('create table if not exists inverter (id serial primary key, name text, plant integer)')
        self.dbmanager.db_con.execute(
            "insert into inverter(id, name, plant) values ({}, '{}', {})".format(
                1, 'Alibaba_inverter', 1
            )
        )
        self.dbmanager.db_con.execute('create table if not exists sensor (id serial primary key, name text, plant integer not null, description text, deviceColumname text)')
        self.dbmanager.db_con.execute(
            "insert into sensor(id, name, plant, description, deviceColumname) values ({}, '{}', {}, '{}', '{}')".format(
                1, 'SensorIrradiation1', 1, '', 'sensor'
            )
        )

        self.dbmanager.db_con.execute('create table if not exists solarevent (id serial primary key, plant integer not null, sunrise timestamptz, sunset timestamptz)')
        self.dbmanager.db_con.execute(
            "insert into solarevent(id, plant, sunrise, sunset) values ({}, {}, '{}', '{}')".format(
                1, 1,
                sunrise.strftime('%Y-%m-%d %H:%M:%S%z'),sunset.strftime('%Y-%m-%d %H:%M:%S%z')
            )
        )

    def test__alarm_maintenance_via_sql__from_file(self):

        # setup
        # create source table
        readingtime = datetime.datetime(2021,1,1,12,tzinfo=datetime.timezone.utc)
        inverter_5m_table_name = 'inverterregistry_5min_avg'
        self.dbmanager.db_con.execute('create table {} (time timestamptz, inverter SERIAL PRIMARY KEY, power_w integer)'.format(inverter_5m_table_name))
        self.dbmanager.db_con.execute(
            "insert into {}(time, power_w) values ('{}', {}), ('{}', {})".format(
                inverter_5m_table_name,
                readingtime.strftime('%Y-%m-%d %H:%M:%S%z'), 0,
                (readingtime+datetime.timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S%z'), 10
            )
        )

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)

        self.create_plant(sunrise, sunset)

        # create destination table
        dst_table_name = 'test_alarm_destination'
        self.dbmanager.db_con.execute('create table {} (like {}, alarm bool)'.format(dst_table_name, inverter_5m_table_name))

        # execute
        # load query and run maintenance
        query = Path('queries/zero_inverter_power_at_daylight.sql').read_text(encoding='utf8')
        # modify where clause
        query = query.format((readingtime-datetime.timedelta(minutes=5)).strftime("'%Y-%m-%d %H:%M:%S%z'"))
        results =  self.dbmanager.db_con.execute(query).fetchall()

        # check
        expected = [(readingtime, 1,1, 0, sunrise, sunset)]
        # check inserted results
        self.assertListEqual(results, expected)

class MaintenanceTests(TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF

        debug = False

        cls.dbmanager = DBManager(**database_info, echo=debug).__enter__()

        cls.factory = DbTestFactory(cls.dbmanager)
        cls.plantfactory = DbPlantFactory(cls.dbmanager)


    @classmethod
    def tearDownClass(cls):
        cls.dbmanager.__exit__()

    def setUp(self):
        self.session = self.dbmanager.db_con.begin()
        self.dbmanager.db_con.execute('SET TIME ZONE UTC;')

    def tearDown(self):
        self.session.rollback()
        self.session.close()

    def create_plant(self, sunrise, sunset):
        # TODO tables already exist, why?
        self.plantfactory.create_inverter_sensor_plant(sunrise, sunset)

    def read_csv(self, csvfile):
        df = pd.read_csv(csvfile, sep=',')
        return df

    def test__timezone(self):
        time = self.dbmanager.db_con.execute('''
            set time zone 'UTC';
            select time from (values (1, '2021-01-01 10:00+00:00'::timestamptz)) as t (id, time);
            ''').fetchone()[0]

        self.assertEqual(time.tzinfo, datetime.timezone.utc)

    def test__update_bucketed_inverter_registry__base(self):
        try:
            self.factory.create('inverterregistry_factory_case1.csv', 'inverterregistry')
            self.factory.create_bucket_5min_inverterregistry_empty_table()

            to_date = datetime.datetime(2022,2,17,0,20, tzinfo=datetime.timezone.utc)
            result = update_bucketed_inverter_registry(self.dbmanager.db_con, to_date)
            result = pd.DataFrame(result, columns=['time', 'inverter', 'temperature_dc', 'power_w', 'energy_wh'])
            #result['time'] = result['time'].dt.tz_convert('UTC')

            expected = pd.read_csv('test_data/update_bucketed_inverter_registry__base.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
            pd.testing.assert_frame_equal(result, expected)

        except:
            self.factory.delete('inverterregistry')
            self.factory.delete('bucket_5min_inverterregistry')
            raise

    def test__update_bucketed_inverter_registry__DST(self):
        try:
            self.factory.create('inverterregistry_factory_case_horary_change.csv', 'inverterregistry')
            self.factory.create_bucket_5min_inverterregistry_empty_table()
            to_date = datetime.datetime(2021,3,27,1,15, tzinfo=datetime.timezone.utc)
            result = update_bucketed_inverter_registry(self.dbmanager.db_con, to_date=to_date)
            result = pd.DataFrame(result, columns=['time', 'inverter', 'temperature_dc', 'power_w', 'energy_wh'])
            expected = pd.read_csv('test_data/update_bucketed_inverter_registry_case_horary_change.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
            pd.testing.assert_frame_equal(result, expected)

        except:
            self.factory.delete('inverterregistry')
            self.factory.delete('bucket_5min_inverterregistry')
            raise

    def test__update_bucketed_inverter_registry__is_hypertable(self):
        try:
            self.factory.create('inverterregistry_factory_case1.csv', 'inverterregistry')

            to_date = datetime.datetime(2022,2,17,0,20, tzinfo=datetime.timezone.utc)
            update_bucketed_inverter_registry(self.dbmanager.db_con, to_date)

            result = self.dbmanager.db_con.execute('''
                SELECT * FROM _timescaledb_catalog.hypertable WHERE table_name = 'bucket_5min_inverterregistry';
            ''').fetchone()

            self.assertTrue(result)

        except:
            self.factory.delete('inverterregistry')
            self.factory.delete('bucket_5min_inverterregistry')
            raise

    def test__update_bucketed_string_registry__base(self):
        try:
            self.factory.create('stringregistry_factory_case1.csv', 'stringregistry')
            self.factory.create_bucket_5min_stringregistry_empty_table()

            to_date = datetime.datetime(2022,2,17,12,20, tzinfo=datetime.timezone.utc)
            result = update_bucketed_string_registry(self.dbmanager.db_con, to_date)
            result = pd.DataFrame(result, columns=['time', 'string', 'intensity_ma'])
            #result['time'] = result['time'].dt.tz_convert('UTC')

            expected = pd.read_csv('test_data/update_bucketed_string_registry__base.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
            pd.testing.assert_frame_equal(result, expected)

        except:
            self.factory.delete('stringregistry')
            self.factory.delete('bucket_5min_stringregistry')
            raise

    def test__alarm_maintenance__no_bucket_tables(self):

        alarm_maintenance(self.dbmanager.db_con)
