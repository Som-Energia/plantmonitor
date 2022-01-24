#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from pathlib import Path
from unittest import TestCase

from .maintenance import (
    round_dt_to_5minutes,
    fill_holes,
    resample,
    alarm_maintenance_via_sql
)
from .db_manager import DBManager

import pandas as pd
from pandas.testing import assert_frame_equal

import datetime


class IrradiationDBConnectionTest(TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF
        db_info = database_info
        db_info['dbname'] = database_info['database']
        del db_info['provider']
        del db_info['database']

        debug = False

        cls.dbmanager = DBManager(**db_info, echo=debug)
        cls.dbmanager.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.dbmanager.__exit__()

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

    def test__alarm_maintenance_via_sql(self):

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
        result = alarm_maintenance_via_sql(self.dbmanager.db_con, query)

        results = self.dbmanager.db_con.execute('select * from {}'.format(dst_table_name)).fetchall()

        # check
        expected = [(readingtime, 0, True)]
        # check inserted results
        self.assertListEqual(results, expected)

    def _test__alarm_maintenance_via_sql__from_file(self):

        # setup
        # create source table
        readingtime = datetime.datetime(2021,1,1,12,tzinfo=datetime.timezone.utc)
        src_table_name = 'test_alarm_source'
        self.dbmanager.db_con.execute('create table {} (time timestamptz, power_w integer)'.format(src_table_name))
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
        query = Path('queries/zero_inverter_power_at_daylight.sql').read_text(encoding='utf8')
        result = alarm_maintenance_via_sql(self.dbmanager.db_con, query)

        results = self.dbmanager.db_con.execute('select * from {}'.format(dst_table_name)).fetchall()

        # check
        expected = [(readingtime, 0, True)]
        # check inserted results
        self.assertListEqual(results, expected)




class IrradiationMaintenanceTests(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # FYI reading data with mixed time shifts
    # https://stackoverflow.com/questions/21269399/datetime-dtypes-in-pandas-read-csv

    def test__round_minutes(self):

        df = pd.read_csv('test_data/df_test_round_minutes.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
        expected = df.copy()
        basetime = datetime.datetime(2021,3,26,23,15,0,tzinfo=datetime.timezone.utc)
        dt = datetime.timedelta(minutes=5)
        expected['time'] = [basetime, basetime + dt, basetime + 2*dt, basetime + 3*dt]

        result = round_dt_to_5minutes(df)

        assert_frame_equal(result, expected, check_datetimelike_compat=True)

    def test__duplicate_minutes(self):

        df = pd.read_csv('test_data/df_test_round_minutes_duplicates.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
        expected = pd.DataFrame({
            'sensor': [7]*4,
            'time': pd.Series([
                    '2021-03-27 11:15:00+00:00',
                    '2021-03-27 11:20:00+00:00',
                    '2021-03-27 11:25:00+00:00',
                    '2021-03-27 11:35:00+00:00'
                ]),
            'irradiation_w_m2': [465, 578, 234, 290],
            'temperature_dc': [119]*4
        })
        expected['time'] = pd.to_datetime(expected['time'])

        result = round_dt_to_5minutes(df)

        assert_frame_equal(result, expected, check_datetimelike_compat=True)

    def _test__duplicate_minutes__many_sensors(self):
        pass

    def test__fill_holes(self):

        df = pd.read_csv('test_data/df_test_fill_holes.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
        expected = pd.DataFrame({
            'sensor': [7]*5,
            'time': pd.Series([
                    '2021-03-27 11:15:00+00:00',
                    '2021-03-27 11:20:00+00:00',
                    '2021-03-27 11:25:00+00:00',
                    '2021-03-27 11:30:00+00:00',
                    '2021-03-27 11:35:00+00:00'
                ]),
            'irradiation_w_m2': [100, 125, 150, 175, 290],
            'temperature_dc': [119]*5
        })
        expected['time'] = pd.to_datetime(expected['time'])

        result = fill_holes(df)

        print(result)
        print(expected)

        assert_frame_equal(result, expected, check_datetimelike_compat=True)

    def test__resample(self):

        df = pd.read_csv('test_data/df_test_round_minutes.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
        expected = df.copy()
        basetime = datetime.datetime(2021,3,26,23,15,0,tzinfo=datetime.timezone.utc)
        dt = datetime.timedelta(minutes=5)
        expected['time'] = [basetime, basetime + dt, basetime + 2*dt, basetime + 3*dt]

        result = resample(df)

        assert_frame_equal(result, expected, check_datetimelike_compat=True)
