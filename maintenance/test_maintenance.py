#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from pathlib import Path
from unittest import TestCase

from .maintenance import (
    get_latest_reading,
    round_dt_to_5minutes,
    fill_holes,
    resample,
    update_alarm_meteorologic_station_maintenance_via_sql,
    update_bucketed_inverter_registry,
    update_alarm_nopower_inverter,
    create_alarm_table,
    create_alarm_status_table,
    create_alarm_historic_table,
    get_alarm_status_nopower_alarmed,
    get_alarm_current_nopower_inverter,
    set_new_alarm
)
from .db_manager import DBManager

import pandas as pd
from pandas.testing import assert_frame_equal

import datetime

from .db_test_factory import DbTestFactory


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

    def __test__update_alarm_inverter_maintenance_via_sql(self):

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
        dst_table_name = 'zero_inverter_power_at_daylight'
        self.dbmanager.db_con.execute('create table {} (like {}, alarm bool)'.format(dst_table_name, inverter_5m_table_name))

        new_records = update_alarm_inverter_maintenance_via_sql(self.dbmanager.db_con)

        expected = [(readingtime, 1,1, 0, sunrise, sunset)]
        self.assertListEqual(new_records, expected)

    def __test__update_alarm_meteorologic_station_maintenance_via_sql_with_zero_irradiation(self):

        readingtime = datetime.datetime(2021,1,1,12,tzinfo=datetime.timezone.utc)
        sensor_irraditation_5m_table_name = 'sensorirradiationregistry_5min_avg'
        self.dbmanager.db_con.execute('create table {} (sensor SERIAL, time timestamptz, irradiation_w_m2 integer, temperature_dc integer, PRIMARY KEY (sensor, time))'. format(sensor_irraditation_5m_table_name))
        self.dbmanager.db_con.execute(
            "insert into {}(time, irradiation_w_m2, temperature_dc) values('{}', {}, {}), ('{}', {}, {})".format(
                sensor_irraditation_5m_table_name,
                readingtime.strftime('%Y-%m-%d %H:%M:%S%z'), 0, 0,
                (readingtime+datetime.timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S%z'), 0, 15
            )
        )

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        dst_table_name = 'zero_sonda_irradiation_at_daylight'
        self.dbmanager.db_con.execute('create table {} (like {}, alarm bool)'.format(dst_table_name, sensor_irraditation_5m_table_name))

        new_records = update_alarm_meteorologic_station_maintenance_via_sql(self.dbmanager.db_con)

        expected = [(1, readingtime, 1, 0, 0, sunrise, sunset)]
        self.assertListEqual(new_records, expected)

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

    def _test__fill_holes(self):

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

    def _test__resample(self):

        df = pd.read_csv('test_data/df_test_round_minutes.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
        expected = df.copy()
        basetime = datetime.datetime(2021,3,26,23,15,0,tzinfo=datetime.timezone.utc)
        dt = datetime.timedelta(minutes=5)
        expected['time'] = [basetime, basetime + dt, basetime + 2*dt, basetime + 3*dt]

        result = resample(df)

        assert_frame_equal(result, expected, check_datetimelike_compat=True)

class InverterMaintenanceTests(TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF

        debug = False

        cls.dbmanager = DBManager(**database_info, echo=debug).__enter__()

        cls.factory = DbTestFactory(cls.dbmanager)


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
            result = update_bucketed_inverter_registry(self.dbmanager.db_con)
            result = pd.DataFrame(result, columns=['time', 'inverter', 'temperature_dc', 'power_w', 'energy_wh'])
            #result['time'] = result['time'].dt.tz_convert('UTC')

            expected = pd.read_csv('test_data/update_bucketed_inverter_registry_case1.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
            pd.testing.assert_frame_equal(result, expected)

        except:
            self.factory.delete('inverterregistry')
            self.factory.delete('bucket_5min_inverterregistry')
            raise

    def test__update_bucketed_inverter_registry__DST(self):
        try:
            self.factory.create('inverterregistry_factory_case_horary_change.csv', 'inverterregistry')
            self.factory.create_bucket_5min_inverterregistry_empty_table()
            result = update_bucketed_inverter_registry(self.dbmanager.db_con)
            result = pd.DataFrame(result, columns=['time', 'inverter', 'temperature_dc', 'power_w', 'energy_wh'])
            expected = pd.read_csv('test_data/update_bucketed_inverter_registry_case_horary_change.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
            pd.testing.assert_frame_equal(result, expected)

        except:
            self.factory.delete('inverterregistry')
            self.factory.delete('bucket_5min_inverterregistry')
            raise

    def test__create_alarm_table(self):
        create_alarm_table(self.dbmanager.db_con)
        result = self.dbmanager.db_con.execute('''
            SELECT id, name, description, severity, createdate
            FROM alarm
        ''')
        self.assertTrue(result)


    def test__create_alarm_status_table(self):
        create_alarm_table(self.dbmanager.db_con)
        create_alarm_status_table(self.dbmanager.db_con)
        result = self.dbmanager.db_con.execute('''
            SELECT device_table, device_id, device_name, alarm, create_date, update_date, status
            FROM alarm_status
        ''')
        import pdb; pdb.set_trace()
        self.assertTrue(result)

    def test__create_alarm_historic_table(self):
        create_alarm_table(self.dbmanager.db_con)
        create_alarm_historic_table(self.dbmanager.db_con)
        result = self.dbmanager.db_con.execute('''
            SELECT device_table, device_id, device_name, alarm, description, severity, started, ended, updated
            FROM alarm_historic
        ''')
        self.assertTrue(result)

    def create_alarm_nopower_inverter_tables(self):
        create_alarm_table(self.dbmanager.db_con)
        create_alarm_status_table(self.dbmanager.db_con)
        create_alarm_historic_table(self.dbmanager.db_con)

    def test__get_alarm_status_nopower_alarmed(self):
        self.create_alarm_nopower_inverter_tables()
        self.factory.create_without_time('input_alarm_status_nopower_alarmed.csv', 'alarm_status')
        result = get_alarm_status_nopower_alarmed(self.dbmanager.db_con, 'nopower', 'inverter', 1)
        import pdb; pdb.set_trace()
        self.assertTrue(result)

    def test__get_alarm_current_nopower_inverter__alarm_triggered(self):
        self.factory.create('input_get_alarm_current_nopower_inverter__alarm_triggered.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = '2022-02-17 00:24:55'
        result = get_alarm_current_nopower_inverter(self.dbmanager.db_con, check_time)

        expected = [(1, 'Alibaba_inverter', True)]
        self.assertListEqual(result, expected)

    def test__get_alarm_current_nopower_inverter__no_alarm(self):
        self.factory.create('input_get_alarm_current_nopower_inverter__no_alarm.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = '2022-02-17 00:24:55'
        result = get_alarm_current_nopower_inverter(self.dbmanager.db_con, check_time)

        expected = [(1, 'Alibaba_inverter', False)]
        self.assertListEqual(result, expected)

    def test__get_alarm_current_nopower_inverter__none_power_readings(self):
        self.factory.create('input_get_alarm_current_nopower_inverter__none_power_readings.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = '2022-02-17 00:24:55'
        result = get_alarm_current_nopower_inverter(self.dbmanager.db_con, check_time)

        expected = [(1, 'Alibaba_inverter', None)]
        self.assertListEqual(result, expected)

    #TODO theoretically should not happen because the gapfill is ran always. Should we throw?
    def _test__get_alarm_current_nopower_inverter__no_readings(self):
        self.factory.create('bucket_5min_inverterregistry_case1.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = '2022-02-17 00:24:55'
        result = get_alarm_current_nopower_inverter(self.dbmanager.db_con, check_time)

        expected = [(1, 'Alibaba_inverter', True)]
        self.assertListEqual(result, expected)

    def test__update_alarm_nopower_inverter(self):
        self.factory.create('bucket_5min_inverterregistry_case1.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()

        result = None#update_alarm_nopower_inverter()

        result = pd.DataFrame(result)
        # result = pd.DataFrame(result, columns=['time', 'inverter', 'temperature_dc', 'power_w', 'energy_wh'])
        expected = pd.read_csv('test_data/alarm_nopower_inverter_case1.csv', sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
        pd.testing.assert_frame_equal(result, expected)

    def test__set_new_alarm(self):

        alarm = {
            'name': 'nopower',
            'description': '',
            'severity': 'critical',
            'createdate': '2022-03-18'
        }

        result = set_new_alarm(self.dbmanager.db_con,alarm['name'],alarm['description'],alarm['severity'],alarm['createdate'])
        expected = [('nopower','', 'nopower', '2022-03-18')]

        self.assertListEqual(result, expected)
