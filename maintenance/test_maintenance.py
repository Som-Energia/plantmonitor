#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from pathlib import Path
from unittest import TestCase, skipIf

from .maintenance import (
    get_latest_reading,
    update_bucketed_inverter_registry,
    update_alarm_nopower_inverter,
    create_alarm_table,
    create_alarm_status_table,
    create_alarm_historic_table,
    get_alarm_status_nopower_alarmed,
    get_alarm_current_nopower_inverter,
    set_new_alarm,
    set_alarm_status,
    set_alarm_historic,
    set_alarm_status_update_time,
    is_daylight,
    NoSolarEventError
)
from .db_manager import DBManager

import pandas as pd
from pandas.testing import assert_frame_equal

import datetime

from .db_test_factory import DbTestFactory

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

    def test__create_alarm_table__base(self):
        create_alarm_table(self.dbmanager.db_con)
        result = self.dbmanager.db_con.execute('''
            SELECT id, name, description, severity, createdate
            FROM alarm
        ''')
        self.assertTrue(result)


    def test__create_alarm_table__twice(self):
        create_alarm_table(self.dbmanager.db_con)
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
            SELECT device_table, device_id, device_name, alarm, start_time, update_time, status
            FROM alarm_status
        ''')
        self.assertTrue(result)

    def test__create_alarm_historic_table__base_case(self):
        create_alarm_table(self.dbmanager.db_con)
        create_alarm_historic_table(self.dbmanager.db_con)
        result = self.dbmanager.db_con.execute('''
            SELECT device_table, device_id, device_name, alarm, start_time, end_time
            FROM alarm_historic
        ''')
        self.assertTrue(result)
    
    def test__create_alarm_historic_table_is_hypertable__no_timescale(self):
        self.dbmanager.db_con.execute('''
            DROP EXTENSION IF EXISTS timescaledb;
        ''')
        create_alarm_table(self.dbmanager.db_con)
        create_alarm_historic_table(self.dbmanager.db_con)
        has_timescale = bool(self.dbmanager.db_con.execute('''
            SELECT extversion
            FROM pg_extension
            where extname = 'timescaledb';
        ''').fetchone())
        self.assertFalse(has_timescale)

        result = self.dbmanager.db_con.execute('''
            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE  schemaname = 'public'
                AND    tablename  = 'alarm_historic'
            );
        ''')
        self.assertTrue(result)

    def test__create_alarm_historic_table_is_hypertable__timescale(self):
        create_alarm_table(self.dbmanager.db_con)
        create_alarm_historic_table(self.dbmanager.db_con)
        result = self.dbmanager.db_con.execute('''
            SELECT * FROM _timescaledb_catalog.hypertable WHERE table_name = 'alarm_historic';
        ''')
        self.assertTrue(result)

    def create_alarm_nopower_inverter_tables(self):
        create_alarm_table(self.dbmanager.db_con)

        nopower_alarm = {
            'name': 'nopowerinverter',
            'description': 'Inversor sense potència entre alba i posta',
            'severity': 'critical',
            'createdate': datetime.date.today()
        }
        set_new_alarm(db_con=self.dbmanager.db_con, **nopower_alarm)
        create_alarm_status_table(self.dbmanager.db_con)
        create_alarm_historic_table(self.dbmanager.db_con)

    def test__get_alarm_status_nopower_alarmed(self):
        self.create_alarm_nopower_inverter_tables()
        self.factory.create_without_time('input_alarm_status_nopower_alarmed.csv', 'alarm_status')
        result = get_alarm_status_nopower_alarmed(self.dbmanager.db_con, 1, 'inverter', 1)
        self.assertTrue(result)

    def test__get_alarm_current_nopower_inverter__alarm_triggered(self):
        self.factory.create('input_get_alarm_current_nopower_inverter__alarm_triggered.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,15,0,tzinfo=datetime.timezone.utc)
        result = get_alarm_current_nopower_inverter(self.dbmanager.db_con, check_time)

        expected = [(1, 'Alibaba_inverter', True)]
        self.assertListEqual(result, expected)

    def test__get_alarm_current_nopower_inverter__no_alarm(self):
        self.factory.create('input_get_alarm_current_nopower_inverter__no_alarm.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,0,24,55,tzinfo=datetime.timezone.utc)
        result = get_alarm_current_nopower_inverter(self.dbmanager.db_con, check_time)

        expected = [(1, 'Alibaba_inverter', False)]
        self.assertListEqual(result, expected)

    def test__get_alarm_current_nopower_inverter__none_power_readings(self):
        self.factory.create('input_get_alarm_current_nopower_inverter__none_power_readings.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,0,24,55,tzinfo=datetime.timezone.utc)
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

    def test__set_alarm_status__new_alarm(self):

        self.create_alarm_nopower_inverter_tables()

        alarm_status = {
            'device_table':'inverter',
            'device_id':1,
            'device_name':'Inverter1',
            'alarm':1,
            'update_time':datetime.datetime(2022,3,21,12,14,tzinfo=datetime.timezone.utc),
            'status':True
        }

        result = set_alarm_status(
            self.dbmanager.db_con,
            **alarm_status
        )
        expected = {'id':1, **alarm_status, 'start_time': alarm_status['update_time'], 'old_start_time': None, 'old_status': None}
        self.assertDictEqual(dict(result), expected)

    def insert_alarm_status(
        self,
        device_table='inverter',
        device_id=1,
        device_name='Inverter1',
        alarm=1,
        start_time=datetime.datetime(2022,3,21,12,14,tzinfo=datetime.timezone.utc),
        status=False
    ):
        query = f'''
            INSERT INTO
            alarm_status (
                device_table,
                device_id,
                device_name,
                alarm,
                start_time,
                update_time,
                status
            )
            VALUES('{device_table}','{device_id}','{device_name}','{alarm}','{start_time}', '{start_time}', '{status}')
        RETURNING
            device_table, device_id, device_name, alarm, update_time, status;
        '''
        return dict(self.dbmanager.db_con.execute(query).fetchone())

    def test__set_alarm_status__OK_to_OK(self):

        self.create_alarm_nopower_inverter_tables()

        alarm_status = self.insert_alarm_status()
        old_start_time = alarm_status['update_time']
        alarm_status['update_time'] = alarm_status['update_time']+datetime.timedelta(hours=1)

        result = set_alarm_status(
            self.dbmanager.db_con,
            **alarm_status
        )
        expected = {'id':1, **alarm_status, 'start_time': old_start_time, 'old_start_time': old_start_time, 'old_status': False}
        self.assertDictEqual(dict(result), expected)

    def test__set_alarm_status__OK_to_NOK(self):

        self.create_alarm_nopower_inverter_tables()

        alarm_status = self.insert_alarm_status()
        old_start_time = alarm_status['update_time']
        alarm_status['update_time'] = alarm_status['update_time']+datetime.timedelta(hours=1)
        alarm_status['status'] = True

        result = set_alarm_status(
            self.dbmanager.db_con,
            **alarm_status
        )

        expected = {'id':1, **alarm_status, 'start_time': alarm_status['update_time'], 'old_start_time': old_start_time, 'old_status': False}
        self.assertDictEqual(dict(result), expected)

    def test__set_alarm_status__NOK_to_NOK(self):

        self.create_alarm_nopower_inverter_tables()

        alarm_status = self.insert_alarm_status(status=True)
        old_start_time = alarm_status['update_time']
        alarm_status['update_time'] = old_start_time+datetime.timedelta(hours=1)
        alarm_status['status'] = True
        result = set_alarm_status(
            self.dbmanager.db_con,
            **alarm_status
        )
        expected = {'id':1, **alarm_status, 'start_time': old_start_time, 'old_start_time': old_start_time, 'old_status': True}
        self.assertDictEqual(dict(result), expected)

    def test__set_new_alarm(self):
        create_alarm_table(self.dbmanager.db_con)

        alarm = {
            'name': 'invent',
            'description': '',
            'severity': 'critical',
            'createdate': datetime.datetime(2022,3,18)
        }

        result = set_new_alarm(self.dbmanager.db_con,alarm['name'],alarm['description'],alarm['severity'],alarm['createdate'])
        expected = 1

        self.assertEqual(result, expected)

    def test__set_alarm_historic(self):

        self.create_alarm_nopower_inverter_tables()

        alarm_historic = {
            'device_table': 'invent',
            'device_id': 1,
            'device_name': 'critical',
            'alarm': 1,
            'start_time': datetime.datetime(2022,3,21,12,14,tzinfo=datetime.timezone.utc),
            'end_time': datetime.datetime(2022,3,22,23,34,tzinfo=datetime.timezone.utc)
        }

        result = set_alarm_historic(self.dbmanager.db_con, **alarm_historic)

        expected = {'id': 1, **alarm_historic}
        self.assertDictEqual(dict(result), expected)

    def test__update_alarm_nopower_inverter__new_alarm(self):
        self.factory.create('input_get_alarm_current_nopower_inverter__alarm_triggered.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        sunrise = datetime.datetime(2022,2,17,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2022,2,17,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)
        update_alarm_nopower_inverter(self.dbmanager.db_con, check_time)

        result = self.dbmanager.db_con.execute('select * from alarm_status').fetchall()
        expected = (1, 'inverter', 1, 'Alibaba_inverter', 1, datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), True)

        self.assertEqual(len(result), 1)
        self.assertTupleEqual(tuple(result[0]), expected)

    def test__update_alarm_nopower_inverter__status_change(self):
        self.factory.create('input_get_alarm_current_nopower_inverter__change_status.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        sunrise = datetime.datetime(2022,2,17,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2022,2,17,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)
        update_alarm_nopower_inverter(self.dbmanager.db_con, check_time)

        check_time = check_time + datetime.timedelta(minutes=5)
        update_alarm_nopower_inverter(self.dbmanager.db_con, check_time)

        result = self.dbmanager.db_con.execute('select * from alarm_status').fetchall()
        expected = (1, 'inverter', 1, 'Alibaba_inverter', 1, datetime.datetime(2022, 2, 17, 13, 20, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 2, 17, 13, 20, tzinfo=datetime.timezone.utc), False)

        self.assertEqual(len(result), 1)
        self.assertTupleEqual(tuple(result[0]), expected)

        result = self.dbmanager.db_con.execute('select * from alarm_historic').fetchall()
        expected = (1, 'inverter', 1, 'Alibaba_inverter', 1, datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 2, 17, 13, 20, tzinfo=datetime.timezone.utc))

        self.assertEqual(len(result), 1)
        self.assertTupleEqual(tuple(result[0]), expected)

    def test__update_alarm_nopower_inverter__none_power_readings(self):
        self.factory.create('input_get_alarm_current_nopower_inverter__none_power_readings.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        sunrise = datetime.datetime(2022,2,17,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2022,2,17,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)
        update_alarm_nopower_inverter(self.dbmanager.db_con, check_time)

        result = self.dbmanager.db_con.execute('select * from alarm_status').fetchall()
        expected = (1, 'inverter', 1, 'Alibaba_inverter', 1, datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), True)

        self.assertEqual(len(result), 1)
        self.assertTupleEqual(tuple(result[0]), expected)

    def test__is_daylight__night(self):

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2021,1,1,3,15,tzinfo=datetime.timezone.utc)
        result = is_daylight(self.dbmanager.db_con, 1, check_time)

        self.assertEqual(result, False)

    def test__is_daylight__day(self):

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2021,1,1,12,15,tzinfo=datetime.timezone.utc)
        result = is_daylight(self.dbmanager.db_con, 1, check_time)

        self.assertEqual(result, True)

    def test__is_daylight__not_found(self):

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,3,12,12,15,tzinfo=datetime.timezone.utc)

        with self.assertRaises(NoSolarEventError):
            is_daylight(self.dbmanager.db_con, 1, check_time)
    
    def test__set_alarm_status_update_time__empty(self):

        self.create_alarm_nopower_inverter_tables()
        
        alarm_status = {
            'device_table': 'inverter',
            'device_id': 1,
            'alarm': 1
        }

        check_time = datetime.datetime(2022,1,1,tzinfo=datetime.timezone.utc)

        result = set_alarm_status_update_time(self.dbmanager.db_con, **alarm_status, check_time=check_time)

        self.assertIsNone(result)
    
    def test__set_alarm_status_update_time__base(self):
        
        self.create_alarm_nopower_inverter_tables()
        self.factory.create_without_time('input_alarm_status_nopower_alarmed.csv', 'alarm_status')
        
        alarm_status = {
            'device_table': 'inverter',
            'device_id': 1,
            'alarm': 1
        }

        check_time = datetime.datetime(2022,2,17,1,15,0,tzinfo=datetime.timezone.utc)

        result = set_alarm_status_update_time(self.dbmanager.db_con, **alarm_status, check_time=check_time)

        self.assertEqual(result['update_time'], check_time)
        self.assertTrue(result['status'])