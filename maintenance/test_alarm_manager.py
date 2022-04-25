#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

from maintenance.alarm import Alarm
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import datetime

from unittest import TestCase, skipIf

from .db_manager import DBManager

from .db_test_factory import DbTestFactory, DbPlantFactory

from maintenance.alarm_manager import AlarmManager


class AlarmManagerTests(TestCase):

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

    def test__timezone(self):
        time = self.dbmanager.db_con.execute('''
            set time zone 'UTC';
            select time from (values (1, '2021-01-01 10:00+00:00'::timestamptz)) as t (id, time);
            ''').fetchone()[0]

        self.assertEqual(time.tzinfo, datetime.timezone.utc)

    def test__create_alarm_table__base(self):
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.create_alarm_table()
        result = self.dbmanager.db_con.execute('''
            SELECT id, name, description, severity, createdate
            FROM alarm
        ''')
        self.assertTrue(result)


    def test__create_alarm_table__twice(self):
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.create_alarm_table()
        alarm_manager.create_alarm_table()
        result = self.dbmanager.db_con.execute('''
            SELECT id, name, description, severity, createdate
            FROM alarm
        ''')
        self.assertTrue(result)


    def test__create_alarm_status_table(self):
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.create_alarm_table()
        alarm_manager.create_alarm_status_table()
        result = self.dbmanager.db_con.execute('''
            SELECT device_table, device_id, device_name, alarm, start_time, update_time, status
            FROM alarm_status
        ''')
        self.assertTrue(result)

    def test__create_alarm_historic_table__base_case(self):
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.create_alarm_table()
        alarm_manager.create_alarm_historic_table()
        result = self.dbmanager.db_con.execute('''
            SELECT device_table, device_id, device_name, alarm, start_time, end_time
            FROM alarm_historic
        ''')
        self.assertTrue(result)

    @skipIf(True, 'You must be superuser to drop extensions')
    def test__create_alarm_historic_table_is_hypertable__no_timescale(self):
        self.dbmanager.db_con.execute('''
            DROP EXTENSION IF EXISTS timescaledb;
        ''')
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.create_alarm_table()
        alarm_manager.create_alarm_historic_table()
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
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.create_alarm_table()
        alarm_manager.create_alarm_historic_table()
        result = self.dbmanager.db_con.execute('''
            SELECT * FROM _timescaledb_catalog.hypertable WHERE table_name = 'alarm_historic';
        ''')
        self.assertTrue(result)

    def create_alarm_nopower_inverter_tables(self):
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.create_alarm_table()
        alarms_yaml_content = { 'alarms':[{
                'name': 'noinverterpower',
                'description': 'Inversor sense potència entre alba i posta',
                'severity': 'critical',
                'createdate': datetime.date.today()
            }]
        }
        alarm_manager.insert_alarms_from_config(alarms_yaml_content)

    def create_alarm_nointensity_string_tables(self):
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.create_alarm_table()
        alarms_yaml_content = { 'alarms':[{
                'name': 'nostringintensity',
                'description': "String sense intensitat durant potència d'inversor",
                'severity': 'critical',
                'createdate': datetime.date.today()
            }]
        }
        alarm_manager.insert_alarms_from_config(alarms_yaml_content)

    def insert_alarm_status(
        self,
        device_table='inverter',
        device_id=1,
        device_name='Inverter1',
        alarm=1,
        start_time=datetime.datetime(2022,3,21,12,14,tzinfo=datetime.timezone.utc),
        status=False
    ):
        status = 'NULL' if status is None else status

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
            VALUES('{device_table}','{device_id}','{device_name}','{alarm}','{start_time}', '{start_time}', {status})
        RETURNING
            device_table, device_id, device_name, alarm, update_time, status;
        '''
        return dict(self.dbmanager.db_con.execute(query).fetchone())

    def test__read_alarms__base(self):
        self.maxDiff=None
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarms = alarm_manager.read_alarms_config('test_data/alarms_testing.yaml')

        expected_alarms = {'alarms': sorted([
            {
            'name': 'noinverterpower',
            'description': 'Inversor sense potència entre alba i posta',
            'severity': 'critical',
            'active': True,
            'sql': 'noinverterpower'
            },{
            'name': 'nostringintensity',
            'description': "String d'inversor sense potència amb potència inversor > 10 kw",
            'severity': 'critical',
            'active': True,
            'sql': 'nostringintensity'
            }
        ],key=lambda d: d['name'])}

        self.assertListEqual(sorted(alarms['alarms'], key=lambda d: d['name']), expected_alarms['alarms'])

    def test__insert_alarms_from_config__base(self):
        alarm_manager = AlarmManager(self.dbmanager.db_con)

        alarms_yaml_file = 'test_data/alarms_testing.yaml'
        alarms_yaml_content = alarm_manager.read_alarms_config(alarms_yaml_file)

        alarm_manager.create_alarm_table()

        alarm_manager.insert_alarms_from_config(alarms_yaml_content)

        alarms = self.dbmanager.db_con.execute('select name from alarm order by name;').fetchall()

        self.assertListEqual(alarms, [('noinverterpower',), ('nostringintensity',)])

    def test__update_alarms__no_bucket_tables(self):
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.update_alarms()

    def test__update_alarms__base(self):
        sunrise = datetime.datetime(2022,2,17,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2022,2,17,18,tzinfo=datetime.timezone.utc)
        self.plantfactory.create_inverter_string_plant_with_solar_events(sunrise, sunset)

        self.factory.create('input__get_alarm_current_nointensity_inverter__none_inverter_power_readings.csv', 'bucket_5min_inverterregistry')
        self.factory.create('input__get_alarm_current_nointensity_string__alarm_triggered.csv', 'bucket_5min_stringregistry')


        check_time = datetime.datetime(2022,2,17,13,15,0,tzinfo=datetime.timezone.utc)
        alarm_manager = AlarmManager(self.dbmanager.db_con)
        alarm_manager.update_alarms(check_time=check_time)