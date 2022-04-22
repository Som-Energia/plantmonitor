#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from pathlib import Path
from unittest import TestCase, skipIf

from .db_manager import DBManager

import pandas as pd
from pandas.testing import assert_frame_equal

import datetime

from .db_test_factory import DbTestFactory, DbPlantFactory

from re import search

from .alarm import Alarm, NoSolarEventError
from .alarm_manager import AlarmManager

class AlarmTests(TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF

        debug = False

        cls.dbmanager = DBManager(**database_info, echo=debug).__enter__()

        cls.factory = DbTestFactory(cls.dbmanager)
        cls.plantfactory = DbPlantFactory(cls.dbmanager)
        cls.alarm_manager = AlarmManager(cls.dbmanager.db_con)

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

    def create_alarm_nopower_inverter_tables(self):
        self.alarm_manager.create_alarm_tables()
        alarms_yaml_content = { 'alarms':[{
                'name': 'nopowerinverter',
                'description': 'Inversor sense potència entre alba i posta',
                'severity': 'critical',
                'createdate': datetime.date.today()
            }]
        }
        self.alarm_manager.insert_alarms_from_config(alarms_yaml_content)

    def create_alarm_nointensity_string_tables(self):
        self.alarm_manager.create_alarm_tables()
        alarms_yaml_content = { 'alarms':[{
                'name': 'nointensitystring',
                'description': "String sense intensitat durant potència d'inversor",
                'severity': 'critical',
                'createdate': datetime.date.today()
            }]
        }
        self.alarm_manager.insert_alarms_from_config(alarms_yaml_content)


    def test__get_alarm_status_nopower_alarmed(self):
        self.factory.create_without_time('input__alarm_status_nopower_alarmed.csv', 'alarm_status')
        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        result = alarm.get_alarm_status_nopower_alarmed(1, 'inverter', 1)

        self.assertTrue(result)

    def test__get_alarm_current_nopower_inverter__alarm_triggered(self):
        self.factory.create('input__get_alarm_current_nopower_inverter__alarm_triggered.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,15,0,tzinfo=datetime.timezone.utc)
        result = alarm.get_alarm_current_nopower_inverter(check_time)

        expected = [(1, 'Alibaba_inverter', True)]
        self.assertListEqual(result, expected)

    def test__get_alarm_current_nopower_inverter__no_alarm(self):
        self.factory.create('input__get_alarm_current_nopower_inverter__no_alarm.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,24,55,tzinfo=datetime.timezone.utc)
        result = alarm.get_alarm_current_nopower_inverter(check_time)

        expected = [(1, 'Alibaba_inverter', False)]
        self.assertListEqual(result, expected)

    def test__get_alarm_current_nopower_inverter__none_power_readings(self):
        self.factory.create('input__get_alarm_current_nopower_inverter__none_power_readings.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,24,55,tzinfo=datetime.timezone.utc)
        result = alarm.get_alarm_current_nopower_inverter(check_time)

        expected = [(1, 'Alibaba_inverter', True), (2, 'Quaranta_Lladres_inverter', None)]
        self.assertListEqual(result, expected)

    #TODO theoretically should not happen because the gapfill is ran always. Should we throw?
    def _test__get_alarm_current_nopower_inverter__no_readings(self):
        self.factory.create('bucket_5min_inverterregistry_case1.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = '2022-02-17 00:24:55'
        result = alarm.get_alarm_current_nopower_inverter(check_time)

        expected = [(1, 'Alibaba_inverter', True)]
        self.assertListEqual(result, expected)

    def test__set_alarm_status__new_alarm(self):

        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        alarm_status = {
            'device_table':'inverter',
            'device_id':1,
            'device_name':'Inverter1',
            'alarm':1,
            'update_time':datetime.datetime(2022,3,21,12,14,tzinfo=datetime.timezone.utc),
            'status':True
        }

        result = alarm.set_alarm_status(**alarm_status)
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

    def test__set_alarm_status__OK_to_OK(self):

        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        alarm_status = self.insert_alarm_status()
        old_start_time = alarm_status['update_time']
        alarm_status['update_time'] = alarm_status['update_time']+datetime.timedelta(hours=1)

        result = alarm.set_alarm_status(
            **alarm_status
        )
        expected = {'id':1, **alarm_status, 'start_time': old_start_time, 'old_start_time': old_start_time, 'old_status': False}
        self.assertDictEqual(dict(result), expected)

    def test__set_alarm_status__OK_to_NOK(self):

        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        alarm_status = self.insert_alarm_status()
        old_start_time = alarm_status['update_time']
        alarm_status['update_time'] = alarm_status['update_time']+datetime.timedelta(hours=1)
        alarm_status['status'] = True

        result = alarm.set_alarm_status(
            **alarm_status
        )

        expected = {'id':1, **alarm_status, 'start_time': alarm_status['update_time'], 'old_start_time': old_start_time, 'old_status': False}
        self.assertDictEqual(dict(result), expected)

    def test__set_alarm_status__NOK_to_NOK(self):

        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        alarm_status = self.insert_alarm_status(status=True)
        old_start_time = alarm_status['update_time']
        alarm_status['update_time'] = old_start_time+datetime.timedelta(hours=1)
        alarm_status['status'] = True
        result = alarm.set_alarm_status(
            **alarm_status
        )
        expected = {'id':1, **alarm_status, 'start_time': old_start_time, 'old_start_time': old_start_time, 'old_status': True}
        self.assertDictEqual(dict(result), expected)

    def test__set_alarm_status__NOK_to_None(self):

        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        alarm_status = self.insert_alarm_status(status=True)

        old_start_time = alarm_status['update_time']
        alarm_status['update_time'] = old_start_time+datetime.timedelta(hours=1)
        alarm_status['status'] = None
        result = alarm.set_alarm_status(
            **alarm_status
        )
        expected = {'id':1, **alarm_status, 'start_time': alarm_status['update_time'], 'old_start_time': old_start_time, 'old_status': True}
        self.assertDictEqual(dict(result), expected)

    def test__set_alarm_status__None_to_None(self):

        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        alarm_status = self.insert_alarm_status(status=None)

        old_start_time = alarm_status['update_time']
        alarm_status['update_time'] = old_start_time+datetime.timedelta(hours=1)
        alarm_status['status'] = None
        result = alarm.set_alarm_status(
            **alarm_status
        )
        expected = {'id':1, **alarm_status, 'start_time': old_start_time, 'old_start_time': old_start_time, 'old_status': None}
        self.assertDictEqual(dict(result), expected)


    def test__set_new_alarm__base(self):
        self.alarm_manager.create_alarm_table()

        alarm = {
            'name': 'invent',
            'description': '',
            'severity': 'critical',
            'active': True,
            'createdate': datetime.datetime(2022,3,18)
        }

        alarm = Alarm(self.dbmanager.db_con, **alarm)

        expected = 1

        self.assertEqual(alarm.id, expected)

    def test__set_new_alarm__sql_injection(self):
        self.alarm_manager.create_alarm_table()

        alarm = {
            'name': "foo\'; TRUNCATE TABLE alarm; select 1 where ''=\'",
            'description': '',
            'severity': 'critical',
            'createdate': datetime.datetime(2022,3,18)
        }

        alarm = Alarm(self.dbmanager.db_con, **alarm)

        num_alarms = self.dbmanager.db_con.execute("select count(*) from alarm limit 1;").fetchone()
        self.assertEqual(num_alarms, (1,))
        alarm_expected = self.dbmanager.db_con.execute("select name from alarm order by name;").fetchone()
        self.assertEqual(alarm_expected, ("foo'; TRUNCATE TABLE alarm; select 1 where ''='",))

    def test__set_alarm_historic(self):

        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        alarm_historic = {
            'device_table': 'invent',
            'device_id': 1,
            'device_name': 'critical',
            'alarm': 1,
            'start_time': datetime.datetime(2022,3,21,12,14,tzinfo=datetime.timezone.utc),
            'end_time': datetime.datetime(2022,3,22,23,34,tzinfo=datetime.timezone.utc)
        }

        result = alarm.set_alarm_historic(**alarm_historic)

        expected = {'id': 1, **alarm_historic}
        self.assertDictEqual(dict(result), expected)

    def test__update_alarm_nopower_inverter__new_alarm(self):
        self.factory.create('input__get_alarm_current_nopower_inverter__alarm_triggered.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        sunrise = datetime.datetime(2022,2,17,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2022,2,17,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)
        alarm.update_alarm_nopower_inverter(check_time)

        result = self.dbmanager.db_con.execute('select * from alarm_status').fetchall()
        expected = (1, 'inverter', 1, 'Alibaba_inverter', 1, datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), True)

        self.assertEqual(len(result), 1)
        self.assertTupleEqual(tuple(result[0]), expected)

    def test__update_alarm_nopower_inverter__status_change(self):
        self.factory.create('input__get_alarm_current_nopower_inverter__change_status.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        sunrise = datetime.datetime(2022,2,17,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2022,2,17,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)
        alarm.update_alarm_nopower_inverter(check_time)

        check_time = check_time + datetime.timedelta(minutes=5)
        alarm.update_alarm_nopower_inverter(check_time)

        result = self.dbmanager.db_con.execute('select * from alarm_status').fetchall()
        expected = (1, 'inverter', 1, 'Alibaba_inverter', 1, datetime.datetime(2022, 2, 17, 13, 20, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 2, 17, 13, 20, tzinfo=datetime.timezone.utc), False)

        self.assertEqual(len(result), 1)
        self.assertTupleEqual(tuple(result[0]), expected)

        result = self.dbmanager.db_con.execute('select * from alarm_historic').fetchall()
        expected = (1, 'inverter', 1, 'Alibaba_inverter', 1, datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 2, 17, 13, 20, tzinfo=datetime.timezone.utc))

        self.assertEqual(len(result), 1)
        self.assertTupleEqual(tuple(result[0]), expected)

    def test__update_alarm_nopower_inverter__none_power_readings(self):
        self.factory.create('input__get_alarm_current_nopower_inverter__none_power_readings.csv', 'bucket_5min_inverterregistry')
        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        sunrise = datetime.datetime(2022,2,17,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2022,2,17,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)
        alarm.update_alarm_nopower_inverter(check_time)

        result = self.dbmanager.db_con.execute('select * from alarm_status').fetchall()
        expected = (1, 'inverter', 1, 'Alibaba_inverter', 1, datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), True)

        self.assertEqual(len(result), 2)
        self.assertTupleEqual(tuple(result[0]), expected)

    def test__is_daylight__night(self):

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2021,1,1,3,15,tzinfo=datetime.timezone.utc)
        result = Alarm.is_daylight(self.dbmanager.db_con, 'inverter', 1, check_time)

        self.assertEqual(result, False)

    def test__is_daylight__day(self):

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2021,1,1,12,15,tzinfo=datetime.timezone.utc)
        result = Alarm.is_daylight(self.dbmanager.db_con, 'inverter', 1, check_time)

        self.assertEqual(result, True)

    def test__is_daylight__not_found(self):

        sunrise = datetime.datetime(2021,1,1,8,tzinfo=datetime.timezone.utc)
        sunset = datetime.datetime(2021,1,1,18,tzinfo=datetime.timezone.utc)
        self.create_plant(sunrise, sunset)

        check_time = datetime.datetime(2022,3,12,12,15,tzinfo=datetime.timezone.utc)

        with self.assertRaises(NoSolarEventError):
            Alarm.is_daylight(self.dbmanager.db_con, 'inverter', 1, check_time)

    def test__get_alarm_by_name__base(self):
        self.create_alarm_nointensity_string_tables()
        alarm = self.alarm_manager.get_alarm_by_name('nointensitystring')
        self.assertIsNotNone(alarm)

    def test__set_alarm_status_update_time__empty(self):

        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        alarm_status = {
            'device_table': 'inverter',
            'device_id': 1,
            'alarm': 1
        }

        check_time = datetime.datetime(2022,1,1,tzinfo=datetime.timezone.utc)

        result = alarm.set_alarm_status_update_time(**alarm_status, check_time=check_time)

        self.assertIsNone(result)

    def test__set_alarm_status_update_time__base(self):

        self.create_alarm_nopower_inverter_tables()
        alarm = self.alarm_manager.alarms[0]

        self.factory.create_without_time('input__alarm_status_nopower_alarmed.csv', 'alarm_status')

        alarm_status = {
            'device_table': 'inverter',
            'device_id': 1,
            'alarm': 1
        }

        check_time = datetime.datetime(2022,2,17,1,15,0,tzinfo=datetime.timezone.utc)

        result = alarm.set_alarm_status_update_time(**alarm_status, check_time=check_time)

        self.assertEqual(result['update_time'], check_time)
        self.assertTrue(result['status'])

    def test__update_alarm_nointensity_string__new_alarm(self):
        self.factory.create('input__get_alarm_current_nopower_inverter__no_alarm.csv', 'bucket_5min_inverterregistry')
        self.factory.create('input__get_alarm_current_nointensity_string__alarm_triggered.csv', 'bucket_5min_stringregistry')
        self.create_alarm_nointensity_string_tables()
        alarm = self.alarm_manager.get_alarm_by_name('nointensitystring')

        self.plantfactory.create_inverter_string_plant()

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)
        alarm.update_alarm_nointensity_string(check_time)

        result = self.dbmanager.db_con.execute('select * from alarm_status').fetchall()
        expected = (1, 'string', 1, 'string1', 1, datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), datetime.datetime(2022, 2, 17, 13, 15, tzinfo=datetime.timezone.utc), True)

        self.assertEqual(len(result), 1)
        self.assertTupleEqual(tuple(result[0]), expected)

    def test__get_alarm_current_nointensity_string__alarm_ok(self):
        self.plantfactory.create_inverter_string_plant()
        self.factory.create('input__get_alarm_current_nopower_inverter__no_alarm.csv', 'bucket_5min_inverterregistry')
        self.factory.create('input__get_alarm_current_nointensity_string__alarm_ok.csv', 'bucket_5min_stringregistry')

        self.create_alarm_nointensity_string_tables()
        alarm = self.alarm_manager.get_alarm_by_name('nointensitystring')

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)

        result = alarm.get_alarm_current_nointensity_string(check_time=check_time)

        expected = [(1, 'string1', False)]

        self.assertListEqual(result, expected)

    def test__get_alarm_current_nointensity_string__alarm_triggered(self):
        self.plantfactory.create_inverter_string_plant()
        self.factory.create('input__get_alarm_current_nopower_inverter__no_alarm.csv', 'bucket_5min_inverterregistry')
        self.factory.create('input__get_alarm_current_nointensity_string__alarm_triggered.csv', 'bucket_5min_stringregistry')

        self.create_alarm_nointensity_string_tables()
        alarm = self.alarm_manager.get_alarm_by_name('nointensitystring')

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)

        result = alarm.get_alarm_current_nointensity_string(check_time=check_time)

        expected = [(1, 'string1', True)]

        self.assertListEqual(result, expected)

    def test__get_alarm_current_nointensity_string__only_string_inverter_joined(self):
        self.plantfactory.create_inverter_string_plant()
        self.factory.create('input__get_alarm_current_nopower_inverter__two_inverters_readings.csv', 'bucket_5min_inverterregistry')
        self.factory.create('input__get_alarm_current_nointensity_string__alarm_triggered.csv', 'bucket_5min_stringregistry')

        self.create_alarm_nointensity_string_tables()
        alarm = self.alarm_manager.get_alarm_by_name('nointensitystring')

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)

        result = alarm.get_alarm_current_nointensity_string(check_time=check_time)

        expected = [(1, 'string1', True)]

        self.assertListEqual(result, expected)

    def test__get_alarm_current_nointensity_string__none_inverter_readings(self):
        self.plantfactory.create_inverter_string_plant()
        self.factory.create('input__get_alarm_current_nointensity_inverter__none_inverter_power_readings.csv', 'bucket_5min_inverterregistry')
        self.factory.create('input__get_alarm_current_nointensity_string__alarm_triggered.csv', 'bucket_5min_stringregistry')

        self.create_alarm_nointensity_string_tables()
        alarm = self.alarm_manager.get_alarm_by_name('nointensitystring')

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)

        result = alarm.get_alarm_current_nointensity_string(check_time=check_time)

        expected = [(1, 'string1', None)]

        self.assertListEqual(result, expected)

    def test__get_alarm_current_nointensity_string__many_strings_many_inverter_all_casesjoined(self):
        self.plantfactory.create_inverter_string_plant()
        self.factory.create('input__get_alarm_current_nopower_inverter__two_inverters_readings.csv', 'bucket_5min_inverterregistry')
        self.factory.create('input__get_alarm_current_nointensity_string__alarm_triggered.csv', 'bucket_5min_stringregistry')

        self.create_alarm_nointensity_string_tables()
        alarm = self.alarm_manager.get_alarm_by_name('nointensitystring')

        check_time = datetime.datetime(2022,2,17,13,15,tzinfo=datetime.timezone.utc)

        result = alarm.get_alarm_current_nointensity_string(check_time=check_time)

        expected = [(1, 'string1', True)]

        self.assertListEqual(result, expected)