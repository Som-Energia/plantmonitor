#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'plantmonitor.conf.settings.testing')

import unittest

from pony import orm

import datetime

from .models import database
from .models import (
    Plant,
    Meter,
    MeterRegistry,
)

from .orm_util import *


class ORMSetup_Test(unittest.TestCase):

    def setUp(self):

        from plantmonitor.conf import config
        self.assertEqual(config.SETTINGS_MODULE, 'plantmonitor.conf.settings.testing')

        orm.rollback()
        self.maxDiff=None
        # try:
        #     database.bind(**databaseInfo)
        #     database.generate_mapping()
        #     print('mapping generated')
        # except orm.core.BindingError:
        #     print('Binding error')
        #     pass

        database.drop_all_tables(with_all_data=True)
        database.create_tables()
        # database.generate_mapping(create_tables=True)
        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()

    def test_Environment(self):
        #TODO will it be too late if the config is misconfigured?
        from plantmonitor.conf import config
        self.assertEqual(config.SETTINGS_MODULE, 'plantmonitor.conf.settings.testing')

    def test_Empty(self):
        with orm.db_session:
            self.assertEqual(database.get_connection().status, 1)

    def test_InsertOnePlantOneMeter(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', description='descripción de planta')
            meter = Meter(name='Mary', plant=alcolea)

    def test_InsertOnePlantOneMeterOneRegistry(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', description='descripción de planta')
            meter = Meter(name='Mary', plant=alcolea)
            meterRegistry = MeterRegistry(
                meter=meter,
                time=datetime.datetime.now(),
                export_energy=10,
                import_energy=77,
                r1=0,
                r2=0,
                r3=0,
                r4=0,
            )

    def test_ReadOnePlantOneMeterOneRegistry(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', description='A fotovoltaic plant')
            meter = Meter(name='Mary', plant=alcolea)
            meterRegistry = MeterRegistry(
                meter=meter,
                time=datetime.datetime.now(),
                export_energy=10,
                import_energy=77,
                r1=0,
                r2=0,
                r3=0,
                r4=0,
            )

            alcolea_read = Plant[1]
            self.assertEqual(alcolea_read, alcolea)
            self.assertEqual(alcolea_read.name, alcolea.name)
