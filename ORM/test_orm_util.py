#!/usr/bin/env python3

import unittest

from pony import orm

import datetime

from .models import database
from .models import (
    Plant,
    Meter,
    MeterRegistry,
)

from ..conf.config_test import databaseInfo

from .orm_util import *

class ORMSetup_Test(unittest.TestCase):


    def setUp(self):
        orm.rollback()
        self.maxDiff=None
        try:
            database.bind(**databaseInfo)
            database.generate_mapping()
            print('mapping generated')
        except orm.core.BindingError:
            print('Binding error')
            pass

        #database.drop_all_tables(with_all_data=True)
        #database.create_tables()
        #database.generate_mapping(create_tables=True)
        orm.db_session.__enter__()
        


    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.disconnect()


    def test_Empty(self):
        with orm.db_session:
            self.assertEqual(databaseInfo['database'], 'orm_test')


    def test_OnePlantOneMeter(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', description='descripción de planta')
            meter = Meter(name='Mary', plant=alcolea)
        pass


    def test_OnePlantOneMeterOneRegistry(self):
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
        pass