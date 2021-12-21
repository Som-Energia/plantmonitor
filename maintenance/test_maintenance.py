#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from unittest import TestCase

from .maintenance import (
    round_dt_to_5minutes
)
from .db_manager import DBManager

import pandas as pd
from pandas.testing import assert_frame_equal

from datetime import datetime


class IrradiationDBConnectionTest(TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF

        cls.dbmanager = DBManager(**database_info)
        cls.dbmanager.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.dbmanager.__exit__()

    def setUp(self):
        self.session = self.dbmanager.create_session()

    def tearDown(self):
        self.session.rollback()
        self.session.close()

    def _test_readirradiance(self):

        # add values

        irradiation_maintenance = IrradiationMaintenance(self.db)

        irradiation_maintenance.irradiation()


class IrradiationMaintenanceTests(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__round_minutes(self):

        dtypes = [int, datetime, int, int]
        df = pd.read_csv('test_data/df_test_round_minutes.csv', sep = ';', dtype=dtypes)

        round_dt_to_5minutes(df,colname='time')

        result = df

        expected = pd.DataFrame()

        assert_frame_equal(result, expected)

    def test__duplicate_minutes(self):

        df = pd.read_csv('test_data/df_test_round_minutes_duplicateds.csv')

        round_dt_to_5minutes()
