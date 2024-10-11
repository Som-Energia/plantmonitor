#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from pathlib import Path
from unittest import TestCase
from plantmonitor.maintenance.db_test_factory import DbTestFactory

from psycopg2 import OperationalError

from .db_manager import DBManager

class TestDbManager(TestCase):

    def test__connection_base(self):
        from conf import envinfo

        database_info = envinfo.DB_CONF

        debug = False

        with DBManager(**database_info, echo=debug) as dbmanager:
            pass

    def test__connection_with_reserved_characters(self):
        from conf import envinfo

        database_info = envinfo.DB_CONF
        db_info = database_info.copy()

        db_info['password'] = 'aa@#\\/:aa'

        debug = False

        # The password is invented which might raise depending on the local configuration
        # but we are solving the OperationalError caused by reserved characters in the password
        try:
            with DBManager(**db_info, echo=debug) as dbmanager:
                pass
        except Exception as autherror:
            if str(autherror).find('password authentication failed') == -1:
                raise
