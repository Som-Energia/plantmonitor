# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
from yamlns import namespace as ns
import datetime as dt

from click.testing import CliRunner

from meteologica.plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBMock,
    PlantmonitorDBError,
)

from meteologica.utils import todt

from pathlib import Path

from pony import orm

from ORM.pony_manager import PonyManager

from check_db_connection import check_db_connection, check_db_connection_CLI

class CheckDBConnection_Test(unittest.TestCase):

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()

        self.pony = PonyManager(envinfo.DB_CONF)

        self.pony.define_all_models()
        self.pony.binddb(create_tables=True)

        self.pony.db.drop_all_tables(with_all_data=True)

        #orm.set_sql_debug(True)
        self.pony.db.create_tables()

        self.pony.db.disconnect()

        # database.generate_mapping(create_tables=True)
        # orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        # orm.db_session.__exit__()
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()


    def test__check_db_connection(self):
        check_db_connection()

    def test__check_db_connectionCLI(self):
        runner = CliRunner()
        result = runner.invoke(check_db_connection_CLI, [])
        self.assertEqual(0, result.exit_code)
