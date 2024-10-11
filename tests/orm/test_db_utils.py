# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

from pony import orm

from pathlib import Path

from plantmonitor.ORM.db_utils import setupDatabase, getTablesToTimescale, timescaleTables
from plantmonitor.ORM.pony_manager import PonyManager

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("test")

class DBUtils_Test(unittest.TestCase):

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        os.environ['PGTZ'] = 'UTC'

        database_info = envinfo.DB_CONF

        self.pony = PonyManager(database_info)
        self.pony.define_all_models()
        self.pony.binddb(create_tables=False, check_tables=False)

    def tearDown(self):

        '''
        binddb calls gneerate_mapping which creates the tables outside the transaction
        drop them
        '''
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()

    def test__check_schema_change(self):

        # doesn't really test, but let's git tell you the schema has changed

        currentschema = (
            "-- Before commit changes in this file, create the corresponding migration\n\n{}"
            .format(self.pony.db.schema.generate_create_script()))
        schemafile = Path(__file__).parent/"schema.sql"
        if not schemafile.exists() or schemafile.read_text(encoding='utf8'):
            logger.info("Database models modified, updated schema at {}".format(schemafile))
            schemafile.write_text(currentschema, encoding='utf8')



