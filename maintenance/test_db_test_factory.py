#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from pathlib import Path
from unittest import TestCase
from maintenance.db_test_factory import DbTestFactory

class TestDbTestFactory(TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF
        db_info = database_info.copy()
        db_info['dbname'] = database_info['database']
        del db_info['provider']
        del db_info['database']

        cls.db_info = db_info

    def test_create(self):
        factory = DbTestFactory(**self.db_info)
        factory.create('inverterregistry_factory.csv', 'inverterregistry')
        factory.delete('inverterregistry')

