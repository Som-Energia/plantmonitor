#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from pathlib import Path
from unittest import TestCase
from maintenance.db_test_factory import DbTestFactory

from .db_manager import DBManager

class TestDbTestFactory(TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF
        db_info = database_info.copy()
        db_info['dbname'] = database_info['database']
        del db_info['provider']
        del db_info['database']

        debug = False

        cls.dbmanager = DBManager(**db_info, echo=debug)
    
    @classmethod
    def tearDownClass(cls) -> None:
        cls.dbmanager.close_db()

    def setUp(self):
        self.session = self.dbmanager.db_con.begin()

    def tearDown(self):
        self.session.rollback()
        self.session.close()

    def test_create(self):
        factory = DbTestFactory(self.dbmanager)
        factory.create('inverterregistry_factory.csv', 'inverterregistry')
        factory.delete('inverterregistry')

