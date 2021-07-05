#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables
from ORM.models import database

setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)


