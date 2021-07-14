#!/usr/bin/env python3

"""
You can call this script to regenerate ORM/schema.sql
in order to detect changes caused by pony ORM models
in the git changes and create the proper yoyo migration.
"""


import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables
from ORM.models import database

setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)


