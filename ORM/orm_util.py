#!/usr/bin/env python
from pony import orm

from .models import database

from conf import config_test


def setup(create_tables=True):

    database.bind(**databaseInfo)

    # map the models to the database
    # and create the tables, if they don't exist
    database.generate_mapping(create_tables=create_tables)

setup()


def dailyInsert():
    # TODO implement daily insert from inverter
    pass   