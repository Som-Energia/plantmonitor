#!/usr/bin/env python
from pony import orm

from .models import database

def setupDatabase(create_tables=True):

    from ..conf import config

    databaseInfo = config.DB_CONF

    database.bind(**databaseInfo)

    # map the models to the database
    # and create the tables, if they don't exist
    database.generate_mapping(create_tables=create_tables)


setupDatabase()

def dailyInsert():
    # TODO implement daily insert from inverter
    pass
