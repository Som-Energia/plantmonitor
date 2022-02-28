#!/usr/bin/env python
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')

import datetime
import click
from yamlns import namespace as ns

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from ORM.pony_manager import PonyManager

from ORM.db_utils import setupDatabase, dropTables

from ORM.migrations import migrateLegacyToPony
from pony import orm

from meteologica.plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBError,
)

# ```
# psql -U postgres -h localhost
# CREATE database plants;
# \c plants
# CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
# ```


def legacyMigrate(skipList=[]):
    configdbns = ns.load('conf/configlegacydb.yaml')
    migrateLegacyToPony(configdbns, skipList=skipList)


@click.command()
@click.option('--generate/--nogenerate', default=True, help='Regenerate mapping')
@click.option('--create/--nocreate', default=True, help='Regenerate tables')
@click.option('--destroy/--nodestroy', default=False, help='Drop tables')
@click.option('--timescale/--notimescale', default=True, help='Timescale the Registry tables.')
@click.option('--migrate/--nomigrate', default=False, help='Run the migration scripts using conf/configdb.yaml')
@click.option('--sqldebug/--nosqldebug', default=False, help='print all sql issued by the ORM')
@click.option('--noplants/--plants', default=False, help='create plants')
@click.option('--noinverters/--inverters', default=False, help='migrate inverters')
@click.option('--nometers/--meters', default=False, help='migrate meters')
@click.option('--nosensors/--sensors', default=False, help='migrate sensors')
def deployDatabase(generate, create, destroy, timescale, migrate, sqldebug, noplants, noinverters, nometers, nosensors):
    if sqldebug:
        orm.set_sql_debug(True)

    if destroy:
        dropTables()

    skipList = []
    if noplants:
        skipList += ['plants']
    if noinverters:
        skipList += ['inverters']
    if nometers:
        skipList += ['meters']
    if nosensors:
        skipList += ['sensors']

    if generate:
        print('database setup')

        setupDatabase(create_tables=create, timescale_tables=timescale, drop_tables=destroy)

        with orm.db_session:
            dsn = database.get_connection().dsn
            print(dsn)

        print('database created')

    if migrate:
        legacyMigrate(skipList)


if __name__ == "__main__":
    deployDatabase()
