#!/usr/bin/env python
import os
from pathlib import Path
from pony import orm

from conf.config import env, env_active

from .models import database

from .models import (
    Plant,
    Meter,
    MeterRegistry,
    Inverter,
    InverterRegistry,
    Sensor,
    SensorIntegratedIrradiation,
    SensorIrradiation,
    SensorTemperatureModule,
    SensorTemperatureAmbient,
    SensorIrradiationRegistry,
    SensorTemperatureAmbientRegistry,
    SensorTemperatureModuleRegistry,
    IntegratedIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")


def dropTables():

    from conf import envinfo

    databaseInfo = envinfo.DB_CONF

    print("dropping tables in {}".format(databaseInfo))

    database.bind(**databaseInfo)
    database.generate_mapping(check_tables=False, create_tables=False)
    database.drop_all_tables(with_all_data=True)
    database.disconnect()

def connectDatabase():
    from conf import envinfo

    databaseInfo = envinfo.DB_CONF

    # TODO find a better way to avoid BindingError if database was bound from another test file
    try:
        # unbind necessary when mixing databases
        database.bind(**databaseInfo)
    except orm.core.BindingError as e:
        # TODO: capturing this exception is pontentially dangerous if databaseInfo changed
        # let's be sure
        with orm.db_session:
            dsn = database.get_connection().dsn
            dsnDict = {
                key: value if value!="''" else ""
                for key, value in (
                    pair.split('=')
                    for pair in dsn.split()
                )
            }
            dsnDict['provider'] = database.provider_name
            dsnDict['database'] = dsnDict.pop('dbname')
            if 'password' in databaseInfo:
                dsnDict['password'] = databaseInfo['password']
            if not databaseInfo == dsnDict:
                logger.debug("Database was already bound to a different database.")
                raise
    else:
        database.generate_mapping(create_tables=False, check_tables=False)

def setupDatabase(create_tables=True, timescale_tables=True, drop_tables=False):

    from conf import envinfo

    databaseInfo = envinfo.DB_CONF

    # print(databaseInfo)

    try:
        # unbind necessary when mixing databases
        database.bind(**databaseInfo)
    except orm.core.BindingError as e:
        # TODO: capturing this exception is pontentially dangerous if databaseInfo changed
        # let's be sure
        with orm.db_session:
            dsn = database.get_connection().dsn
            dsnDict = {
                key: value if value!="''" else ""
                for key, value in (
                    pair.split('=')
                    for pair in dsn.split()
                )
            }
            dsnDict['provider'] = database.provider_name
            dsnDict['database'] = dsnDict.pop('dbname')
            if 'password' in databaseInfo:
                dsnDict['password'] = databaseInfo['password']
            if not databaseInfo == dsnDict:
                logger.debug("Database was already bound to a different database.")
                raise
    else:
        #this `else` will not run if the database was already connected
        # (necessary for test multiple SetUps until we fix this)

        # requires superuser privileges
        # with orm.db_session:
        #     database.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

        #orm.set_sql_debug(True)
        database.generate_mapping(create_tables=False, check_tables=False)
        currentschema = (
            "-- Before commit changes in this file, create the corresponding migration\n\n{}"
            .format(database.schema.generate_create_script()))
        schemafile = Path(__file__).parent/"schema.sql"
        if not schemafile.exists() or schemafile.read_text(encoding='utf8'):
            logger.info("Database models modified, updated schema at {}".format(schemafile))
            schemafile.write_text(currentschema, encoding='utf8')

        if drop_tables:
            print("Dropping all tables")
            database.drop_all_tables(with_all_data=True)
        # database.disconnect()

        # map the models to the database
        # and create the tables, if they don't exist
        if create_tables:
            database.create_tables()
            logger.info("Database {} generated".format(databaseInfo['database']))

        if env_active == env['plantmonitor_server'] or env_active == env['test']:
            if timescale_tables:
                tablesToTimescale = getTablesToTimescale()
                logger.info("Timescaling the tables {}".format(tablesToTimescale))
                with orm.db_session:
                    timescaleTables(tablesToTimescale)


def getTablesToTimescale():
    tablesToTimescale = [
        "MeterRegistry",
        "InverterRegistry",
        "SensorIrradiationRegistry",
        "SensorTemperatureAmbientRegistry",
        "SensorTemperatureModuleRegistry",
        "IntegratedIrradiationRegistry",
        "InclinometerRegistry",
        "AnemometerRegistry",
        "OmieRegistry",
        "MarketRepresentativeRegistry",
        "SimelRegistry",
        "NagiosRegistry",
    ]
    return tablesToTimescale


def timescaleTables(tablesToTimescale):

    #foo = database.execute("CREATE INDEX ON meterregistry (meter, id, time DESC);")
    #boo = database.execute("SELECT create_hypertable('meterregistry', 'time', 'meter', 10);")

    for t in tablesToTimescale:
          database.execute("SELECT create_hypertable('{}', 'time');".format(t.lower()))


def dailyInsert():
    # TODO implement daily insert from inverter
    pass
