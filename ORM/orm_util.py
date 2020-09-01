#!/usr/bin/env python
import os

from pony import orm

from .models import database

from .models import (
    Plant,
    Meter,
    MeterRegistry,
    Sensor,
    SensorIntegratedIrradiation,
    SensorIrradiation,
    SensorTemperature,
    SensorIrradiationRegistry,
    SensorTemperatureRegistry,
    IntegratedIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)

def setupDatabase(create_tables=True):

    from conf import dbinfo

    databaseInfo = dbinfo.DB_CONF

    try:
        # unbind necessary when mixing databases
        database.bind(**databaseInfo)
    except orm.core.BindingError as e:
        # TODO: capturing this exception is pontentially dangerous if databaseInfo changed
        # let's be sure
        with orm.db_session:
            dsn = database.get_connection().dsn
            dsnDict = {pair.split('=')[0]:pair.split('=')[1] for pair in dsn.split(' ')}
            dsnDict['provider'] = database.provider_name
            dsnDict['database'] = dsnDict.pop('dbname')
            if not databaseInfo == dsnDict:
                 print("Database was already bound to a different database.")
                 raise e
    else:
        # requires superuser privileges
        # with orm.db_session:
        #     database.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

        #orm.set_sql_debug(True)

        # map the models to the database
        # and create the tables, if they don't exist
        database.generate_mapping(create_tables=create_tables)

        print(f"Database {databaseInfo['database']} generated")

    # if env_active == env['plantmonitor_server']:
    #     tablesToTimescale = getTablesToTimescale()
    #     print("timescaling the tables {}".format(tablesToTimescale))
    #     timescaleTables()


def getTablesToTimescale():
    tablesToTimescale = [
        "MeterRegistry",
        "InverterRegistry",
        "SensorIrradiationRegistry",
        "SensorTemperatureRegistry",
        "IntegratedIrradiationRegistry",
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
