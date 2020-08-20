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

    from conf import config

    databaseInfo = config.DB_CONF

    database.bind(**databaseInfo)

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
