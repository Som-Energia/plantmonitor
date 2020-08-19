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

from conf.config import env, env_active


def setupDatabase(create_tables=True):

    from conf import config

    databaseInfo = config.DB_CONF

    database.bind(**databaseInfo)

    #orm.set_sql_debug(True)

    # map the models to the database
    # and create the tables, if they don't exist
    database.generate_mapping(create_tables=create_tables)

    print(f"Database {databaseInfo['database']} generated")

    if env_active == env['plantmonitor_server']:
        tablesToTimescale = getTablesToTimescale()
        print("timescaling the tables {}".format(tablesToTimescale))
        timescaleTables()

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

    for t in tablesToTimescale:
        raw_sql("SELECT create_hypertable({}, 'time');".format(t))


def dailyInsert():
    # TODO implement daily insert from inverter
    pass
