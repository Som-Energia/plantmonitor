
from yamlns import namespace as ns

import datetime
import sys
from pathlib import Path

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from meteologica.plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBError,
)

from pony import orm

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
    SensorTemperature,
    SensorIrradiationRegistry,
    SensorTemperatureRegistry,
    IntegratedIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)

def psql_dt():
    return """SELECT c.relname AS Tables_in FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE pg_catalog.pg_table_is_visible(c.oid)
AND c.relkind = 'r'
AND relname NOT LIKE 'pg_%'
ORDER BY 1"""

def psql_d(tablename):
    return """select column_name, data_type
from INFORMATION_SCHEMA.COLUMNS where table_name ='{}';""".format(tablename)

def loadDump(db, dumpdir, excluded):
    cur = db._client.cursor()
    # cur.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'public'")
    # tables = cur.fetchall()
    cur.execute(psql_dt())
    dt = cur.fetchall()
    for t in dt:
        cur.execute(psql_d(t[0]))
        t_info = cur.fetchall()
        # print("\n{} defined by \n".format(t[0]))
        # [print("\t{} {}".format(n,v)) for n,v in t_info]

    # tableNames = [p.stem for p in Path(dumpdir).iterdir()]
    # hypertablesNames = [n for n in tableNames if n not in excluded]

    # [cur.execute("SELECT create_hypertable('{}', 'time');".format(t)) for t in hypertablesNames]

    # import data
    for tableDumpPath in Path(dumpdir).iterdir():
        with open(tableDumpPath, 'r') as f:
            cur.copy_from(f, tableDumpPath.stem, sep=",", null='')


def migrateLegacyToPony(configdb):
    with PlantmonitorDB(configdb) as db:
        pass


def main():

    from conf import config

    migrateLegacyToPony(config)
