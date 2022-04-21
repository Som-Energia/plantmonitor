#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import numpy as np

import yaml

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

import datetime

from .alarm_manager import AlarmManager

def get_latest_reading(db_con, target_table, source_table=None):
    table_exists = db_con.execute("SELECT to_regclass('{}');".format(target_table)).fetchone()
    if not table_exists:
        return None
    last_bucket = db_con.execute('select time from {} order by time desc limit 1;'.format(target_table)).fetchone()
    if not last_bucket:
        last_bucket = db_con.execute('select time from {} order by time limit 1;'.format(source_table)).fetchone()

    return last_bucket[0]


# TODO Refactor the bucketing to make it agnostic of the registry. Requires:
# - pass registry table name as a parameter
# - specifying which columns are metrics (for the update)
# - specifying which agregation method to use on the buckets (avg, max, ...)

def update_bucketed_inverter_registry(db_con, to_date=None):
    to_date = to_date or datetime.datetime.now(datetime.timezone.utc)
    setup_5min_table = '''
        CREATE TABLE IF NOT EXISTS
            bucket_5min_inverterregistry
            (time timestamptz, inverter integer, temperature_dc bigint, power_w bigint, energy_wh bigint);

        CREATE UNIQUE INDEX IF NOT EXISTS time_inverter
            ON bucket_5min_inverterregistry (time, inverter);

        SELECT create_hypertable('bucket_5min_inverterregistry', 'time', if_not_exists => TRUE)
    '''
    db_con.execute(setup_5min_table)
    source_table = 'inverterregistry'
    target_table = 'bucket_5min_{}'.format(source_table)
    latest_reading = get_latest_reading(db_con, target_table, source_table)
    logger.debug(f"Latest reading of inverter {latest_reading}")
    query = Path('queries/maintenance/bucket_5min_{}.sql'.format(source_table)).read_text(encoding='utf8')
    query = query.format(latest_reading, to_date.strftime('%Y-%m-%d %H:%M:%S%z'))
    insert_query = f'''
        INSERT INTO {target_table}
         {query}
         ON CONFLICT (time, inverter) DO
            UPDATE
	            SET temperature_dc = excluded.temperature_dc,
	            power_w = excluded.power_w,
	            energy_wh = excluded.energy_wh
        RETURNING time, inverter, temperature_dc, power_w, energy_wh
    '''
    logger.debug("Insert query")
    return db_con.execute(insert_query).fetchall()

#TODO abstract the time_bucketing
def update_bucketed_string_registry(db_con, to_date=None):
    to_date = to_date or datetime.datetime.now(datetime.timezone.utc)
    setup_5min_table = '''
        CREATE TABLE IF NOT EXISTS
            bucket_5min_stringregistry
            (time TIMESTAMP WITH TIME ZONE NOT NULL, string integer not null, intensity_ma bigint);

        CREATE UNIQUE INDEX IF NOT EXISTS time_string
            ON bucket_5min_stringregistry (time, string);

        SELECT create_hypertable('bucket_5min_stringregistry', 'time', if_not_exists => TRUE)
    '''
    db_con.execute(setup_5min_table)
    source_table = 'stringregistry'
    target_table = 'bucket_5min_{}'.format(source_table)
    latest_reading = get_latest_reading(db_con, target_table, source_table)
    logger.debug(f"Latest reading of string {latest_reading}")
    query = Path('queries/maintenance/bucket_5min_{}.sql'.format(source_table)).read_text(encoding='utf8')
    query = query.format(latest_reading, to_date.strftime('%Y-%m-%d %H:%M:%S%z'))
    insert_query = f'''
        INSERT INTO {target_table}
         {query}
         ON CONFLICT (time, string) DO
            UPDATE
	            SET intensity_ma = excluded.intensity_ma
        RETURNING time, string, intensity_ma
    '''
    logger.debug("Insert query")
    return db_con.execute(insert_query).fetchall()

def alarm_maintenance(db_con):
    alarm_manager = AlarmManager(db_con)
    alarm_manager.create()
    alarm_manager.check_alarms()


def bucketed_registry_maintenance(db_con):
    logger.debug("Updating bucketed registries table")
    update_bucketed_inverter_registry(db_con)
    logger.info('Updated bucketed inverter registry table')
    update_bucketed_string_registry(db_con)
    logger.info('Update bucketed inverterstring registry table')

# Pending:
# Dashboard: Alarma 3 Inversors temperatura anòmala històric temps real->
#   - https://redash.somenergia.coop/queries/137/source?p_interval=d_this_year#218
#   - https://redash.somenergia.coop/queries/163/source#266
#

# INVERSOR - ALARMA 2: INTENSITATS ENTRADA INVERSOR
#    Strings? stringregistry <- dependencia
#   - https://redash.somenergia.coop/queries/129/source#207


