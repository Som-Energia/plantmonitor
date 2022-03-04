#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import numpy as np

import datetime

# class IrradiationMaintenance():

#     def __init__(self, db):
#         self.db = db
#         self.df = None

#     # TODO read table (consider putting it on init)
#     def readIrradiation(self):

#         self.df = pd.read_table()

def irradiation_maintenance(df):
    return df\
        .pipe(round_dt_to_5minutes)\
        .pipe(fill_holes)

def round_dt_to_5minutes(df):

    return df\
        .assign(time = lambda d: d['time'].dt.round('5min'))\
        .groupby(['sensor', 'time'], as_index=False)\
        .mean(numeric_only=True)\
        .round({
            'irradiation_w_m2':0,
            'temperature_dc':0
        })\
        .astype({'irradiation_w_m2':int, 'temperature_dc':int})

def fill_holes(df):

    # df_complete = pd.date_range(min(df['time']), max(df['time']), freq="5min", tz='UTC').to_frame(name='time')
    # df = pd.merge(df, df_complete, how='right', on = {'time':0})

    df = df\
        .set_index(['time'], drop=False)\
        .groupby(['sensor'])\
        .resample('5min')\
        .mean()\
        .interpolate()

    return df

def resample(df):
    df = df\
        .set_index(['time'], drop=False)\
        .groupby(['sensor'])\
        .resample('5min')\
        .mean()\
        .interpolate()

    return df

def get_latest_reading(db_con, target_table, source_table=None):
    table_exists = db_con.execute("SELECT to_regclass('{}');".format(target_table)).fetchone()
    if not table_exists:
        return None
    last_bucket = db_con.execute('select time from {} order by time desc limit 1;'.format(target_table)).fetchone()
    if not last_bucket:
        last_bucket = db_con.execute('select time from {} order by time limit 1;'.format(source_table)).fetchone()

    return last_bucket[0]


# idea use a simple cron sql query that adds rows to the derivate table
def update_alarm_inverter_maintenance_via_sql(db_con):
    table_name = 'zero_inverter_power_at_daylight'
    latest_reading = get_latest_reading(db_con, table_name)
    query = Path('queries/zero_inverter_power_at_daylight.sql').read_text(encoding='utf8')
    query = query.format(latest_reading)

    new_records = db_con.execute(query).fetchall()
    new_records_strs = ','.join(["('{}', {}, {})".format(r[0].strftime('%Y-%m-%d %H:%M:%S%z'),r[2],r[3]) for r in new_records])
    insert_query = 'insert into {}(time, inverter, power_w) VALUES {}'.format(table_name, new_records_strs)

    db_con.execute(insert_query)

    return new_records

def update_alarm_meteorologic_station_maintenance_via_sql(db_con):
    table_name = 'zero_sonda_irradiation_at_daylight'
    latest_reading = get_latest_reading(db_con, table_name)
    query = Path('queries/zero_sonda_irradiation_at_daylight.sql').read_text(encoding='utf8')
    query = query.format(latest_reading)
    new_records = db_con.execute(query).fetchall()
    new_records_strs = ','.join(["({}, '{}', {}, {})".format(r[0],r[1].strftime('%Y-%m-%d %H:%M:%S%z'),r[3],r[4]) for r in new_records])
    insert_query = 'insert into {}(sensor, time, irradiation_w_m2, temperature_dc) VALUES {}'.format(table_name, new_records_strs)
    db_con.execute(insert_query)
    return new_records

def update_bucketed_inverter_registry(db_con):
    setup_5min_table = '''
        CREATE TABLE IF NOT EXISTS 
            bucket_5min_inverterregistry 
            (time timestamptz, inverter integer, temperature_dc bigint, power_w bigint, energy_wh bigint);
        
        CREATE UNIQUE INDEX IF NOT EXISTS time_inverter
            ON bucket_5min_inverterregistry (time, inverter);
    '''
    db_con.execute(setup_5min_table)
    source_table = 'inverterregistry'
    target_table = 'bucket_5min_{}'.format(source_table)
    latest_reading = get_latest_reading(db_con, target_table, source_table)
    query = Path('queries/maintenance/bucket_5min_{}.sql'.format(source_table)).read_text(encoding='utf8')
    query = query.format(latest_reading, datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S%z'))
    insert_query = '''
        INSERT INTO {}
         {} 
         ON CONFLICT (time, inverter) DO 
            UPDATE
	            SET temperature_dc = excluded.temperature_dc,
	            power_w = excluded.power_w,
	            energy_wh = excluded.energy_wh
        RETURNING time, inverter, temperature_dc, power_w, energy_wh'''.format(target_table, query)
    return db_con.execute(insert_query).fetchall()

def alarm_maintenance(db_con):
    update_alarm_inverter_maintenance_via_sql(db_con)
    update_alarm_meteorologic_station_maintenance_via_sql(db_con)


# Alarms:
# Taula comuna actual:
# device_name,alarm,last_reading
# inversor1,OK,23/02/22 10:05


# Nova taula comuna
# device_table,device_id,device_name,alarm,description,severity,started,ended,updated
# Devices: Qualsevol cosa, sonda, inversor, planta, temps meteorlògic
# Si ended is null  es current alarm
