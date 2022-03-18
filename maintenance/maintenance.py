#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import numpy as np

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")


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

def get_from_date_alarm(db_con, alarma, registry_table):
    last_time_alarm = db_con.execute("SELECT updated FROM alarm_normalized_historic WHERE alarm == {} ORDER BY updated DESC LIMIT 1".format(alarm)).fetchone()
    if not last_time_alarm:
        last_time_alarm = db_con.execute('select time from {} order by time limit 1;'.format(registry_table)).fetchone()

    return last_time_alarm

# # idea use a simple cron sql query that adds rows to the derivate table
# def update_alarm_inverter_maintenance_via_sql(db_con):
#     target_name = 'alarm_normalized_historic'
#     source_table = 'inverterregistry_5min_avg'
#     latest_reading = get_latest_reading(db_con, target_name, source_table)
#     query = Path('queries/zero_inverter_power_at_daylight.sql').read_text(encoding='utf8')
#     query = query.format(latest_reading)

#     new_records = db_con.execute(query).fetchall()
#     new_records_strs = ','.join(["('{}', {}, {})".format(r[0].strftime('%Y-%m-%d %H:%M:%S%z'),r[2],r[3]) for r in new_records])
#     insert_query = 'insert into {}(time, inverter, power_w) VALUES {}'.format(table_name, new_records_strs)

#     db_con.execute(insert_query)

#     return new_records


def get_alarm_status_nopower_alarmed(db_con, alarm, device_table, device_id):

    query = f'''
        SELECT status
        FROM alarm_status
        WHERE
            device_table = '{device_table}' AND
            device_id = '{device_id}' AND
            alarm = '{alarm}'
    '''
    return db_con.execute(query).fetchone()[0]

def get_alarm_current_nopower_inverter(db_con, check_time):
    query = f'''
        SELECT reg.inverter AS inverter, inv.name as inverter_name, max(reg.power_w) = 0 as nopower
        FROM bucket_5min_inverterregistry as reg
        LEFT JOIN inverter AS inv ON inv.id = reg.inverter
        WHERE '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
        group by reg.inverter, inv.name
    '''
    return db_con.execute(query).fetchall()

def set_alarm_status(db_con, device_table, device_id, device_name, alarm, check_time, status):

    start_time = 'NULL' if alarm == 'OK' else 'TARGET.start_time'

    query = f'''
        INSERT INTO
        alarm_status (
            device_table,
            device_id,
            device_name,
            alarm,
            start_time,
            update_time,
            status
        )
        VALUES('{device_table}','{device_id}','{device_name}','{alarm}','{check_time}', '{check_time}', '{status}')
        ON CONFLICT (device_table, device_id, alarm)
        DO UPDATE
        SET
            device_name =  EXCLUDED.device_name,
            --TODO check if this works cuz then we can avoid doing it in python
            --start_time = CASE WHEN status = OK THEN NULL ELSE TARGET.start_time END
            start_time = {start_time}
            update_time = EXCLUDED.update_time,
            status = EXCLUDED.status
        RETURNING
            id, device_table, device_id, device_name, alarm, updated, status;
    '''
    return db_con.execute(query).fetchone()

def set_new_alarm(db_con, name, description, severity, createdate):
    query = f'''
       INSERT INTO
        alarm (
            name,
            description,
            severity,
            createdate,
        )
        VALUES('{name}','{description}','{severity}','{createdate}')
        ON CONFLICT (name) DO IGNORE
        RETURNING
            id, name, description, severity, createdate
        '''
    row = db_con.execute(query).fetchone()
    return row and row[0]

def update_alarm_nopower_inverter(db_con, check_time = None):
    check_time = check_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    device_table = 'inverter'

    nopower_alarm = {
        'name': 'nopower',
        'description': '',
        'severity': 'critical',
        'createdate': datetime.date.today().isoformat()
    }

    alarm_id, *_ = set_new_alarm(db_con=db_con, **nopower_alarm)
    # TODO check alarma noreading que invalida l'alarma nopower

    alarm_current = get_alarm_current_nopower_inverter(db_con, check_time)

    for device_id, device_name, status in alarm_current:

        # if night:
        #     continue

        # alarm_status = select status from alarm_status where device_table = 'inverter' and device_id = inverter and alarm = 'nopower' limit 1
        alarm_previous = get_alarm_status_nopower_alarmed(db_con, alarm_id, device_table, device_id)

        if not status and alarm_previous:
            #insert la row a la taula historica
            set_alarm_historic(db_con, device_table, device_id, device_name, alarm_id, check_time)

        if not alarm_previous:
            set_alarm_status(db_con, device_table, device_id, device_name, alarm_id, check_time, status)
            # insert new row amb el alarm_current
            return


device_table varchar,
device_id integer,
device_name varchar,
alarm INTEGER NOT NULL,
description varchar,
severity integer,
started timestamptz,
ended timestamptz,
updated timestamptz)


        # UPDATE PSQL ROW with alarm_current
        set_alarm_status(db_con, device_table, device_id, device_name, alarm_id, check_time, status)

# device_table,device_id,device_name,alarm,description,severity,started,ended,updated

    # per inversor: max(power_w) == 0: alarma


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

def create_alarm_table(db_con):
    table_name = 'alarm'
    alarm_registry = '''
        CREATE TABLE IF NOT EXISTS
            {}
            (id serial primary key,
            name varchar,
            description varchar,
            severity integer,
            createdate date
        );
    '''.format(table_name)
    db_con.execute(alarm_registry)
    return table_name

def create_alarm_status_table(db_con):
    table_name = 'alarm_status'
    alarm_registry = '''
        CREATE TABLE IF NOT EXISTS
            {}
            (device_table varchar,
             device_id integer,
             device_name varchar,
             alarm INTEGER NOT NULL,
             start_time timestamptz,
             update_time timestamptz,
             status boolean);

        ALTER TABLE "alarm_status" ADD CONSTRAINT "fk_alarm_status__alarm" FOREIGN KEY ("alarm") REFERENCES "alarm" ("id") ON DELETE CASCADE;
    '''.format(table_name)
    db_con.execute(alarm_registry)
    return table_name

def create_alarm_historic_table(db_con):
    table_name = 'alarm_historic'
    alarm_registry = '''
        CREATE TABLE IF NOT EXISTS
            {}
            (device_table varchar,
             device_id integer,
             device_name varchar,
             alarm INTEGER NOT NULL,
             description varchar,
             severity integer,
             started timestamptz,
             ended timestamptz,
             updated timestamptz);

        ALTER TABLE "alarm_historic" ADD CONSTRAINT "fk_alarm_historic__alarm" FOREIGN KEY ("alarm") REFERENCES "alarm" ("id") ON DELETE CASCADE;
    '''.format(table_name)
    db_con.execute(alarm_registry)
    return table_name




def update_alarm_nopower(db_con):
    target_table = create_alarm_normalized_historic_table(db_con)

    device_raw_table = 'inverterregistry'
    clean_source_table = 'bucket_5min_{}'.format(device_raw_table)

    latest_reading = get_latest_reading(db_con, target_table, clean_source_table)
    query = Path('queries/maintenance/alarms/alarm1_nopower_inverter.sql').read_text(encoding='utf8')
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
    create_alarm_table(db_con)
    create_alarm_status_table(db_con)
    create_alarm_historic_table(db_con)

def bucketed_registry_maintenance(db_con):
    update_bucketed_inverter_registry(db_con)


# Alarms:
# Taula comuna actual:
# device_name,alarm,last_reading
# inversor1,OK,23/02/22 10:05


# Nova taula comuna
# device_table,device_id,device_name,alarm,description,severity,started,ended,updated
# Devices: Qualsevol cosa, sonda, inversor, planta, temps meteorlògic
# Si ended is null  es current alarm

# Alarmes inversors
# Dashboard: Alarma en temps real potència nula amb radiació tots els inversors
#   - https://redash.somenergia.coop/queries/128/source#206
#   - irradiationsensor <- dependencia
#   - Això no hauria de mirar orto i ocaso?

# Dashboard: Alarma 3 Inversors temperatura anòmala històric temps real->
#   - https://redash.somenergia.coop/queries/137/source?p_interval=d_this_year#218
#   - https://redash.somenergia.coop/queries/163/source#266
#


# INVERSOR - ALARMA 2: INTENSITATS ENTRADA INVERSOR
#    Strings? stringregistry <- dependencia
#   - https://redash.somenergia.coop/queries/129/source#207


