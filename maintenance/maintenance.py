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

class NoSolarEventError(Exception): pass


def create_alarm_table(db_con):
    table_name = 'alarm'
    alarm_registry = '''
        CREATE TABLE IF NOT EXISTS
            {}
            (id serial primary key,
            name varchar,
            description varchar,
            severity varchar,
            createdate date,
            CONSTRAINT "unique_alarm__name" UNIQUE ("name")
        );

    '''.format(table_name)
    db_con.execute(alarm_registry)
    return table_name

def create_alarm_status_table(db_con):
    table_name = 'alarm_status'
    alarm_registry = f'''
        CREATE TABLE IF NOT EXISTS
            {table_name}
            (id serial primary key,
             device_table varchar,
             device_id integer,
             device_name varchar,
             alarm INTEGER NOT NULL,
             start_time timestamptz,
             update_time timestamptz,
             status boolean,
             CONSTRAINT "fk_alarm_status__alarm" FOREIGN KEY ("alarm") REFERENCES "alarm" ("id") ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_idx_device_table_device_id_alarm ON {table_name}(device_table, device_id, alarm);
    '''
    db_con.execute(alarm_registry)
    return table_name

def create_alarm_historic_table(db_con):
    table_name = 'alarm_historic'
    alarm_registry = f'''
        CREATE TABLE IF NOT EXISTS
            {table_name}
            (
                id serial primary key,
                device_table varchar,
                device_id integer,
                device_name varchar,
                alarm INTEGER NOT NULL,
                start_time timestamptz,
                end_time timestamptz,
                CONSTRAINT "fk_alarm_historic__alarm" FOREIGN KEY ("alarm") REFERENCES "alarm" ("id") ON DELETE CASCADE
            );
    '''
    db_con.execute(alarm_registry)
    return table_name


def get_latest_reading(db_con, target_table, source_table=None):
    table_exists = db_con.execute("SELECT to_regclass('{}');".format(target_table)).fetchone()
    if not table_exists:
        return None
    last_bucket = db_con.execute('select time from {} order by time desc limit 1;'.format(target_table)).fetchone()
    if not last_bucket:
        last_bucket = db_con.execute('select time from {} order by time limit 1;'.format(source_table)).fetchone()

    return last_bucket[0]

def get_alarm_status_nopower_alarmed(db_con, alarm, device_table, device_id):

    query = f'''
        SELECT status
        FROM alarm_status
        WHERE
            device_table = '{device_table}' AND
            device_id = {device_id} AND
            alarm = {alarm}
    '''
    return db_con.execute(query).fetchone()[0]

def get_alarm_current_nopower_inverter(db_con, check_time):
    check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")
    query = f'''
        SELECT reg.inverter AS inverter, inv.name as inverter_name, max(reg.power_w) = 0 as nopower
        FROM bucket_5min_inverterregistry as reg
        LEFT JOIN inverter AS inv ON inv.id = reg.inverter
        WHERE '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
        group by reg.inverter, inv.name
    '''
    return db_con.execute(query).fetchall()

def set_new_alarm(db_con, name, description, severity, createdate):
    createdate = createdate.strftime("%Y-%m-%d")

    # TODO this has a corner case race condition, see alternative:
    # https://stackoverflow.com/questions/40323799/return-rows-from-insert-with-on-conflict-without-needing-to-update
    query = f'''
        WITH ins AS (
            INSERT INTO
                alarm (
                    name,
                    description,
                    severity,
                    createdate
                )
                VALUES('{name}','{description}','{severity}','{createdate}')
                ON CONFLICT (name) DO NOTHING
                RETURNING
                    id
        )
        SELECT id FROM ins
        UNION  ALL
        SELECT id FROM alarm
        WHERE  name = '{name}'  -- only executed if no INSERT
        LIMIT  1;
    '''
    row = db_con.execute(query).fetchone()
    return row and row[0]

def set_alarm_status_update_time(db_con, device_table, device_id, alarm, check_time):

    check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")

    query = f'''
        UPDATE alarm_status 
        SET update_time = '{check_time}'
        WHERE 
            alarm = {alarm} AND 
            device_table = '{device_table}' AND 
            device_id = '{device_id}'
        RETURNING *
    '''
    return db_con.execute(query).fetchone()

def set_alarm_status(db_con, device_table, device_id, device_name, alarm, update_time, status):
    update_time = update_time.strftime("%Y-%m-%d %H:%M:%S%z")

    # start_time = 'NULL' if alarm == 'OK' else 'TARGET.start_time'
    start_time = None

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
        VALUES('{device_table}','{device_id}','{device_name}','{alarm}','{update_time}', '{update_time}', '{status}')
        ON CONFLICT (device_table, device_id, alarm)
        DO UPDATE
        SET
            device_name = EXCLUDED.device_name,
            start_time = CASE
                WHEN alarm_status.status != EXCLUDED.status THEN '{update_time}'
                ELSE alarm_status.start_time
                END,
            update_time = EXCLUDED.update_time,
            status = EXCLUDED.status
        RETURNING
            id, device_table, device_id, device_name, alarm, start_time, update_time, status,
            (SELECT old.status FROM alarm_status old WHERE old.id = alarm_status.id) AS old_status,
            (SELECT old.start_time FROM alarm_status old WHERE old.id = alarm_status.id) AS old_start_time;
    '''
    return db_con.execute(query).fetchone()

def set_alarm_historic(db_con, device_table, device_id, device_name, alarm, start_time, end_time):
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S%z")
    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S%z")

    query = f'''
        INSERT INTO
        alarm_historic (
            device_table,
            device_id,
            device_name,
            alarm,
            start_time,
            end_time
        )
        VALUES('{device_table}','{device_id}','{device_name}','{alarm}','{start_time}', '{end_time}')
        RETURNING
            id, device_table, device_id, device_name, alarm, start_time, end_time;
    '''
    return db_con.execute(query).fetchone()

def is_daylight(db_con, inverter, check_time):
    query = f'''
        select 
            case when 
                '{check_time}' between sunrise and sunset then TRUE
                else FALSE 
            END as is_daylight
            from solarevent
            left join inverter on inverter.id = {inverter}
            left join plant on plant.id = inverter.plant
        where solarevent.plant = plant.id
        and '{check_time}'::date = sunrise::date
    '''
    result = db_con.execute(query).fetchone()
    is_day = result and result[0]
    
    if is_day is None: 
        raise NoSolarEventError(f"Error: Current datetime of inverter {inverter} does not match any solarevent at {check_time}")

    return is_day


def update_alarm_nopower_inverter(db_con, check_time = None):
    check_time = check_time or datetime.datetime.now()

    device_table = 'inverter'

    nopower_alarm = {
        'name': 'nopower',
        'description': '',
        'severity': 'critical',
        'createdate': datetime.date.today()
    }

    alarm_id = set_new_alarm(db_con=db_con, **nopower_alarm)
    # TODO check alarma noreading que invalida l'alarma nopower

    alarm_current = get_alarm_current_nopower_inverter(db_con, check_time)

    for device_id, device_name, status in alarm_current:

        is_day = is_daylight(db_con, device_id, check_time)
        if not is_day:
            set_alarm_status_update_time(db_con, device_table, device_id, alarm_id, check_time)
            continue

        current_alarm = set_alarm_status(db_con, device_table, device_id, device_name, alarm_id, check_time, status)

        if current_alarm['old_status'] == True and status == False:
            set_alarm_historic(db_con, device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

def update_bucketed_inverter_registry(db_con, to_date=None):
    to_date = to_date or datetime.datetime.now(datetime.timezone.utc)
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
    return db_con.execute(insert_query).fetchall()

def alarm_maintenance(db_con):
    create_alarm_table(db_con)
    create_alarm_status_table(db_con)
    create_alarm_historic_table(db_con)
    update_alarm_nopower_inverter(db_con)

def bucketed_registry_maintenance(db_con):
    update_bucketed_inverter_registry(db_con)


# Pending:
# Dashboard: Alarma 3 Inversors temperatura anòmala històric temps real->
#   - https://redash.somenergia.coop/queries/137/source?p_interval=d_this_year#218
#   - https://redash.somenergia.coop/queries/163/source#266
#

# INVERSOR - ALARMA 2: INTENSITATS ENTRADA INVERSOR
#    Strings? stringregistry <- dependencia
#   - https://redash.somenergia.coop/queries/129/source#207


