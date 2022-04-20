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

class NoSolarEventError(Exception): pass

def read_alarms_config(yamlfile):
    with open(yamlfile, 'r') as stream:
        alarms = yaml.safe_load(stream)
        return alarms

def create_alarms(db_con, alarms_yaml_content):
    # TODO use this on the final caller
    # from conf import envinfo
    # alarms_yaml_file = getattr(envinfo,'ALARMS_YAML','conf/alarms.yaml')
    # alarms = read_alarms_config(alarms_yaml)

    #TODO should we try to create, we usually will have created alarm,status and historic via create_alarm_tables
    create_alarm_table(db_con)

    #TODO silently continue if alarms key does not exist?
    for alarm in alarms_yaml_content.get('alarms', []):
        alarm['createdate'] = alarm.get('createdate', datetime.datetime.today())
        set_new_alarm(db_con=db_con, **alarm)


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
                id serial,
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

    check_timescale = '''
        SELECT extversion
        FROM pg_extension
        where extname = 'timescaledb';
    '''
    has_timescale = db_con.execute(check_timescale).fetchone()

    if has_timescale:
        do_hypertable = '''
        SELECT create_hypertable('alarm_historic','start_time', if_not_exists => TRUE);
        '''
        db_con.execute(do_hypertable).fetchone()
    else:
        logger.warning(f"Database {db_con.info} does not have timescale")

    return table_name

def create_alarm_tables(db_con):
    create_alarm_table(db_con)
    create_alarm_status_table(db_con)
    create_alarm_historic_table(db_con)

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

def get_alarm_current_nointensity_string(db_con, check_time):
    check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")

    query = f'''
        SELECT reg.string as string, s.name as string_name, max(reg.intensity_ma) = 0 as nointensity
        FROM bucket_5min_stringregistry as reg
        LEFT JOIN string AS s ON s.id = reg.string
        LEFT JOIN inverter AS inv ON inv.id = s.inverter
        WHERE '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
        group by inverter, string, inv.name, s.name
    '''
    return db_con.execute(query).fetchall()

# TODO store the sql in db?
def set_new_alarm(db_con, name, description, severity, createdate, sql=None):
    createdate = createdate.strftime("%Y-%m-%d")

    # TODO this has a corner case race condition, see alternative:
    # https://stackoverflow.com/questions/40323799/return-rows-from-insert-with-on-conflict-without-needing-to-update
    # TODO: How to prevent autoincrement of id serial on conflict
    query = f'''
        WITH ins AS (
            INSERT INTO
                alarm (
                    name,
                    description,
                    severity,
                    createdate
                )
                VALUES(%s, %s, %s, %s)
                ON CONFLICT (name) DO NOTHING
                RETURNING
                    id
        )
        SELECT id FROM ins
        UNION  ALL
        SELECT id FROM alarm
        WHERE  name = %s  -- only executed if no INSERT
        LIMIT  1;
    '''
    row = db_con.execute(query, (name, description, severity, createdate, name)).fetchone()

    db_con.execute("SELECT setval('alarm_id_seq', MAX(id), true) FROM alarm;")

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
    status = 'NULL' if status is None else status

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
        VALUES('{device_table}','{device_id}','{device_name}','{alarm}','{update_time}', '{update_time}', {status})
        ON CONFLICT (device_table, device_id, alarm)
        DO UPDATE
        SET
            device_name = EXCLUDED.device_name,
            start_time = CASE
                WHEN EXCLUDED.status is distinct from alarm_status.status THEN '{update_time}'
                ELSE alarm_status.start_time
                END,
            update_time = EXCLUDED.update_time,
            status = EXCLUDED.status
        RETURNING
            id, device_table, device_id, device_name, alarm, start_time, update_time, status,
            (SELECT old.status FROM alarm_status old WHERE old.id = alarm_status.id) AS old_status,
            (SELECT old.start_time FROM alarm_status old WHERE old.id = alarm_status.id) AS old_start_time;
    '''
    result = db_con.execute(query).fetchone()
    db_con.execute("SELECT setval('alarm_status_id_seq', MAX(id), true) FROM alarm_status;")
    return result

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

def set_devices_alarms(db_con, alarm_id, device_table, alarms_current, check_time):
    raise NotImplementedError
    for device_id, device_name, status in alarms_current:
        if status is not None:
            set_alarm_status_update_time(db_con, device_table, device_id, alarm_id, check_time)

        current_alarm = set_alarm_status(db_con, device_table, device_id, device_name, alarm_id, check_time, status)

        if current_alarm['old_status'] == True and status != True:
            set_alarm_historic(db_con, device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

# TODO the is_day condition could be abstracted and passed as a parameter if other alarms have different conditions
# e.g. skip_condition(db_con, inverter, check_time)
def set_devices_alarms_if_daylight(db_con, alarm_id, device_table, alarms_current, check_time):
    for device_id, device_name, status in alarms_current:
        if status is not None:
            is_day = is_daylight(db_con, device_id, check_time)
            if not is_day:
                set_alarm_status_update_time(db_con, device_table, device_id, alarm_id, check_time)
                continue

        current_alarm = set_alarm_status(db_con, device_table, device_id, device_name, alarm_id, check_time, status)

        if current_alarm['old_status'] == True and status != True:
            set_alarm_historic(db_con, device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)


def update_alarm_nopower_inverter(db_con, check_time = None):
    check_time = check_time or datetime.datetime.now()

    device_table = 'inverter'

    nopower_alarm = {
        'name': 'nopowerinverter',
        'description': 'Inversor sense potència entre alba i posta',
        'severity': 'critical',
        'createdate': datetime.date.today()
    }

    alarm_id = set_new_alarm(db_con=db_con, **nopower_alarm)
    # TODO check alarma noreading que invalida l'alarma nopower

    alarm_current = get_alarm_current_nopower_inverter(db_con, check_time)
    set_devices_alarms_if_daylight(db_con, alarm_id, device_table, alarm_current, check_time)

def update_alarm_nointensity_string(db_con, check_time = None):
    check_time = check_time or datetime.datetime.now()

    device_table = 'string'

    nointensity_alarm = {
        'name': 'nointensitystring',
        'description': 'String sense intensitat entre alba i posta',
        'severity': 'critical',
        'createdate': datetime.date.today()
    }

    alarm_id = set_new_alarm(db_con=db_con, **nointensity_alarm)
    # TODO check alarma noreading que invalida l'alarma nopower
    alarm_current = get_alarm_current_nointensity_string(db_con, check_time)

    set_devices_alarms(db_con, alarm_id, device_table, alarm_current, check_time)

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
    logger.debug("Updating alarms maintenance")
    create_alarm_table(db_con)
    create_alarm_status_table(db_con)
    create_alarm_historic_table(db_con)
    logger.debug("alarm tables creation checked")
    update_alarm_nopower_inverter(db_con)
    update_alarm_nointensity_string(db_con)
    logger.info("Updated alarms maintenance")


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


