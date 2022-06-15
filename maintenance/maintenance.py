#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import numpy as np

import yaml

from sqlalchemy import BigInteger, MetaData, Table, Text, Integer, Column, DateTime, ForeignKey
from sqlalchemy import insert
from conf.log import logger

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

def update_bucketed_sensorirradiation_registry(db_con, to_date=None):
    to_date = to_date or datetime.datetime.now(datetime.timezone.utc)

    registry = 'sensorirradiationregistry'
    device = 'sensor'
    source_table = registry
    target_table = 'bucket_5min_{}'.format(source_table)

    setup_5min_table = f'''
        CREATE TABLE IF NOT EXISTS
            {target_table}
            (time timestamptz, {device} integer, irradiation_w_m2 bigint, temperature_dc bigint);

        ALTER INDEX IF EXISTS time_sensor RENAME TO {target_table}_time_{device};

        CREATE UNIQUE INDEX IF NOT EXISTS {target_table}_time_{device}
            ON {target_table} (time, {device});

        SELECT create_hypertable('{target_table}', 'time', if_not_exists => TRUE)
    '''
    db_con.execute(setup_5min_table)

    latest_reading = get_latest_reading(db_con, target_table, source_table)
    logger.debug(f"Latest reading of {device} {latest_reading}")
    query = Path(f'queries/maintenance/{target_table}.sql').read_text(encoding='utf8')
    query = query.format(latest_reading, to_date.strftime('%Y-%m-%d %H:%M:%S%z'))
    insert_query = f'''
        INSERT INTO {target_table}
         {query}
         ON CONFLICT (time, {device}) DO
            UPDATE
	            SET irradiation_w_m2 = excluded.irradiation_w_m2,
	            temperature_dc = excluded.temperature_dc
        RETURNING time, {device}, irradiation_w_m2, temperature_dc
    '''
    logger.debug("Insert query")
    return db_con.execute(insert_query).fetchall()


def update_bucketed_inverter_registry(db_con, to_date=None):
    to_date = to_date or datetime.datetime.now(datetime.timezone.utc)
    setup_5min_table = '''
        CREATE TABLE IF NOT EXISTS
            bucket_5min_inverterregistry
            (time timestamptz, inverter integer, temperature_dc bigint, power_w bigint, energy_wh bigint);

        CREATE UNIQUE INDEX IF NOT EXISTS bucket_5min_inverterregistry_time_inverter
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

        CREATE UNIQUE INDEX IF NOT EXISTS bucket_5min_stringregistry_time_string
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
    result = db_con.execute(insert_query).fetchall()
    logger.debug("{} records inserted".format(len(result)))
    return result

def update_irradiationregistry(db_con, to_date=None):
    to_date = to_date or datetime.datetime.now(datetime.timezone.utc)

    source_table = 'bucket_5min_sensorirradiationregistry'
    target_table = 'irradiationregistry'

    setup_irradiation = f'''
        CREATE TABLE IF NOT EXISTS
            {target_table}
            (time TIMESTAMP WITH TIME ZONE NOT NULL, sensor integer not null, irradiation_wh_m2 float, quality float);

        CREATE UNIQUE INDEX IF NOT EXISTS irradiationregistry_time_sensor
            ON {target_table} (time, sensor);

        SELECT create_hypertable('{target_table}', 'time', if_not_exists => TRUE, migrate_data => TRUE)
    '''
    db_con.execute(setup_irradiation)

    latest_reading = get_latest_reading(db_con, target_table, source_table)

    logger.debug(f"Latest reading of string {latest_reading}")
    query = Path('queries/maintenance/bucket_1h_irradiance.sql').read_text(encoding='utf8')
    query = query.format(latest_reading, to_date.strftime('%Y-%m-%d %H:%M:%S%z'))

    insert_query = f'''
        INSERT INTO {target_table}
         {query}
         ON CONFLICT (time, sensor) DO
            UPDATE
	            SET irradiation_wh_m2 = excluded.irradiation_wh_m2,
                quality = excluded.quality
        RETURNING time, sensor, irradiation_wh_m2, quality
    '''
    logger.debug("Insert query")
    result = db_con.execute(insert_query).fetchall()
    logger.debug("{} records inserted".format(len(result)))
    return result

def create_clean_irradiation(db_con, target_table_name):

    # let's see how sqlalchemy does it

    metadata_obj = MetaData()

    sources_table = Table('source', metadata_obj,
        Column('id', Integer, primary_key=True),
        Column('name', Text, nullable=False),
        Column('description', Text)
    )

    clean_irradiation = Table(target_table_name, metadata_obj,
        Column('sensor', Integer, nullable=False, primary_key=True),
        Column('time', DateTime(timezone=True), primary_key=True),
        Column('irradiation_w_m2', BigInteger),
        Column('temperature_dc', BigInteger),
        Column('source', Integer, ForeignKey("source.id"))
    )

    metadata_obj.create_all(db_con)

    sources_data = [
        {'name': 'raw', 'description': 'original values'},
        {'name': 'satellite readings', 'description': 'satellite readings from external apis (e.g. solargis)'}
    ]

    db_con.execute(sources_table.insert(), sources_data)

    create_hipertable_query = f'''
        CREATE UNIQUE INDEX IF NOT EXISTS time_sensor
            ON {target_table_name} (time, sensor);

        SELECT create_hypertable('{target_table_name}', 'time', if_not_exists => TRUE)
    '''

    db_con.execute(create_hipertable_query)

    # setup_target_table = f'''
    #     CREATE TABLE IF NOT EXISTS
    #        {target_table}
    #         (
    #             time TIMESTAMP WITH TIME ZONE NOT NULL,
    #             sensor integer not null,
    #             irradiation_w_m2 bigint,
    #             temperature_dc bigint,
    #             source integer not null,
    #             CONSTRAINT "fk_clean_irradiation__sources" FOREIGN KEY ("source") REFERENCES "source" ("id") ON DELETE CASCADE
    #         );

    #     CREATE UNIQUE INDEX IF NOT EXISTS time_sensor
    #         ON {target_table} (time, sensor);

    #     SELECT create_hypertable('{target_table}', 'time', if_not_exists => TRUE)
    # '''
    # db_con.execute(setup_target_table)

def get_irradiation_readings_with_plant(db_con, source_table, from_date, to_date):
    from_date_str = from_date.strftime("%Y-%m-%d %H:%M:%S%z")
    to_date_str = to_date.strftime("%Y-%m-%d %H:%M:%S%z")

    device = 'sensor'
    # where assumes UTC otherwise it would be wrong on DST change days
    query = f'''
        SELECT reg.time as time, dev.plant as plant, reg.sensor AS sensor, reg.irradiation_w_m2, reg.temperature_dc
        FROM {source_table} as reg
        LEFT JOIN {device} AS dev ON dev.id = reg.{device}
        WHERE dev.plant IS NOT NULL
        AND '{from_date_str}'::timestamptz <= reg.time and reg.time <= '{to_date_str}'::timestamptz
        order by reg.time desc, dev.plant, reg.sensor
    '''
    queryresult = db_con.execute(query)
    return queryresult.fetchall(), queryresult.keys()

def get_satellite_irradiation_readings(db_con, from_date, to_date):

    # solargis uses half-hour time point
    from_date = from_date.replace(second=0, minute=0) - datetime.timedelta(hours=1)
    to_date = to_date.replace(second=0, minute=0) + datetime.timedelta(hours=1)

    from_date_str = from_date.strftime("%Y-%m-%d %H:%M:%S%z")
    to_date_str = to_date.strftime("%Y-%m-%d %H:%M:%S%z")

    # where assumes UTC otherwise it would be wrong on DST change days
    query = f'''
        SELECT *
        FROM satellite_readings as reg
        WHERE '{from_date_str}'::timestamptz <= reg.time and reg.time <= '{to_date_str}'::timestamptz
        order by reg.time
    '''
    queryresult = db_con.execute(query)

    return queryresult.fetchall(), queryresult.keys()

def satellite_upsampling(sat_df):

    sat_df_per_plant = sat_df.set_index('time').groupby('plant')

    # resample hourly to 5minutal
    # there are other options if this one is too slow
    # TODO test speed on our data style (~11 groups max, hourly upsampling)

    # Option 1:
    # assumes equispaced time index
    # sat_df_per_plant.resample('5T')[['global_horizontal_irradiation_wh_m2','global_tilted_irradiation_wh_m2']].interpolate()

    # Option 2:
    # apply reset per group foreach group
    #sat_df_per_plant[['global_horizontal_irradiation_wh_m2','global_tilted_irradiation_wh_m2']].apply(lambda g: g.resample('5T').interpolate(method='index'))
    sat_df_5min = sat_df_per_plant[[
            'global_horizontal_irradiation_wh_m2',
            'global_tilted_irradiation_wh_m2',
            'module_temperature_dc',
            'photovoltaic_energy_output_wh'
        ]].apply(lambda group: group.resample('5T').interpolate('index'))

    # Option3:
    # concat interpolate per group

    sat_df_5min[['source', 'request_time']] = sat_df_per_plant[['source', 'request_time']].resample('5T').backfill()

    return sat_df_5min

def clean_irradiation_readings(db_con, source_table, from_date, to_date):

    irradiation_readings, keys = get_irradiation_readings_with_plant(db_con, source_table, from_date, to_date)
    satellite_irradiation_readings, satellite_keys = get_satellite_irradiation_readings(db_con, from_date, to_date)
    sources = pd.read_sql('source', db_con)

    irr_df = pd.DataFrame(irradiation_readings, columns=keys)
    sat_df = pd.DataFrame(satellite_irradiation_readings, columns=satellite_keys)

    sat_df_5min = satellite_upsampling(sat_df)

    irr_ts_df = irr_df.set_index(['plant','time']) #.asfreq('5T') #we assume it's bucketed already
    sat_ts_df = sat_df_5min#.reset_index('plant')
    sat_ts_df.rename(columns={'global_tilted_irradiation_wh_m2': 'irradiation_w_m2', 'module_temperature_dc': 'temperature_dc'}, inplace=True)

    irr_ts_df['irradiation_w_m2_source'] = 'sonda'
    irr_ts_df['temperature_dc_source'] = 'sonda'
    irr_ts_df.loc[irr_ts_df.irradiation_w_m2.isnull(), 'irradiation_w_m2_source'] = 'solargis'
    irr_ts_df.loc[irr_ts_df.temperature_dc.isnull(), 'temperature_dc_source'] = 'solargis'
    irr_ts_df.update(sat_ts_df, overwrite=False)

    irr_ts_df['source'] = irr_ts_df.apply(
        lambda r: 'solargis' if r.irradiation_w_m2_source == 'solargis' or r.temperature_dc_source == 'solargis' else 'sonda',
        axis=1
    )

    irr_ts_df.source = irr_ts_df.irradiation_w_m2_source.map({'sonda':'raw','solargis':'satellite readings'}).map(sources.set_index('name').id)

    irr_ts_df.reset_index(inplace=True)

    irr_ts_df.drop(columns=['plant'], inplace=True)
    irr_ts_df.drop(columns=['irradiation_w_m2_source', 'temperature_dc_source'], inplace=True)

    clean_irradiation_readings_df = irr_ts_df

    return clean_irradiation_readings_df

def insert_ts_readings(db_con, target_table, clean_readings):
    metadata_obj = MetaData()
    clean_irradiation_table = Table(target_table, metadata_obj, autoload_with=db_con)
    insert_stmt = insert(clean_irradiation_table).returning(clean_irradiation_table.c.time)
    return db_con.execute(insert_stmt, clean_readings)

def insert_clean_irradiation_readings(db_con, target_table, clean_readings_df):

    clean_readings_df_nones = clean_readings_df.copy()

    # replace nans with Nones otherwise insert complains nan overflows bigint
    clean_readings_df_nones[['irradiation_w_m2', 'temperature_dc']] = clean_readings_df_nones[['irradiation_w_m2', 'temperature_dc']].replace({np.nan: None})
    clean_readings = clean_readings_df_nones.to_dict(orient='records')

    return insert_ts_readings(db_con, target_table, clean_readings)

def irradiance_cleaning(db_con, to_date=None):
    metric = 'sensorirradiationregistry'
    source_table = f'bucket_5min_{metric}'
    target_table = f'clean_{metric}'
    to_date = to_date or datetime.datetime.now(datetime.timezone.utc)

    # TODO check that source table exists and throw or just return if it doesnt
    if not db_con.dialect.has_table(db_con, source_table):
        logger.error(f"{source_table} does not exist, we can't clean it.")
        return

    if not db_con.dialect.has_table(db_con, target_table):
        create_clean_irradiation(db_con, target_table)

    latest_reading = get_latest_reading(db_con, target_table, source_table)
    logger.debug(f"Latest reading {target_table} is {latest_reading}")
    clean_readings_df = clean_irradiation_readings(db_con=db_con, source_table=source_table, from_date=latest_reading, to_date=to_date)
    result = insert_clean_irradiation_readings(db_con, target_table, clean_readings_df)
    logger.debug(result)
    return clean_readings_df

def irradiance_cleaning_via_sql(db_con, to_date=None):

    metric = 'sensorirradiationregistry'
    source_table = f'bucket_5min_{metric}'
    target_table = f'clean_{metric}'
    to_date = to_date or datetime.datetime.now(datetime.timezone.utc)

    # TODO check that source table exists and throw or just return if it doesnt
    if not db_con.dialect.has_table(db_con, source_table):
        logger.error(f"{source_table} does not exist, we can't clean it.")
        return

    if not db_con.dialect.has_table(db_con, target_table):
        create_clean_irradiation(db_con, target_table)

    latest_reading = get_latest_reading(db_con, target_table, source_table)
    logger.debug(f"Latest reading {target_table} is {latest_reading}")

    query = Path(f'queries/maintenance/clean_{source_table}.sql').read_text(encoding='utf8')
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
    result = db_con.execute(insert_query).fetchall()
    logger.debug("{} records inserted".format(len(result)))
    return result

def alarm_maintenance(db_con):
    alarm_manager = AlarmManager(db_con)
    alarm_manager.update_alarms()

def bucketed_registry_maintenance(db_con):
    logger.debug("Updating bucketed registries table")
    update_bucketed_inverter_registry(db_con)
    logger.info('Updated bucketed inverter registry table')
    update_bucketed_string_registry(db_con)
    logger.info('Update bucketed inverterstring registry table')
    update_bucketed_sensorirradiation_registry(db_con)
    logger.info('Update bucketed sensorirradiation registry table')

def cleaning_maintenance(db_con):
    update_irradiationregistry(db_con)
    logger.info('Update irradiation registry table')

    # Unused, irradiance is not used anywhere, only irradiation (wh/m2) is useful
    # leaving it in case we ever change our mind or we have a similar problem
    # logger.info('Clean irradiation')
    # irradiance_cleaning(db_con)

# Pending:
# Dashboard: Alarma 3 Inversors temperatura anòmala històric temps real->
#   - https://redash.somenergia.coop/queries/137/source?p_interval=d_this_year#218
#   - https://redash.somenergia.coop/queries/163/source#266
#

# INVERSOR - ALARMA 2: INTENSITATS ENTRADA INVERSOR
#    Strings? stringregistry <- dependencia
#   - https://redash.somenergia.coop/queries/129/source#207


