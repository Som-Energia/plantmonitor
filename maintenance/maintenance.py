#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

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

    import pdb; pdb.set_trace()
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

# idea use a simple periodic sql query that adds rows to the derivate table
# TODO assumes table exists
def alarm_maintenance_via_sql(db_con, query):
    result = db_con.execute(query)

    return result

def alarm_maintenance():
    pass