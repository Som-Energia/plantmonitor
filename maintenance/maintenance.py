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
    column_name = 'irradiation_w_m2'
    df[column_name] = df[column_name].dt.round('5min')

def round_dt_to_5minutes(df, colname):
    df[colname] = df[colname].dt.round('5min')
    return df

