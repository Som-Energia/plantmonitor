#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from maintenance.db_manager import DBManager

import pandas as pd

class DbTestFactory():

    def __init__(self, dbmanager):
        self.dbmanager = dbmanager

    def create(self, csv_file, table_name):
        df = pd.read_csv('test_data/{}'.format(csv_file), sep = ',', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
        df.to_sql(table_name, self.dbmanager.db_con, if_exists='replace', index = False)

    def create_custom_time(self, time_columns, csv_file, table_name):
        df = pd.read_csv('test_data/{}'.format(csv_file), sep = ';', parse_dates=time_columns, date_parser=lambda col: pd.to_datetime(col, utc=True))
        df.to_sql(table_name, self.dbmanager.db_con, if_exists='replace', index = False)

    def create_without_time(self, csv_file, table_name):
        df = pd.read_csv('test_data/{}'.format(csv_file), sep = ';')
        df.to_sql(table_name, self.dbmanager.db_con, if_exists='append', index = False)

    def delete(self, table_name):
        self.dbmanager.db_con.execute("DROP TABLE IF EXISTS {};".format(table_name))

    def create_bucket_5min_inverterregistry_empty_table(self):
        table_name = 'bucket_5min_inverterregistry'
        self.dbmanager.db_con.execute('create table {} (time timestamptz, inverter integer, temperature_dc bigint, power_w bigint, energy_wh bigint)'.format(table_name))