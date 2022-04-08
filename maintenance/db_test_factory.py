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

    def create_bucket_5min_stringregistry_empty_table(self):
        self.dbmanager.db_con.execute("""
        CREATE TABLE "bucket_5min_stringregistry" (
            "time" TIMESTAMP WITH TIME ZONE NOT NULL,
            "string" INTEGER NOT NULL,
            "intensity_ma" BIGINT
        );""")


class DbPlantFactory():

    def __init__(self, dbmanager):
        self.dbmanager = dbmanager

    # TODO use plant yaml (used in pony) instead of hardcoding structure
    def create_inverter_sensor_plant(self, sunrise, sunset):
        self.dbmanager.db_con.execute('create table if not exists plant (id serial primary key, name text, codename text, description text)')
        self.dbmanager.db_con.execute(
            "insert into plant(id, name, codename, description) values ({}, '{}', '{}', '{}')".format(
                1, 'Alibaba', 'SomEnergia_Alibaba', ''
            )
        )
        self.dbmanager.db_con.execute('create table if not exists inverter (id serial primary key, name text, plant integer)')
        self.dbmanager.db_con.execute(
            "insert into inverter(id, name, plant) values ({}, '{}', {}),({}, '{}', {})".format(
                1, 'Alibaba_inverter', 1,
                2, 'Quaranta_Lladres_inverter', 1
            )
        )
        self.dbmanager.db_con.execute('create table if not exists sensor (id serial primary key, name text, plant integer not null, description text, deviceColumname text)')
        self.dbmanager.db_con.execute(
            "insert into sensor(id, name, plant, description, deviceColumname) values ({}, '{}', {}, '{}', '{}')".format(
                1, 'SensorIrradiation1', 1, '', 'sensor'
            )
        )

        self.dbmanager.db_con.execute('create table if not exists solarevent (id serial primary key, plant integer not null, sunrise timestamptz, sunset timestamptz)')
        self.dbmanager.db_con.execute(
            "insert into solarevent(id, plant, sunrise, sunset) values ({}, {}, '{}', '{}')".format(
                1, 1,
                sunrise.strftime('%Y-%m-%d %H:%M:%S%z'),sunset.strftime('%Y-%m-%d %H:%M:%S%z')
            )
        )

    def create_inverter_string_plant(self):

        self.dbmanager.db_con.execute('create table if not exists plant (id serial primary key, name text, codename text, description text)')
        self.dbmanager.db_con.execute(
            "insert into plant(id, name, codename, description) values ({}, '{}', '{}', '{}')".format(
                1, 'Alibaba', 'SomEnergia_Alibaba', ''
            )
        )
        self.dbmanager.db_con.execute('create table if not exists inverter (id serial primary key, name text, plant integer)')
        self.dbmanager.db_con.execute(
            "insert into inverter(id, name, plant) values ({}, '{}', {}),({}, '{}', {})".format(
                1, 'Alibaba_inverter', 1,
                2, 'Quaranta_Lladres_inverter', 1
            )
        )
        self.dbmanager.db_con.execute("""
            CREATE TABLE "string" (
            "id" SERIAL PRIMARY KEY,
            "inverter" INTEGER NOT NULL,
            "name" TEXT NOT NULL,
            "stringbox_name" TEXT
            );

            CREATE INDEX "idx_string__inverter" ON "string" ("inverter");

            ALTER TABLE "string" ADD CONSTRAINT "fk_string__inverter" FOREIGN KEY ("inverter") REFERENCES "inverter" ("id") ON DELETE CASCADE;
        """)

        self.dbmanager.db_con.execute(
            "insert into string(id, inverter, name, stringbox_name) values (%s, %s, %s, %s)",(
                1, 1, 'string1', ''
            )
        )
