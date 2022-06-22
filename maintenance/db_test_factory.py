#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from external_api.api_solargis import ApiSolargis
from maintenance.db_manager import DBManager
from sqlalchemy import MetaData, Table, insert

import numpy as np
import pandas as pd

class DbTestFactory():

    def __init__(self, dbmanager):
        self.dbmanager = dbmanager

    def create(self, csv_file, table_name):
        df = pd.read_csv('test_data/{}'.format(csv_file), sep = ';', parse_dates=['time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
        df.to_sql(table_name, self.dbmanager.db_con, if_exists='replace', index = False)

    def create_custom_time(self, time_columns, csv_file, table_name):
        df = pd.read_csv('test_data/{}'.format(csv_file), sep = ';', parse_dates=time_columns, date_parser=lambda col: pd.to_datetime(col, utc=True))
        df.to_sql(table_name, self.dbmanager.db_con, if_exists='replace', index = False)

    def create_without_time(self, csv_file, table_name):
        df = pd.read_csv('test_data/{}'.format(csv_file), sep = ';')
        df.to_sql(table_name, self.dbmanager.db_con, if_exists='append', index = False)

    def delete(self, table_name):
        self.dbmanager.db_con.execute("DROP TABLE IF EXISTS {};".format(table_name))

    def create_bucket_5min_irradiationregistry_empty_table(self):
        table_name = 'bucket_5min_irradiationregistry'
        self.dbmanager.db_con.execute('create table {} (time timestamptz, sensor integer, irradiation_w_m2 bigint, temperature_dc bigint)'.format(table_name))

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


    # TODO use plant yaml (used in pony) instead of hardcoding structure
    def create_inverter_sensor_two_plants(self, sunrise, sunset):
        self.dbmanager.db_con.execute('create table if not exists plant (id serial primary key, name text, codename text, description text)')
        self.dbmanager.db_con.execute(
            "insert into plant(id, name, codename, description) values ({}, '{}', '{}', '{}'), ({}, '{}', '{}', '{}')".format(
                1, 'Alibaba', 'SomEnergia_Alibaba', '',
                2, 'TrucuAlmendrucu', 'SomEnergia_TrucuAlmendrucu', ''

            )
        )
        self.dbmanager.db_con.execute('create table if not exists inverter (id serial primary key, name text, plant integer)')
        self.dbmanager.db_con.execute(
            "insert into inverter(id, name, plant) values ({}, '{}', {}),({}, '{}', {}),({}, '{}', {})".format(
                1, 'Alibaba_inverter', 1,
                2, 'Quaranta_Lladres_inverter', 1,
                3, 'Almendrucu_inverter', 2
            )
        )
        self.dbmanager.db_con.execute('create table if not exists sensor (id serial primary key, name text, plant integer not null, description text, deviceColumname text)')
        self.dbmanager.db_con.execute(
            "insert into sensor(id, name, plant, description, deviceColumname) values ({}, '{}', {}, '{}', '{}'), ({}, '{}', {}, '{}', '{}')".format(
                1, 'SensorIrradiation1', 1, '', 'sensor',
                2, 'SensorIrradiation1', 2, '', 'sensor'
            )
        )

        self.dbmanager.db_con.execute('create table if not exists solarevent (id serial primary key, plant integer not null, sunrise timestamptz, sunset timestamptz)')
        self.dbmanager.db_con.execute(
            "insert into solarevent(id, plant, sunrise, sunset) values ({}, {}, '{}', '{}'),({}, {}, '{}', '{}')".format(
                1, 1,
                sunrise.strftime('%Y-%m-%d %H:%M:%S%z'),sunset.strftime('%Y-%m-%d %H:%M:%S%z'),
                2, 2,
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

    def add_more_strings_and_inverters(self):

        self.dbmanager.db_con.execute(
            "insert into inverter(id, name, plant) values (%s, %s, %s)",
            [
                (3, 'Wonderland_inverter', 1),
                (4, 'Borderland_inverter', 1),
                (5, 'Collierland_inverter', 1),
                (6, 'Dunklerland_inverter', 1)
            ]
        )

        string_tuple_list = [
            (2, 1, 'string11', ''),
            (3, 1, 'string22', ''),
            (4, 2, 'string11', ''),
            (5, 2, 'string22', ''),
            (6, 3, 'string18', ''),
            (7, 3, 'string19', ''),
            (8, 4, 'string11', ''),
            (9, 5, 'string22', '')
        ]

        self.dbmanager.db_con.execute(
            "insert into string(id, inverter, name, stringbox_name) values (%s, %s, %s, %s)", string_tuple_list
        )

    def create_inverter_string_plant_with_solar_events(self, sunrise, sunset):

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

    # TODO use plant yaml (used in pony) instead of hardcoding structure
    def create_plant_with_location(self):
        self.dbmanager.db_con.execute('create table if not exists plant (id serial primary key, name text, codename text, description text)')
        self.dbmanager.db_con.execute(
            "insert into plant(id, name, codename, description) values ({}, '{}', '{}', '{}'),({}, '{}', '{}', '{}')".format(
                1, 'Alibaba', 'SomEnergia_Alibaba', '',
                2, 'Quaranta_Lladres', 'SomEnergia_Quaranta_Lladres', ''
            )
        )

        self.dbmanager.db_con.execute('''
            create table if not exists plantlocation (
            id serial primary key,
            plant integer not null references plant (id),
            latitude DOUBLE PRECISION NOT NULL,
            longitude DOUBLE PRECISION NOT NULL)
        ''')

        self.dbmanager.db_con.execute(
            "insert into plantlocation(id, plant, latitude, longitude) values ({}, {}, {}, {}),({}, {}, {}, {})".format(
                1, 1, 40.932389, -4.968694,
                2, 2, 39.440722, -0.428722
            )
        )

    def create_solargis(self, csv_file=None):

        # TODO set table name as a variable/parametrize the static method
        solargis_table = 'satellite_readings'
        # TODO assumes you've created plant table
        ApiSolargis.create_table(self.dbmanager.db_con)

        if csv_file:
            df = pd.read_csv(f'test_data/{csv_file}', sep = ';', parse_dates=['time', 'request_time'], date_parser=lambda col: pd.to_datetime(col, utc=True))
            df.replace({np.nan:None}, inplace=True)

            readings = df.to_dict(orient='records')

            metadata_obj = MetaData()
            clean_irradiation_table = Table(solargis_table, metadata_obj, autoload_with=self.dbmanager.db_con)
            insert_stmt = insert(clean_irradiation_table)

            self.dbmanager.db_con.execute(insert_stmt, readings)

    def create_meter_plant(self, sunrise, sunset):
        self.dbmanager.db_con.execute('create table if not exists plant (id serial primary key, name text, codename text, description text)')
        self.dbmanager.db_con.execute('insert into plant (id, name, codename, description) values (%s, %s, %s, %s)', [
                (1, 'Alibaba', 'SomEnergia_Alibaba', ''),
                (2, 'Alicia', 'SomEnergia_Alicia', ''),
                (3, 'Jules', 'SomEnergia_Jules', ''),
                (4, 'Arsene', 'SomEnergia_Arsene', '')
        ])

        self.dbmanager.db_con.execute('create table if not exists meter (id serial primary key, name text, plant integer, connection_protocol text)')
        self.dbmanager.db_con.execute('insert into meter (id, name, plant, connection_protocol) values (%s, %s, %s, %s)', [
                (2, 'Alibaba_meter', 1, 'ip'),
                (7, 'Meravelles_meter', 2, 'moxa'),
                (34, 'Verne_meter', 3, 'moxa',),
                (36, 'Lupin_meter', 4, 'moxa'),
        ])
        self.dbmanager.db_con.execute('create table if not exists solarevent (id serial primary key, plant integer not null, sunrise timestamptz, sunset timestamptz)')
        self.dbmanager.db_con.execute(
            "insert into solarevent(id, plant, sunrise, sunset) values ({}, {}, '{}', '{}')".format(
                1, 1,
                sunrise.strftime('%Y-%m-%d %H:%M:%S%z'),sunset.strftime('%Y-%m-%d %H:%M:%S%z')
            )
        )
