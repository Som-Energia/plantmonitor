#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from scipy import integrate
import pandas as pd
from datetime import datetime

from pony import orm
from ORM.models import database
from ORM.models import (
    Plant,
    Meter,
    MeterRegistry,
    Inverter,
    InverterRegistry,
    Sensor,
    SensorIntegratedIrradiation,
    SensorIrradiation,
    SensorTemperatureAmbient,
    SensorTemperatureModule,
    SensorIrradiationRegistry,
    SensorTemperatureAmbientRegistry,
    SensorTemperatureModuleRegistry,
    IntegratedIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)

def trapezoidal_approximation(sensor_df, from_date, to_date, outputDataFormat='%Y-%m-%d %H:%M:%S%z', timeSpacing=5./60., column2integrateTitle=0):
    ''' Trapezoidal aproximation of the 24 hours following first sensor entry'''

    '''Get the first date entry and create hourly time entries'''
    ts_start = pd.Series(pd.date_range(start=from_date, end=to_date, freq='H'))

    deltaT = pd.Timedelta(hours=1)
    ts_end = ts_start + deltaT
    timeStrings = ts_end.dt.tz_convert('Europe/Zurich').dt.strftime(outputDataFormat)

    integrals = []

    print('Starting integration')

    for start, timeString in zip(ts_start, timeStrings):
        end = start + deltaT

        interval4integral = sensor_df.loc[start:end]

        ''' Trapezoidal aproximation of the hour following first sensor entry '''
        test_integral = integrate.trapz(y = interval4integral[column2integrateTitle], dx = timeSpacing)

        integrals.append((timeString, test_integral))
    return integrals

def parse_csv(input_data_file, column2integrate, delimiter, decimal):

    date_col = 0
    time_col = 1

    guessed_encoding = find_encoding(input_data_file)

    columnNames = pd.read_csv(input_data_file, nrows=0, delimiter=delimiter, encoding=guessed_encoding).columns

    if len(columnNames) <= column2integrate:
        print('Requested column \'{}\', but only {} available.'.format(column2integrate, len(columnNames)))
        sys.exit(-1)

    print('\033[94m\033[1mProcessing column {}\033[0m'.format(columnNames[column2integrate]))

    sensor = pd.read_csv(input_data_file, usecols=[date_col,time_col,column2integrate], delimiter=delimiter,
                          encoding=guessed_encoding, decimal=decimal,
                          na_values=[' -nan'], engine='python', parse_dates=[[date_col,time_col]],
                          infer_datetime_format=True, dayfirst=True)

    sensor.rename(columns={'DATE_ TIME':'datetimeECT'}, inplace=True)

    sensor['datetimeUTC'] = pd.to_datetime(sensor['datetimeECT']).dt.tz_localize('Europe/Zurich', ambiguous='infer')
    sensor.set_index('datetimeUTC', inplace=True)
    sensor.drop(['datetimeECT'], axis=1, inplace=True)

    return sensor

def parse_xlsx(input_data_file, column2integrate):

    datetime_col = 0

    columnNames = pd.read_excel(io=input_data_file, header=0).columns

    if len(columnNames) <= column2integrate:
        print('Requested column \'{}\', but only {} available.'.format(column2integrate, len(columnNames)))
        sys.exit(-1)

    print('\033[94m\033[1mProcessing column {}\033[0m'.format(columnNames[column2integrate]))

    sensor = pd.read_excel(input_data_file, usecols=[datetime_col, column2integrate], index_col=datetime_col, parse_dates=True, convert_float=False)

    sensor = sensor.tz_localize('Europe/Zurich', ambiguous='infer')

    return sensor


def dropNonMonotonicRows(df):
    ''' strictly monotonic increasing '''
    anomalies = [r[1] for r in zip(df.index, df.index[1:]) if r[0] >= r[1]]
    df.drop(index=anomalies, inplace=True)
    return anomalies

def irradiance_to_df(sensor):
    query = orm.select(r for r in SensorIrradiationRegistry if r.sensor == sensor).order_by(SensorIrradiationRegistry.time)
    column_names = query[:]._col_names
    # alternative
    if query[:]:
        column_names = query[:].first().to_dict().keys()
    df = pd.DataFrame([r.to_dict() for r in query[:]])
    return df


def main():

    '''TODO: use config file'''
    outputDataFormat = '%Y-%m-%d %H:%M:%S%z'

    '''TODO: sanitize'''
    column2integrate = sys.argv[1]
    input_data_file = sys.argv[2]
    output_data_file = sys.argv[3]
    decimal = sys.argv[4]
    delimiter = sys.argv[5]
    # decimal=',' # European decimal point

    column2integrate = int(column2integrate)

    '''TODO: utf sandwich instead of keeping guessed_encoding'''

    if input_data_file.endswith('csv'):

        sensor_df = parse_csv(input_data_file, column2integrate=column2integrate, delimiter=delimiter, decimal=decimal)

    elif input_data_file.endswith('xls') or input_data_file.endswith('xlsx'):

        sensor_df = parse_xlsx(input_data_file, column2integrate=column2integrate)

    else:
        print("Only .csv, .xls and .xlsx are supported")
        sys.exit(-2)

    ''' sensor_df is a one-column dataframe with datetime column as index '''

    columnTitle = sensor_df.columns[0]

    ''' Time in hours between readings '''
    timeSpacing = 5./60.

    from_date = sensor_df.index[0].floor('d')
    to_date   = sensor_df.index[-1].ceil('d')

    anomalies = False

    if not sensor_df.index.is_monotonic_increasing:
        print("Index is NOT monotonic! Attempting automatic fix")
        anomalies = dropNonMonotonicRows(sensor_df)
        print("\n\033[94m\033[1m[WARNING] Dropped rows with dates: {}\033[0m".format(anomalies))

    integrals = trapezoidal_approximation(sensor_df, from_date, to_date, outputDataFormat, timeSpacing, columnTitle)

    integralsDF = pd.DataFrame(data = integrals, columns = ['datetime', columnTitle])
    integralsDF.to_csv(output_data_file, sep=';', decimal=',', encoding='utf-8', index=False)

    print("Saved {} records from {} to {}".format(len(integralsDF), from_date, to_date))
    asciigraph_print(integrals)

    if anomalies:
        print("\n\033[94m\033[1m[WARNING] Dropped rows with dates: {}\033[0m".format(anomalies))

    print("\nJob's done, have a good day\n")
