#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from zeep import Client

from yamlns import namespace as ns

import psycopg2

from psycopg2 import Error

from datetime import datetime, date, timedelta
import pytz
import sys

def setUp():

    conn = psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'], host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db'])
    cur = conn.cursor()

    # Connexió postgresql
    with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'], host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE if not exists forecastHead(id SERIAL NOT NULL, errorCode VARCHAR(50), facilityId VARCHAR(50), \
            variableId VARCHAR(50), predictorId VARCHAR(20), forecastDate TIMESTAMPTZ, granularity INTEGER, PRIMARY KEY(id));")
            cur.execute("CREATE TABLE forecastData(idForecastHead SERIAL REFERENCES forecastHead(id), time TIMESTAMPTZ, percentil10 INTEGER, percentil50 INTEGER, \
            percentil90 INTEGER, PRIMARY KEY(idForecastHead,time));")
            cur.execute("SELECT create_hypertable('forecastData', 'time');")

    return

def tearDown():
    # Connexió postgresql
    with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'], host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE forecastData;")
            cur.execute("DROP TABLE forecastHead;")

def main():

    configdb = ns.load('config.yaml')

    username = configdb['psql_user']

    # Connexió api meteologica

    client = Client(configdb['meteo_test_url'])
    meteoCredentials = {'username': configdb['meteo_user'], 'password': configdb['meteo_password']}

    loginResult = client.service.login(meteoCredentials)

    if loginResult.errorCode != 'OK':
        print('Connection failed with error code {}'.format(loginResult.errorCode))
        sys.exit(-1)

    print('Connected with sessionToken')

    keepAlive = loginResult
    keepAlive = client.service.keepAlive(keepAlive)

    request = keepAlive
    head = keepAlive['header']

    #client.service.getForecast({'header': header, 'facilityId': 'SomEnergia_Alcolea', 'variableId': 'prod', 'predictorId':'aggregated'})

    facilitiesResponse = client.service.getAllFacilities(request)

    variableId = 'prod'
    predictorId = 'aggregated'
    granularity = '60'
    utcnow = datetime.utcnow()
    forecastDate = utcnow.astimezone(pytz.timezone("Europe/Zurich")).isoformat()
    fromDate = forecastDate
    utcthen = utcnow + timedelta(days=1)
    toDate = utcthen.astimezone(pytz.timezone("Europe/Zurich")).isoformat()

    #forecastRequest = {'header': head, 'facilityId': 'SomEnergia_Alcolea', 'variableId': variableId, 'predictorId': predictorId, 'forecastDate': forecastDate, 'fromDate': fromDate, 'toDate': toDate}

    # Connexió postgresql
    with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
    host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
        with conn.cursor() as cur:
            for facilityItem in facilitiesResponse['facilityItems']['item']:

                print("Processing facility {}".format(facilityItem['facilityName']))

                facilityId = facilityItem['facilityId']
                forecastRequest = {'header': head, 'facilityId': facilityId, 'variableId': variableId,
                'predictorId': predictorId, 'granularity': granularity, 'forecastDate': forecastDate,
                'fromDate': fromDate, 'toDate': toDate}
                forecast = client.service.getForecast(forecastRequest)

                errorCode = forecast['errorCode']
                forecastData = forecast['forecastData']

                cur.execute("INSERT INTO forecastHead(errorCode, facilityId, variableId, \
                predictorId, forecastDate, granularity) VALUES ('{}', '{}', '{}', '{}', '{}', '{}') \
                RETURNING id;".format(errorCode, facilityId, variableId, predictorId, forecastDate, granularity))
                currentIdForecastHead = cur.fetchone()[0]

                forecastDataDict = [entry.split('~') for entry in forecastData.split(':') if entry] # first entry is empty, probably slicing is faster than filtering

                for entry in forecastDataDict:
                    unixts, percentil10, percentil50, percentil90 = entry
                    cur.execute("INSERT INTO forecastData(idForecastHead, time, \
                    percentil10, percentil50, percentil90) VALUES ('{}', to_timestamp('{}'), \
                    '{}', '{}', '{}');".format(currentIdForecastHead, unixts, percentil10,
                    percentil50, percentil90))

if __name__ == "__main__":
    print("Starting job")
    main()
    print("Job's Done, Have a Nice Day")
