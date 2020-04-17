#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from zeep import Client

from yamlns import namespace as ns

import psycopg2

from psycopg2 import Error
import psycopg2.extras

from datetime import datetime, date, timedelta, timezone
import pytz
import time

import sys

import collections

def setUp():

    conn = psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
    host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db'])
    cur = conn.cursor()

    # Connexió postgresql
    with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
    host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE if not exists forecastHead(id SERIAL NOT NULL, errorCode VARCHAR(50), facilityId VARCHAR(50), \
            variableId VARCHAR(50), predictorId VARCHAR(20), forecastDate TIMESTAMPTZ, granularity INTEGER, PRIMARY KEY(id));")
            cur.execute("CREATE TABLE forecastData(idForecastHead SERIAL REFERENCES forecastHead(id), time TIMESTAMPTZ, percentil10 INTEGER, percentil50 INTEGER, \
            percentil90 INTEGER, PRIMARY KEY(idForecastHead,time));")
            cur.execute("SELECT create_hypertable('forecastData', 'time');")

    return

def tearDown():
    # Connexió postgresql
    with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
    host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE forecastData;")
            cur.execute("DROP TABLE forecastHead;")

def clearDb():
    with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
    host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM forecastData;")
            cur.execute("DELETE FROM forecastHead;")

def parseArguments():
    args = ns()
    if len(sys.argv) == 3:
        args['variable'] = sys.argv[1]
        args['fromDate'] = sys.argv[2]
        args['toDate'] = sys.argv[3]
        return args
    else:
        args['variable'] = 'prod'
        return args

def addtz(utc_datetime):
    return utc_datetime.replace(tzinfo=timezone.utc).astimezone(pytz.timezone("Europe/Zurich")).isoformat();

def unixToISOtz(unix_ts):
    return datetime.utcfromtimestamp(int(unix_ts)).replace(tzinfo=timezone.utc).astimezone(pytz.timezone("Europe/Zurich")).isoformat();

def forecast():

    configdb = ns.load('config.yaml')

    timeDelta = configdb['time_delta']
    username = configdb['psql_user']

    # Connexió api meteologica

    client = Client(configdb['meteo_test_url'])
    meteoCredentials = {'username': configdb['meteo_user'], 'password': configdb['meteo_password']}

    loginResult = client.service.login(meteoCredentials)

    if loginResult.errorCode != 'OK':
        print('Connection failed with error code {}'.format(loginResult.errorCode))
        sys.exit(-1)

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
    forecastDate = '2020-04-14T00:00:00.0000+02:00' #addtz(utcnow)
    fromDate = forecastDate
    utcthen = utcnow + timedelta(days=30)
    toDate = '2020-04-17T00:00:00.0000+02:00' #addtz(utcthen)

    #forecastRequest = {'header': head, 'facilityId': 'SomEnergia_Alcolea', 'variableId': variableId, 'predictorId': predictorId, 'forecastDate': forecastDate, 'fromDate': fromDate, 'toDate': toDate}

    # Connexió postgresql

    start = time.perf_counter()

    with psycopg2.connect(user = configdb['psql_user'], password = configdb['psql_password'],
    host = configdb['psql_host'], port = configdb['psql_port'], database = configdb['psql_db']) as conn:
        with conn.cursor() as cur:
            for facilityItem in facilitiesResponse['facilityItems']['item']:

                print("Processing facility {}".format(facilityItem['facilityName']))
                ministart = time.perf_counter()

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


                if errorCode == 'OK':
                    forecastDataDict = [entry.split('~') for entry in forecastData.split(':') if entry] # first entry is empty, probably slicing is faster than filtering
                    realFromDate = unixToISOtz(forecastDataDict[0][0]);
                    realToDate   = unixToISOtz(forecastDataDict[-1][0]);

                    cur.execute("DELETE FROM forecastdata USING forecasthead WHERE forecastdata.idforecasthead = forecasthead.id AND forecasthead.facilityId = '{}' AND time BETWEEN '{}' AND '{}'".format(facilityId, realFromDate, realToDate))

                    #https://hakibenita.com/fast-load-data-python-postgresql

                    psycopg2.extras.execute_values(cur, "INSERT INTO forecastData VALUES %s;", ((
                        currentIdForecastHead,
                        unixToISOtz(record[0]),
                        record[1],
                        record[2],
                        record[3],
                    ) for record in forecastDataDict), page_size=1000)

                elapsed = time.perf_counter() - ministart
                print(f"\t{facilityItem['facilityName']} ({facilityId}) {elapsed:0.4} s")

    elapsed = time.perf_counter() - start
    print(f'Total elapsed time {elapsed:0.4}')

def main():
    args = parseArguments()
    forecast()


if __name__ == "__main__":
    print("Starting job")
    main()
    print("Job's Done, Have a Nice Day")
