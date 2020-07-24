#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from yamlns import namespace as ns
import datetime as dt

from meteologica.plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBError,
)

from meteologica.meteologica_api_utils import (
    MeteologicaApi,
    MeteologicaApiError,
)

from meteologica.utils import todt

import time

import sys


def parseArguments():
    # TODO parse arguments into a ns
    args = ns()
    if len(sys.argv) == 3:
        args[sys.argv[1]] = sys.argv[2]
        return args
    else:
        return args


def download_meter_data(configdb, test_env=True):

    if test_env:
        target_wsdl = configdb['meteo_test_url']
    else:
        target_wsdl = configdb['meteo_url']

    params = dict(
        wsdl=target_wsdl,
        username=configdb['meteo_user'],
        password=configdb['meteo_password'],
        lastDateFile='lastDateFile.yaml',
        lastDateDownloadFile='lastDateDownloadFile.yaml',
        showResponses=False,
    )

    start = time.perf_counter()

    with MeteologicaApi(**params) as api:
        with PlantmonitorDB(configdb) as db:

            facilities = api.getAllFacilities()

            if not facilities:
                print("No facilities in api {}".format(target_wsdl))
                return

            for facility in facilities:
                lastDownload = db.lastDateDownloaded(facility)

                now = dt.datetime.now()
                toDate = now

                if not lastDownload:
                    fromDate = now - dt.timedelta(days=14)
                elif now - lastDownload < dt.timedelta(hours=1):
                    print("{} already up to date".format(facility))
                    continue
                else:
                    fromDate = lastDownload
                try:
                    meterDataForecast = api.getForecast(facility, fromDate, toDate)
                except MeteologicaApiError as e:
                    print("Silenced exeption: {}".format(e))
                    meterDataForecast = None

                if not meterDataForecast:
                    continue

                # conversion from energy to power
                # (Not necessary for hourly values)
                forecastDict = {facility: meterDataForecast}
                forecastDate = now
                db.addForecast(forecastDict, forecastDate)

    elapsed = time.perf_counter() - start
    print('Total elapsed time {:0.4}'.format(elapsed))


def main():
    args = parseArguments()

    configfile = args.get('--config', 'conf/config.yaml')
    configdb = ns.load(configfile)

    configdb.update(args)

    download_meter_data(configdb)


if __name__ == "__main__":
    print("Starting job")
    main()
    print("Job's Done, Have a Nice Day")

'''
from zeep import Client

from yamlns import namespace as ns

import psycopg2

from psycopg2 import Error
import psycopg2.extras

from datetime import datetime, date, timedelta, timezone
import pytz
import time

import sys

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

def upload_meter_data():
    pass

def forecast():
    configdb = ns.load('conf/config_meteologica.yaml')

    params=dict(
        wsdl=configApi['meteo_test_url'],
        username=configApi['meteo_user'],
        password=configApi['meteo_password'],
        lastDateFile='lastDateFile.yaml',
        showResponses=False,
    )

    timeDelta = configdb['time_delta']

    with MeteologicaApi(params) as api:
        with PlantmonitorDB(configdb) as db:
            facilities = api.getAllFacilities()


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
    forecastDate = addtz(utcnow)
    fromDate = forecastDate
    utcthen = utcnow + timedelta(days=1)
    toDate = addtz(utcthen)

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

                    realFromDate = unixToISOtz(forecastDataDict[0][0])
                    realToDate   = unixToISOtz(forecastDataDict[-1][0])

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
                print("\t{} {:0.4} s".format(facilityItem['facilityName'], elapsed))

    elapsed = time.perf_counter() - start
    print('Total elapsed time {:0.4}'.format(elapsed))

def main():
    args = parseArguments()
    forecast()


if __name__ == "__main__":
    print("Starting job")
    main()
    print("Job's Done, Have a Nice Day")
'''