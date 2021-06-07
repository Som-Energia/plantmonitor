#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from yamlns import namespace as ns
import datetime as dt

from pony import orm

import datetime

from ORM.models import database
from ORM.models import (
    Plant,
    Meter,
    MeterRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)
from plantmonitor.storage import PonyMetricStorage, ApiMetricStorage

from ORM.orm_util import setupDatabase, getTablesToTimescale, timescaleTables

from meteologica.meteologica_api_utils import (
    MeteologicaApi,
    MeteologicaApiError,
)

from meteologica.utils import todt, shiftOneHour, shiftOneHourWithoutFacility

import time
import sys

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")


def parseArguments():
    # TODO parse arguments into a ns
    args = ns()

    if len(sys.argv) == 3:
        args[sys.argv[1]] = sys.argv[2]

    return args

class ForecastStatus():
    UNKNOWNFACILITY = "Unknown facility"
    UPTODATE = "UPTODATE"
    OK = "OK"

# TODO use enum on status, or change to exception

def getForecast(api, facility):
    plant = Plant.getFromMeteologica(facility)

    if not plant:
        logger.error("meteologica facility {} is not known to database. Please add it.".format(facility))
        return None, ForecastStatus.UNKNOWNFACILITY

    lastDownload = plant.lastForecastDownloaded()

    now = dt.datetime.now(dt.timezone.utc)

    if not lastDownload:
        fromDate = now - dt.timedelta(days=14)
    elif now - lastDownload < dt.timedelta(hours=1):
        logger.info("{} already up to date".format(facility))
        return None, ForecastStatus.UPTODATE
    else:
        fromDate = lastDownload

    toDate = now + dt.timedelta(days=14)

    try:
        meterDataForecast = api.getForecast(facility, fromDate, toDate)
        status = ForecastStatus.OK
    except MeteologicaApiError as e:
        logger.warning("Silenced exception: {}".format(e))
        status = str(e)
        meterDataForecast = None

    if not meterDataForecast:
        logger.info("No forecast data for {}".format(facility))
        return None, status

    return meterDataForecast, status

def addForecast(facility, meterDataForecast, status):

    deltat = datetime.timedelta(hours=1)

    # TODO api uses start-hour, we should change to end-hour before inserting
    forecasts = [(time + deltat, int(1000*forecast)) for time, forecast in meterDataForecast]

    # conversion from energy to power
    # (Not necessary for hourly values)

    forecastDate = datetime.datetime.now(datetime.timezone.utc)

    plant = Plant.get(codename=facility)
    forecastMetadata = ForecastMetadata.create(plant=plant, forecastdate=forecastDate, errorcode=status)
    if status == ForecastStatus.OK:
        forecastMetadata.addForecasts(forecasts)

def downloadMeterForecasts(configdb, test_env=True):

    if test_env:
        target_wsdl = configdb['meteo_test_url']
        lastDateFile = 'lastDateFile-test.yaml'
    else:
        target_wsdl = configdb['meteo_url']
        lastDateFile = 'lastDateFile.yaml'


    params = dict(
        wsdl=target_wsdl,
        username=configdb['meteo_user'],
        password=configdb['meteo_password'],
        lastDateFile='lastDateFile.yaml',
        lastDateDownloadFile='lastDateDownloadFile.yaml',
        showResponses=False,
    )

    start = time.perf_counter()

    statuses = {}

    with MeteologicaApi(**params) as api:
        facilities = api.getAllFacilities()

        if not facilities:
            logger.info("No facilities in api {}".format(target_wsdl))
            return {}

        for facility in facilities:
            forecast, status = getForecast(api, facility)
            statuses[facility] = status

            if status == ForecastStatus.UPTODATE or status == ForecastStatus.UNKNOWNFACILITY:
                logger.info("Forecast for {} resulted in {}. Skipping.".format(facility, status))
            else:

                with orm.db_session:
                    addForecast(facility, forecast, status)

                logger.info(
                    "Saved {} forecast records from {} to db - {} ".format
                    (
                        len(forecast), facility, status
                    )
                )

    elapsed = time.perf_counter() - start
    logger.info('Total elapsed time {:0.4}'.format(elapsed))

    return statuses

def getMeterReadings(facility, fromDate=None, toDate=None):
    plant = Plant.get(codename=facility)
    if not plant:
        logger.warning("Plant codename {} is unknown to db".format(facility))
        return None
    meter = plant.getMeter()
    if not meter:
        logger.error("Plant {} doesn't have any meter".format(plant.name))
        return None

    query = orm.select(r for r in MeterRegistry if r.meter == meter)
    if fromDate:
        query = query.filter(lambda r: fromDate <= r.time)
    if toDate:
        query = query.filter(lambda r: r.time <= toDate)

    data = [(r.time, r.export_energy_wh) for r in query]

    return data

def getMeterReadingsFromLastUpload(api, facility):
    # TODO use forecastMetadata instead of api lastDateUploaded!
    # Fixes the TODO below and can remote the undo (I guess)
    lastUploadDT = api.lastDateUploaded(facility)
    logger.debug("Facility {} last updated: {}".format(facility, lastUploadDT))
    meterReadings = []
    if not lastUploadDT:
        meterReadings = getMeterReadings(facility)
    else:
        # TODO refactor this undo the hour shift due to api understanding start-hours instead of end-hours (see below @101)
        fromDate = lastUploadDT + dt.timedelta(hours=1)
        meterReadings = getMeterReadings(facility=facility, fromDate=fromDate)

    if not meterReadings:
        logger.warning("No meter readings for facility {} since {}".format(facility, lastUploadDT))

    return meterReadings

def uploadFacilityMeterReadings(api, facility):
    meterReadings = getMeterReadingsFromLastUpload(api, facility)

    if not meterReadings:
        return None

    #Seems to be correct according to documentation (meteologica said the contrary):
    #Hour correction, meteologica expects start hour insted of end hour for readings
    #ERP has end hour. Verified with validated readings. Assumes horari type.
    #Uploaded are one hour behind validated
    meterDataShifted = shiftOneHourWithoutFacility(meterReadings)

    # conversion from energy to power
    # (Not necessary for hourly values)
    logger.debug("Uploading {} data: {} ".format(facility, meterDataShifted))

    response = api.uploadProduction(facility, meterDataShifted)

    logger.info("Uploaded {} observations for facility {} : {}".format(len(meterDataShifted), facility, response))

    return response

def uploadMeterReadings(configdb, test_env=True):

    if test_env:
        target_wsdl = configdb['meteo_test_url']
        lastDateFile = 'lastDateFile-test.yaml'
    else:
        target_wsdl = configdb['meteo_url']
        lastDateFile = 'lastDateFile.yaml'

    params = dict(
        wsdl=target_wsdl,
        username=configdb['meteo_user'],
        password=configdb['meteo_password'],
        lastDateFile=lastDateFile,
        showResponses=False,
    )

    excludedFacilities = ['test_facility']

    responses = {}
    start = time.perf_counter()

    with MeteologicaApi(**params) as api:
        with orm.db_session:
            apifacilities = api.getAllFacilities()

            facilities = [plant.codename for plant in Plant.select()]

            logger.info('Uploading data from {} facilities in db'.format(len(facilities)))

            if not facilities:
                logger.warning("No facilities in db")
                return

            for facility in facilities:
                if facility in excludedFacilities:
                    logger.info("Facility {} excluded manually".format(facility))
                    continue
                if facility not in apifacilities:
                    logger.warning("Facility {} in db is not known for the API, skipping.".format(facility))
                    responses[facility] = "INVALID_FACILITY_ID: {}".format(facility)
                    continue

                responses[facility] = uploadFacilityMeterReadings(api, facility)

    elapsed = time.perf_counter() - start
    logger.info('Total elapsed time {:0.4}'.format(elapsed))

    return responses

def main():
    args = parseArguments()

    configfile = args.get('--config', 'conf/config.yaml')
    configdb = ns.load(configfile)

    configdb.update(args)

    downloadMeterForecasts(configdb)


if __name__ == "__main__":
    logger.info("Starting job")
    main()
    logger.info("Job's Done, Have a Nice Day")

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
