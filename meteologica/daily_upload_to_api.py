#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from yamlns import namespace as ns
import datetime as dt

from plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBError,
    todt,
)

from meteologica_api_utils import (
    MeteologicaApi,
    MeteologicaApiError,
)

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


def upload_meter_data(configdb, test_env=True):

    if test_env:
        target_wsdl = configdb['meteo_test_url']
    else:
        target_wsdl = configdb['meteo_url']

    params = dict(
        wsdl=target_wsdl,
        username=configdb['meteo_user'],
        password=configdb['meteo_password'],
        lastDateFile='lastDateFile.yaml',
        showResponses=False,
    )

    start = time.perf_counter()

    with MeteologicaApi(**params) as api:
        with PlantmonitorDB(configdb) as db:

            facilities = db.getFacilities()

            if not facilities:
                print(f"No facilities in db {configdb['psql_db']} at {configdb['psql_host']}:{configdb['psql_port']}")
                return

            for facility in facilities:
                lastUpload = api.lastDateUploaded(facility)
                lastUploadDT = todt(lastUpload)

                meterData = {}
                if not lastUpload:
                    meterData = db.getMeterData(facility)
                else:
                    toDate = dt.datetime.now()
                    fromDate = lastUploadDT
                    meterData = db.getMeterData(facility, fromDate, toDate)

                if not meterData[facility]:
                    continue

                # conversion from energy to power
                # (Not necessary for hourly values)

                api.uploadProduction(facility, meterData[facility])

    elapsed = time.perf_counter() - start
    print(f'Total elapsed time {elapsed:0.4}')


def main():
    args = parseArguments()

    configfile = args.get('--config', 'conf/config_meteologica.yaml')
    configdb = ns.load(configfile)

    configdb.update(args)

    upload_meter_data(configdb)


if __name__ == "__main__":
    print("Starting job")
    main()
    print("Job's Done, Have a Nice Day")
