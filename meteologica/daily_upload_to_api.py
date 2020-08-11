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
    MeteologicaFacilityIDError,
)

from meteologica.utils import todt

import time
import logging
import sys

logger = logging.getLogger(__name__)

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

    responses = {}
    start = time.perf_counter()

    with MeteologicaApi(**params) as api:
        with PlantmonitorDB(configdb) as db:

            facilities = db.getFacilities()
            apifacilities = api.getAllFacilities()

            if not facilities:
                print("No facilities in db {} at {}:{}".format(configdb['psql_db'], configdb['psql_host'], configdb['psql_port']))
                return

            for facility in facilities:
                if facility not in apifacilities:
                    logger.warning("Facility {} in db is not known for the API, skipping.".format(facility))
                    responses[facility] = "INVALID_FACILITY_ID: {}".format(facility)
                    continue

                lastUploadDT = api.lastDateUploaded(facility)
                print(lastUploadDT)
                meterData = {}
                if not lastUploadDT:
                    meterData = db.getMeterData(facility)
                else:
                    toDate = dt.datetime.now()
                    fromDate = lastUploadDT
                    meterData = db.getMeterData(facility, fromDate, toDate)

                if not meterData:
                    logger.warning("No meter readings for facility {} since {}".format(facility, lastUploadDT))
                # if not meterData[facility]:
                if facility not in meterData:
                    logger.warning("Missing {} in meterData {}".format(facility, meterData))
                    continue

                # conversion from energy to power
                # (Not necessary for hourly values)
                response = api.uploadProduction(facility, meterData[facility])
                responses[facility] = response

    elapsed = time.perf_counter() - start
    print('Total elapsed time {:0.4}'.format(elapsed))

    return responses


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
