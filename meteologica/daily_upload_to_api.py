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

from meteologica.utils import todt, shiftOneHour

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

    excludedFacilities = ['test_facility']

    responses = {}
    start = time.perf_counter()

    with MeteologicaApi(**params) as api:
        with PlantmonitorDB(configdb) as db:

            facilities = db.getFacilities()
            apifacilities = api.getAllFacilities()

            logger.info('Uploading data from {} facilities in db'.format(len(facilities)))

            if not facilities:
                logger.warning("No facilities in db {} at {}:{}".format(configdb['psql_db'], configdb['psql_host'], configdb['psql_port']))
                return

            for facility in facilities:
                if facility in excludedFacilities:
                    logger.info("Facility {} excluded manually".format(facility))
                    continue
                if facility not in apifacilities:
                    logger.warning("Facility {} in db is not known for the API, skipping.".format(facility))
                    responses[facility] = "INVALID_FACILITY_ID: {}".format(facility)
                    continue

                lastUploadDT = api.lastDateUploaded(facility)
                logger.debug("Facility {} last updated: {}".format(facility, lastUploadDT))
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
                    logger.warning("Missing {} in db meter readings {}".format(facility, meterData))
                    continue

                #FALSE Hour correction, meteologica expects start hour insted of end hour for readings
                #meteologica also expects end hour for readings, same as ERP
                #and in fact uploaded are one hour forward validated and ERP, not one hour behind
                #meterDataShifted = shiftOneHour(meterData)

                # conversion from energy to power
                # (Not necessary for hourly values)
                logger.debug("Uploading {} data: {} ".format(facility, meterData[facility]))
                response = api.uploadProduction(facility, meterData[facility])
                responses[facility] = response

                logger.info("Uploaded {} observations for facility {} : {}".format(len(meterData[facility]), facility, response))

    elapsed = time.perf_counter() - start
    logger.info('Total elapsed time {:0.4}'.format(elapsed))

    return responses


def main():
    args = parseArguments()

    configfile = args.get('--config', 'conf/config_meteologica.yaml')
    configdb = ns.load(configfile)

    configdb.update(args)

    upload_meter_data(configdb)


if __name__ == "__main__":
    logger.info("Starting job")
    main()
    logger.info("Job's Done, Have a Nice Day")
