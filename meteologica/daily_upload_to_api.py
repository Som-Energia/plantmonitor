#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from yamlns import namespace as ns
import datetime as dt

import sys

def parseArguments():
    args = ns()
    if len(sys.argv) == 3:
        return args
    else:
        return args

def upload_meter_data():
    
    configdb = ns.load('conf/config_meteologica.yaml')

    params=dict(
        wsdl=configApi['meteo_test_url'],
        username=configApi['meteo_user'],
        password=configApi['meteo_password'],
        lastDateFile='lastDateFile.yaml',
        showResponses=False,
    )

    with MeteologicaApi(params) as api:
        with PlantmonitorDB(configdb) as db:


            facilities = db.getFacilities()

            for facility in facilities:
                lastUpload = api.lastDateUploaded(facility)

                meterData = {}
                if not lastUpload:
                    meterData = db.getMeterData(facility)
                else
                    toDate = dt.datetime.now()
                    fromDate = lastUpload
                    meterData = db.getMeterData(facility, fromDate, toDate)

                if not meterData[facility]:
                    continue

                #conversion from energy to power
                #(Not necessary for hourly values)

                api.uploadProduction(facility, meterData[facility])


    start = time.perf_counter()

    elapsed = time.perf_counter() - start
    print(f'Total elapsed time {elapsed:0.4}')

def main():
    args = parseArguments()
    upload_meter_data()

if __name__ == "__main__":
    print("Starting job")
    main()
    print("Job's Done, Have a Nice Day")
