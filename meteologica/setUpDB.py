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
import math
import sys
import random


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

# def normSamples():
#     [(f'2020-01-01 {h}:00:00', round(1*(1 + math.cos(math.pi*2*h/24. - math.pi))/2*math.pi, 2)) for h in range(24)


def normValue(hour, maxValue=100, randomizeValues=True):
    normApprox = (1 + math.cos(math.pi*2*hour/24. - math.pi))/2.
    if randomizeValues:
        randomValue = maxValue/2*random.random()
        return round(randomValue + maxValue/2.*normApprox, 2)
    else:
        return round(maxValue*normApprox, 2)


def normReadings(startdt=dt.datetime(2020, 4, 1, 0),
                 enddt=dt.datetime(2020, 5, 2, 0),
                 maxValue=100,
                 randomizeValues=True,
                 ):

    return [
        (dt, normValue(dt.hour, maxValue, randomizeValues))
        for dt in datetime_range(startdt, enddt, dt.timedelta(hours=1))
    ]


def parseArguments():
    # TODO parse arguments into a ns
    args = ns()
    if len(sys.argv) == 3:
        args[sys.argv[1]] = sys.argv[2]
        return args
    else:
        return args


def main():
    args = parseArguments()

    configfile = args.get('--config', 'conf/config_meteologica.yaml')
    configdb = ns.load(configfile)

    configdb.update(args)

    PlantmonitorDB(configdb).demoDBsetup(configdb)

    with PlantmonitorDB(configdb) as db:
        alcolea = "SomEnergia_Alcolea"
        fontivsolar = "SomEnergia_Fontivsolar"
        perpinya = "SomEnergia_Perpinya"

        db.addFacilityMeterRelation(alcolea, '123401234')
        rows = {
            alcolea: normReadings(
                dt.datetime(2020, 4, 1, 0),
                dt.datetime(2020, 6, 1, 0)
            )
        }
        db.addMeterData(rows)
        print('Inserted {} entries for {}'.format(len(rows[alcolea]),alcolea))

        db.addFacilityMeterRelation(fontivsolar, '567805678')
        rows = {
            fontivsolar: normReadings(
                dt.datetime(2020, 3, 20, 0),
                dt.datetime(2020, 4, 26, 0)
            )
        }
        db.addMeterData(rows)
        print('Inserted {} entries for {}'.format(len(rows[fontivsolar]),fontivsolar))

        db.addFacilityMeterRelation(perpinya, '909009090')
        rows = {
            perpinya: normReadings(
                dt.datetime(2020, 4, 11, 0),
                dt.datetime(2020, 5, 27, 0)
            )
        }
        db.addMeterData(rows)
        print('Inserted {} entries for {}'.format(len(rows[perpinya]),perpinya))


if __name__ == "__main__":
    print("Starting job")
    main()
    print("Job's Done, Have a Nice Day")
