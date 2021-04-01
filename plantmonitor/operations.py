#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from scipy import integrate
from datetime import datetime, timedelta

import math

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

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

def integrateHour(hourstart, dt=timedelta(hours=1)):

    # for each hour within fromDate and toDate

    hourend = hourstart + dt

    # slice
    irradiationTimeSeries = [
        (r.time, r.irradiation_w_m2)
        for r in SensorIrradiationRegistry.select( lambda r: hourstart <= r.time and r.time <= hourend )
    ]

    if not irradiationTimeSeries:
        # logger.warning("No values in hour range")
        return None

    xvalues, yvalues = list(zip(*irradiationTimeSeries))

    # hourly_trapezoidal_approximation
    integralMetricValueDT = integrate.trapz(y=yvalues, x=xvalues)
    integralMetricValue = integralMetricValueDT.days * 24 + integralMetricValueDT.seconds // 3600

    return integralMetricValue


def integrateSensor(sensorName, metricName, fromDate, toDate):

    integralMetric = []

    dt = timedelta(hours=1)

    fromHourDate = fromDate.replace(minute=0, second=0, microsecond=0)

    hours = [fromHourDate + i*dt for i in range(math.ceil((toDate - fromHourDate)/dt)) ]

    irradiation = [(hourstart + dt, integrateHour(hourstart, dt)) for hourstart in hours]

    # insert

    return irradiation


# def dropNonMonotonicRows(df):
#     ''' strictly monotonic increasing '''
#     anomalies = [r[1] for r in zip(df.index, df.index[1:]) if r[0] >= r[1]]
#     df.drop(index=anomalies, inplace=True)
#     return anomalies