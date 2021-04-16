#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from scipy import integrate
from datetime import datetime, timedelta, timezone

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

# deprecated
def getRegistryQuery(registry, deviceName, metric, fromDate, toDate):
    devicesRegistries = orm.select(r[metric] for r in registry if fromDate <= r.time and r.time <= toDate)

def integrateHour(hourstart, query, dt=timedelta(hours=1)):

    # for each hour within fromDate and toDate

    hourend = hourstart + dt

    # slice
    timeSeries = [
        # (r.time, r.irradiation_w_m2)
        r
        for r in query.filter( lambda r: hourstart <= r.time and r.time <= hourend )
    ]

    if not timeSeries:
        # logger.warning("No values in hour range")
        return None

    xvalues, yvalues = list(zip(*timeSeries))

    # hourly_trapezoidal_approximation
    integralMetricValueDateTime = integrate.trapz(y=yvalues, x=xvalues)
    # trapz returns in x-axis type, so we need to convert the unreal datetime to the metric value
    integralMetricValue = integralMetricValueDateTime.days * 24 + integralMetricValueDateTime.seconds // 3600

    return integralMetricValue


# deprecated
def integrateSensor(sensorName, metricName, fromDate, toDate):

    metricModel = SensorIrradiationRegistry

    integralMetric = []

    dt = timedelta(hours=1)

    fromHourDate = fromDate.replace(minute=0, second=0, microsecond=0)

    hours = [fromHourDate + i*dt for i in range(math.ceil((toDate - fromHourDate)/dt)) ]

    irradiation = [(hourstart + dt, integrateHour(hourstart, metricModel, dt)) for hourstart in hours]

    return irradiation

def integrateMetric(registries, fromDate, toDate):

    # for each hour within fromDate and toDate
    integralMetric = []

    dt = timedelta(hours=1)

    fromHourDate = fromDate.replace(minute=0, second=0, microsecond=0)

    hours = [fromHourDate + i*dt for i in range(math.ceil((toDate - fromHourDate)/dt)) ]

    integralMetric = [(hourstart + dt, integrateHour(hourstart, registries, dt)) for hourstart in hours]

    # should return [(r.time, r.irradiation_w_m2)]
    return integralMetric

def integrateAllSensors(metrics, fromDate, toDate):
    sensors = orm.select(sensor for sensor in SensorIrradiation)
    registry = SensorIrradiationRegistry

    integratedMetrics = {}

    for metric in metrics:
        for sensor in sensors:
            devicesRegistries = orm.select(r[metric]
                for r in registry
                if r.sensor == sensor and fromDate <= r.time and r.time <= toDate
            )
            integratedValues = integrateMetric(devicesRegistries)
            integratedMetrics[metric][sensor] = integratedValues

    return integratedMetrics
    # insert

def computeIntegralMetrics():
    #todo only full hours can be integrated or we'll get shit
    # change toDate
    toDate = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    fromDate = None # TODO set to the latest integrated datetime for example.
    integrateAllSensors(['irradiation_w_m2', fromDate, toDate])


# def dropNonMonotonicRows(df):
#     ''' strictly monotonic increasing '''
#     anomalies = [r[1] for r in zip(df.index, df.index[1:]) if r[0] >= r[1]]
#     df.drop(index=anomalies, inplace=True)
#     return anomalies