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
    SensorIrradiation,
    SensorTemperatureAmbient,
    SensorTemperatureModule,
    SensorIrradiationRegistry,
    SensorTemperatureAmbientRegistry,
    SensorTemperatureModuleRegistry,
    HourlySensorIrradiationRegistry,
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
        (t, v)
        for t,v in query.filter( lambda t,v: hourstart <= t and t <= hourend )
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


def integrateMetric(registries, fromDate, toDate):

    # for each hour within fromDate and toDate
    integralMetric = []

    dt = timedelta(hours=1)

    fromHourDate = fromDate.replace(minute=0, second=0, microsecond=0)

    hours = [fromHourDate + i*dt for i in range(math.ceil((toDate - fromHourDate)/dt)) ]

    integralMetric = [(hourstart + dt, integrateHour(hourstart, registries, dt)) for hourstart in hours]

    # should return [(r.time, r.irradiation_w_m2)]
    return integralMetric


# TODO needs a db refactor to relate sensorIrradiation (or arbitrary metric) with the integral of the registries
def getLatestIntegratedTime():
    lastRegistry = HourlySensorIrradiationRegistry.select().order_by(orm.desc(HourlySensorIrradiationRegistry.time)).first()
    if not lastRegistry:
        return None
    return lastRegistry.time


def integrateAllSensors(metrics, fromDate, toDate):
    sensors = orm.select(sensor for sensor in SensorIrradiation)
    registry = SensorIrradiationRegistry

    integratedMetrics = {}

    # Note how we use getattr to generalize which metric we select
    # we need something to ensure the column (aka metric) exists

    for metric in metrics:
        integratedMetrics[metric] = {}
        for sensor in sensors:
            # TODO we're missing the relation between sensorIrradiation and sensorIntegralIrradiation
            sensorLatestIntegratedTime = getLatestIntegratedTime()
            devicesRegistries = orm.select((r.time,  getattr(r, metric))
                for r in registry
                if r.sensor == sensor and fromDate <= r.time and r.time <= toDate
            )
            integratedValues = integrateMetric(devicesRegistries, fromDate, toDate)
            integratedMetrics[metric][sensor] = integratedValues

    return integratedMetrics

# TODO refactor this function so it can be merged with integrateAllSensors above
def integrateExpectedPower(expectedPowerViewQuery, fromDate, toDate):
    sensors = orm.select(sensor for sensor in SensorIrradiation)
    query = Path('queries/new/view_expected_power.sql').read_text(encoding='utf8')
    registry = expectedPowerViewQuery
    metrics = ["expectedpower"]

    integratedMetrics = {}

    # Note how we use getattr to generalize which metric we select
    # we need something to ensure the column (aka metric) exists

    for metric in metrics:
        integratedMetrics[metric] = {}
        for sensor in sensors:
            # TODO change to the latest expected power instead of the getLatestIntegratedTime which is only for Irradiation
            sensorLatestIntegratedTime = getLatestIntegratedTime()
            devicesRegistries = orm.select((r.time,  getattr(r, metric))
                for r in registry
                if r.sensor == sensor and fromDate <= r.time and r.time <= toDate
            )
            integratedValues = integrateMetric(devicesRegistries, fromDate, toDate)
            integratedMetrics[metric][sensor] = integratedValues

    return integratedMetrics

def computeIntegralMetrics():
    #todo only full hours can be integrated or we'll get shit
    # change toDate
    toDate = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    fromDate = None # TODO set to the latest integrated datetime for example.
    integratedMetrics = integrateAllSensors(['irradiation_w_m2'], fromDate, toDate)

    # store metric into database
    # TODO use plant data instead of direct insert?
    if 'irradiation_w_m2' in integratedMetrics:
        [SensorIrradiation.insertIntegratedIrradiationRegistry(irradiation, time) for time, irradiation in integratedMetrics['irradiation_w_m2']]


# def dropNonMonotonicRows(df):
#     ''' strictly monotonic increasing '''
#     anomalies = [r[1] for r in zip(df.index, df.index[1:]) if r[0] >= r[1]]
#     df.drop(index=anomalies, inplace=True)
#     return anomalies