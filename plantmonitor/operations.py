#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from scipy import integrate
from datetime import datetime, timedelta, timezone

import math
from pathlib import Path

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

def integrateHourFromTimeseries(hourstart, timeseries, dt=timedelta(hours=1)):

    # for each hour within fromDate and toDate

    hourend = hourstart + dt

    # slice
    timeSeriesSlice = [
        (t, v)
        for t,v in timeseries if hourstart <= t and t <= hourend
    ]

    if not timeSeriesSlice:
        # logger.warning("No values in hour range")
        return None

    xvalues, yvalues = list(zip(*timeSeriesSlice))

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

# TODO refactor: establish the time range further down the line to simplify the code

# TODO test that returns None when there are no rows
def getNewestTime(sensor, registry, metric):
    return orm.select(r.time for r in registry if r.sensor == sensor and getattr(r, metric)).order_by(orm.desc(1)).first()

def getOldestTime(sensor, registry, metric):
    return orm.select(r.time for r in registry if r.sensor == sensor and getattr(r, metric)).order_by(1).first()

def getTimeRange(sensor, srcregistry, dstregistry, srcCol, dstCol, fromDate=None, toDate=None):
    if not fromDate:
        fromDate = getNewestTime(sensor, dstregistry, dstCol)
        if not fromDate:
            fromDate = getOldestTime(sensor, srcregistry, srcCol)

    if not toDate:
        toDate = getNewestTime(sensor, srcregistry, srcCol)

    if not fromDate:
        if not srcregistry.select().count():
            logger.warning("{} has no registries".format(srcregistry))
        else:
            logger.warning(
                "fromDate is {} for sensor {} because {} of {} has {} non-null registries".format(
                    fromDate, sensor.to_dict(), srcregistry, srcCol,
                    orm.count(r for r in srcregistry if r.sensor == sensor and getattr(r, srcCol))
                )
            )
            # TODO raise?

    if not toDate:
        logger.warning("toDate is {} for sensor {}".format(toDate, sensor))

    return fromDate, toDate

# TODO refactor this function so it can be merged with integrateAllSensors above
def integrateExpectedPower(fromDate=None, toDate=None):

    sensors = orm.select(sensor for sensor in SensorIrradiation)

    expectedPowerViewQuery = Path('queries/new/view_expected_power.sql').read_text(encoding='utf8')
    srcCol = 'expectedpower'
    dstCol = 'expected_energy_wh'

    expectedPowerRegs = database.select(expectedPowerViewQuery)

    if not expectedPowerRegs:
        logger.warning("No expectedPower registries. expected energy will not be computed.")
        return None

    integratedMetric = {}

    # TODO use fromDate. toDate
    for sensor in sensors:
        metricFromDate = getOldestTime(sensor, HourlySensorIrradiationRegistry, dstCol)

        sensorExpectedPower = [(r['time'],r[srcCol] )for r in expectedPowerRegs if r.sensor == sensor.name]
        if not sensorExpectedPower:
            logger.warning("No expectedPower for sensor {}. Skipping".format(sensor.name))
        # TODO assuming ordered by time (it's in the query)
        metricToDate = sensorExpectedPower[-1]
        if not metricFromDate:
            metricFromDate = sensorExpectedPower[0]

        dt = timedelta(hours=1)
        fromHourDate = fromDate.replace(minute=0, second=0, microsecond=0)
        hours = [fromHourDate + i*dt for i in range(math.ceil((toDate - fromHourDate)/dt)) ]

        # TODO sensorExpectedPower is a list, not a pony query, we have to rewrite evreything

        integratedMetric[sensor] = [(hourstart + dt, integrateHourFromTimeseries(hourstart, sensorExpectedPower, dt)) for hourstart in hours]

    return integratedMetric

# {sensor[i] : [(time, value)] }

def integrateIrradiance(fromDate=None, toDate=None):

    sensors = orm.select(sensor for sensor in SensorIrradiation)
    srcregistry = SensorIrradiationRegistry
    dstregistry = HourlySensorIrradiationRegistry

    srcCol = 'irradiation_w_m2'
    dstCol = 'integratedIrradiation_wh_m2'

    integratedMetric = {}

    for sensor in sensors:
        metricFromDate, metricToDate = getTimeRange(sensor, srcregistry, dstregistry, srcCol, dstCol, fromDate, toDate)
        if not metricFromDate:
            logger.warning("sensor {} {} has no valid registries. skipping.".format(sensor.to_dict(), srcCol))
            continue
        devicesRegistries = orm.select(
            (r.time, getattr(r, srcCol))
            for r in srcregistry
            if r.sensor == sensor and metricFromDate <= r.time and r.time <= metricToDate
        )
        integratedMetric[sensor] = integrateMetric(devicesRegistries, metricFromDate, metricToDate)

    return integratedMetric

def insertHourlySensorIrradiationMetrics(integratedMetrics, columnName):
    # update registry column or create if it doesn't exist
    for sensor, timeseries in integratedMetrics.items():
            for time, value in timeseries:
                reg = HourlySensorIrradiationRegistry.get(sensor=sensor, time=time)
                if not reg:
                    sensor.insertHourlySensorIrradiationMetric(time=time, **{columnName : value}
                    )
                else:
                    reg.set(**{columnName:value})

def computeIntegralMetrics():
    irradiance = integrateIrradiance()
    insertHourlySensorIrradiationMetrics(irradiance, 'integratedIrradiation_wh_m2')
    orm.flush()
    expectedEnergy = integrateExpectedPower()
    insertHourlySensorIrradiationMetrics(expectedEnergy, 'expected_energy_wh')


# def dropNonMonotonicRows(df):
#     ''' strictly monotonic increasing '''
#     anomalies = [r[1] for r in zip(df.index, df.index[1:]) if r[0] >= r[1]]
#     df.drop(index=anomalies, inplace=True)
#     return anomalies