#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from scipy import integrate, interpolate
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

def frontierClosestRead(hourstart, query, dt=timedelta(hours=1)):

    hourend = hourstart + dt

    dt10min = timedelta(minutes=10)

    preRead = query.filter(lambda t, v: hourstart - dt10min <= t and t <= hourstart).order_by(orm.desc(lambda t, v: t)).first()
    postRead = query.filter(lambda t, v: hourend <= t and t <= hourend + dt10min).first()

    preRead = preRead if preRead and preRead[0] < hourstart else None
    postRead = postRead if postRead and hourend < postRead[0] else None

    logger.debug("pre-post {} _ {} _ {}".format(
            preRead[0].isoformat() if preRead else None,
            hourstart,
            postRead[0].isoformat() if postRead else None
        )
    )

    return (preRead, postRead)

def interpolateRead(firstPoint, wantedTime, secondPoint):

    t1, y1 = firstPoint
    t2, y2 = secondPoint

    interp = interpolate.interp1d([t1.timestamp(), t2.timestamp()], [y1, y2])
    logger.debug("inter ({},{}) - ({},{}) - ({},{}) ".format(
        t1.isoformat(), y1,
        wantedTime.isoformat(), interp(wantedTime.timestamp()),
        t2.isoformat(), y2))
    value = float(interp(wantedTime.timestamp()))

    return (wantedTime, value)

def prepareSeriesSlice(hourstart, query, dt):

    # TODO agafar el darrer valor abans de l'hora i el primer després de l'hora
    # si aquesta està dins dels 10 minuts

    hourend = hourstart + dt

    # slice
    timeSeriesSlice = sorted([
        (t, v)
        for t,v in query.filter( lambda t,v: hourstart <= t and t <= hourend )
    ])

    if not timeSeriesSlice or len(timeSeriesSlice) <= 1:
        # logger.warning("No values in hour range")
        return None

    preRead, postRead = frontierClosestRead(hourstart, query, dt)

    if preRead:
        firstread = interpolateRead(preRead, hourstart, timeSeriesSlice[0])
        timeSeriesSlice.insert(0, firstread)

    if postRead:
        lastread = interpolateRead(timeSeriesSlice[-1], hourend, postRead)
        timeSeriesSlice.append(lastread)

    return timeSeriesSlice

def integrateHour(hourstart, query, dt=timedelta(hours=1)):

    timeSeriesSlice = prepareSeriesSlice(hourstart, query, dt)

    if not timeSeriesSlice:
        return None

    xvalues, yvalues = list(zip(*timeSeriesSlice))

    # if yvalues.count(None) >= len(yvalues)-1:
    #     logger.warning("Not enough values in hour range {} - {}: {}".format(xvalues[0], xvalues[-1], yvalues))
    #     return None

    if None in yvalues:
        logger.warning("None values in hour range {} - {}. Skipping.".format(xvalues[0], xvalues[-1]))
        return None

    # hourly_trapezoidal_approximation
    integralMetricValueDateTime = integrate.trapz(y=yvalues, x=xvalues)
    # trapz returns in x-axis type, so we need to convert the unreal datetime to the metric value
    # round to integer with //
    integralMetricValue = integralMetricValueDateTime.days * 24 + integralMetricValueDateTime.seconds // 3600
    logger.debug("range {}- {}\nxvalues {}\nyvalues {}\nresult {}".format(hourstart, hourstart + dt, xvalues,yvalues,integralMetricValue))
    return integralMetricValue

def prepareSeriesSliceFromTimeseries(hourstart, timeseries, dt):

    # TODO agafar el darrer valor abans de l'hora i el primer després de l'hora
    # si aquesta està dins dels 10 minuts

    hourend = hourstart + dt

    # slice
    timeSeriesSlice = sorted([
        (t, v)
        for t,v in timeseries if hourstart <= t and t <= hourend
    ])

    if not timeSeriesSlice or len(timeSeriesSlice) <= 1:
        return None

    dt10min = timedelta(minutes=10)

    preReads = sorted([
        (t, v)
        for t,v in timeseries if hourstart - dt10min <= t and t <= hourstart
    ])

    if preReads:
        preRead = preReads[-1]
        firstread = interpolateRead(preRead, hourstart, timeSeriesSlice[0])
        if firstread:
            timeSeriesSlice.insert(0, firstread)

    postReads = sorted([
        (t, v)
        for t,v in timeseries if hourend <= t and t <= hourend + dt10min
    ])

    if postReads:
        postRead = postReads[0]
        lastread = interpolateRead(timeSeriesSlice[-1], hourend, postRead)
        if lastread:
            timeSeriesSlice.append(lastread)

    return timeSeriesSlice

def integrateHourFromTimeseries(hourstart, timeseries, dt=timedelta(hours=1)):

    # for each hour within fromDate and toDate

    hourend = hourstart + dt

    timeSeriesSlice = prepareSeriesSliceFromTimeseries(hourstart, timeseries, dt)

    if not timeSeriesSlice:
        return None

    xvalues, yvalues = list(zip(*timeSeriesSlice))

    # if yvalues.count(None) >= len(yvalues)-1:
    #     logger.warning("Not enough values in hour range {} - {}: {}".format(xvalues[0], xvalues[-1], yvalues))
    #     return None

    # TODO add support for handling a few None values in hour range
    if None in yvalues:
        logger.warning("None values in hour range {} - {}. Skipping.".format(xvalues[0], xvalues[-1]))
        return None

    # hourly_trapezoidal_approximation
    integralMetricValueDateTime = integrate.trapz(y=yvalues, x=xvalues)
    # trapz returns in x-axis type, so we need to convert the unreal datetime to the metric value
    integralMetricValue = integralMetricValueDateTime.days * 24 + integralMetricValueDateTime.seconds // 3600

    # TODO expectedPower is in kWh! we have to multiply by 1000. Remove as soon as expectedpower is in wh
    return 1000*integralMetricValue

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

    expectedPowerViewQuery = Path('queries/view_expected_power.sql').read_text(encoding='utf8')
    srcCol = 'expectedpower'
    dstCol = 'expected_energy_wh'

    expectedPowerRegs = database.select(expectedPowerViewQuery)

    if not expectedPowerRegs:
        logger.warning("No expectedPower registries. expected energy will not be computed.")
        return {}

    if all(getattr(r, srcCol) is None for r in expectedPowerRegs):
        logger.warning("All expectedPower readings are None. expected energy will not be computed.")
        return {}


    integratedMetric = {}

    # TODO use fromDate. toDate
    for sensor in sensors:
        sensorExpectedPower = [(r.time, getattr(r,srcCol) ) for r in expectedPowerRegs if r.sensor == sensor.id]
        if not sensorExpectedPower:
            logger.warning("No expectedPower for sensor {}. Skipping".format(sensor.name))
            continue

        # TODO assuming ordered by time (it's in the query)
        metricToDate = sensorExpectedPower[-1][0] or datetime.now(timezone.utc)
        metricFromDate = getOldestTime(sensor, HourlySensorIrradiationRegistry, dstCol) or sensorExpectedPower[0][0]

        # apply from/to filter
        metricFromDate = fromDate if fromDate and metricFromDate < fromDate else metricFromDate
        metricToDate   = toDate if toDate and toDate < metricToDate else metricToDate

        dt = timedelta(hours=1)
        fromHourDate = metricFromDate.replace(minute=0, second=0, microsecond=0)
        hours = [fromHourDate + i*dt for i in range(math.ceil((metricToDate - fromHourDate)/dt)) ]

        # TODO sensorExpectedPower is a list, not a pony query, we have to rewrite everything
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
        # Round to nearest hour to avoid computing uncompleted hours
        metricToDate = metricToDate.replace(minute=0, second=0, microsecond=0)
        devicesRegistries = orm.select(
            (r.time, getattr(r, srcCol))
            for r in srcregistry
            if r.sensor == sensor and metricFromDate <= r.time and r.time <= metricToDate
        )
        if metricFromDate.replace(minute=0, second=0, microsecond=0) == metricToDate:
            logger.info("Sensor {} already up to date. Skipping.".format(sensor))
        else:
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
