import datetime
from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

def meter_connection_protocol(c, meters):
    Meter = c.model('giscedata.registrador')
    meter_ids = Meter.search([
        ('name', 'in', meters),
    ])
    return {
        meter['name']: meter['tm_connection']
        for meter in Meter.read(meter_ids, [
            'name',
            'tm_connection',
        ])
    }

def telemeasure_meter_names(c):
    Meter = c.model('giscedata.registrador')
    meter_ids = Meter.search([
        ('technology', '=', 'telemeasure'),
        ])
    meters = Meter.read(meter_ids, ['name'])
    return [meter['name'] for meter in meters]

def measures_from_date(c, meter, beyond, upto):
    measure_ids = c.TmProfile.search([
        ('name','=', meter),
        ('type','=','p'),
        ('utc_timestamp','<', upto),
        ]+([
            ('utc_timestamp','>', beyond)
        ] if beyond else []),
        ) # TODO: Manage duplicated datetimes
    if not measure_ids:
        logger.debug("erp {} returned 0 measures with {} {} {}".format(c, meter, upto, beyond))
        return []
    else:
        logger.debug("erp {} returned {} measures with {} {} {}".format(c, len(measure_ids), meter, upto, beyond))
    measure_ids = [int(m) for m in measure_ids]
    measures = c.TmProfile.read(measure_ids,[
        'r1',
        'r2',
        'r3',
        'r4',
        'ai',
        'ae',
        'utc_timestamp',
        ])
    # TODO: Workaround to remove dupped dates
    measures = {
        measure['utc_timestamp'] : measure
        for measure in measures
    }.values()
    return [(
            measure['utc_timestamp'],
            int(measure['ae']),
            int(measure['ai']),
            int(measure['r1']),
            int(measure['r2']),
            int(measure['r3']),
            int(measure['r4']),
            )
        for measure in sorted(measures,
            key=lambda x: x['utc_timestamp'])
        ]

def rfc3336toiso(isodate):
    dt = datetime.datetime.strptime(
        isodate, "%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def update_meters_protocols(c, meters):
    from ORM.models import Meter

    meters_protocols = meter_connection_protocol(meters, c)
    Meter.updateMeterProtocol(meters_protocols)

# vim: et sw=4 ts=4
