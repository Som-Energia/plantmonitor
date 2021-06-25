from influxdb import InfluxDBClient as client_db
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

def _upload_meter_measures(measures):
    plant = ProductionPlant()
    if not plant.load('conf/modmap.yaml','Alcolea'):
        logger.error('Error loadinf yaml definition file...')
        sys.exit(-1)
    flux_client = client_db(plant.db)

def publish_influx(flux_client,metrics):
    flux_client.write_points([metrics])
    logger.info("[INFO] Sent to InfluxDB")

def upload_measures(flux_client, meter, measures):
    flux_client.write_points([
        dict(
            measurement = 'sistema_contador',
            tags = dict(
                name=meter,
            ),
            fields = dict(
                export_energy = ae,
                import_energy = ai,
                r1 = r1,
                r2 = r2,
                r3 = r3,
                r4 = r4,
            ),
            time = isodatetime,
        )
        for isodatetime, ae, ai, r1, r2, r3, r4 in measures
    ])

def rfc3336toiso(isodate):
    dt = datetime.datetime.strptime(
        isodate, "%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def uploaded_plantmonitor_measures(flux_client, meter):
    result = flux_client.query("""\
        SELECT
            "time",
            "export_energy",
            "import_energy",
            "r1","r2","r3","r4"
        FROM "sistema_contador"
        WHERE "name"=$meter
        ORDER BY "time"
    """, bind_params=dict(meter=meter))
    result = [(
        rfc3336toiso(v['time']),
        v['export_energy'],
        v['import_energy'],
        v['r1'],
        v['r2'],
        v['r3'],
        v['r4'],)
        for v in result.get_points()
        ]
    return result

def last_uploaded_plantmonitor_measures(flux_client, meter):
    result = flux_client.query("""\
        SELECT
            "time",
            "export_energy",
            "import_energy",
            "r1","r2","r3","r4"
        FROM "sistema_contador"
        WHERE "name"=$meter
        ORDER BY "time" DESC LIMIT 1
    """, bind_params=dict(meter=meter))

    for v in result.get_points():
        return rfc3336toiso(v['time'])

    return None

def transfer_meter_to_plantmonitor(c, flux_client, meter, upto):
    last_date = last_uploaded_plantmonitor_measures(flux_client, meter)
    measures = measures_from_date(c, meter, beyond=last_date, upto=upto)
    logger.debug("Uploading {} measures for meter {} older than {} from erp to influxdb".format(len(measures), meter, last_date))
    upload_measures(flux_client, meter, measures)

# vim: et sw=4 ts=4
