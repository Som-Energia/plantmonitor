from plantmeter.isodates import utcisodatetime
from influxdb import InfluxDBClient as client_db
import datetime
import logging

def telemeasure_meter_names(c):
    Meter = c.model('giscedata.lectures.comptador')
    meter_ids = Meter.search([
        ('technology_type', '=', 'telemeasure'),
        ])
    meters = Meter.read(meter_ids, ['name'])
    return [meter['name'] for meter in meters]

def measures_from_date(c, meter, beyond, upto):
    print(meter)
    measure_ids = c.TmProfile.search([
        ('name','=', meter),
        ('utc_timestamp','>', beyond),
        ('utc_timestamp','<', upto),
        ],
        limit=10,
        #order="create_date DESC",
        order="utc_timestamp DESC",
        ) # TODO: Manage duplicated datetimes
    if not measure_ids: return []
    measure_ids = [int(m) for m in measure_ids]
    measures = c.TmProfile.read(measure_ids,[
        'ae',
        'utc_timestamp',
        ])
    return [
        (measure['utc_timestamp'], int(measure['ae']))
        for measure in sorted(measures,
            key=lambda x: x['utc_timestamp'])
        ]

def _upload_meter_measures(measures):
    plant = ProductionPlant()
    if not plant.load('conf/modmap.yaml','Alcolea'):
        logging.error('Error loadinf yaml definition file...')
        sys.exit(-1)    
    flux_client = client_db(plant.db)

def publish_influx(flux_client,metrics):
    flux_client.write_points([metrics])
    logging.info("[INFO] Sent to InfluxDB")

def rfc3336toiso(isodate):
    dt = datetime.datetime.strptime(
        isodate, "%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def upload_measures(flux_client, meter, measures):
    for isodatetime, measure in measures:
        data = dict(
            measurement = 'sistema_contador',
            tags = dict(
                name=meter,
            ),
            fields = dict(
                export_energy = measure,
               # id = tm_profile['id'],
            ),
            time = isodatetime,
        )
        publish_influx(flux_client, data)

def uploaded_plantmonitor_measures(flux_client, meter):
    result = flux_client.query("""\
        SELECT "time","export_energy"
        FROM "sistema_contador"
        WHERE "name"=$meter
        ORDER BY "time"
    """, bind_params=dict(meter=meter))
    result = [
        (rfc3336toiso(v['time']),v['export_energy'])
        for v in result.get_points()
        ]
    return result





