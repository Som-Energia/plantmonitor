import unittest
from erppeek import Client
from yamlns import namespace as ns
from conf import config
from influxdb import InfluxDBClient as client_db 
from .meters import (
    telemeasure_meter_names,
    measures_from_date,
    upload_measures,
    uploaded_plantmonitor_measures
)

def assertTimeSeriesEqual(self, result, expected):
    self.assertNsEqual(ns(data=result), ns(data=expected))


class Meters_Test(unittest.TestCase):
    from yamlns.testutils import assertNsEqual
    assertTimeSeriesEqual = assertTimeSeriesEqual

    def setUp(self):
        self.maxDiff=None

    def test__telemeasure_meter_names(self):
        c = Client(**config.erppeek)
        names = telemeasure_meter_names(c)
        self.assertEqual(sorted(names),[
            '501215455',
            '501215456',
            '501215457',
            '501600324',
            '501815908',
            '88300864',
        ])

    def test__measures_from_date(self):
        c = Client(**config.erppeek)
        meter = '88300864'
        measures = measures_from_date(c, meter,
            beyond="2019-10-02 09:00:00",
            upto  ="2019-10-02 12:00:00")
        self.assertTimeSeriesEqual(measures,[
            ('2019-10-02 10:00:00', 1407),
            ('2019-10-02 11:00:00', 1687),
        ])

    def test__empty_measures_from_date(self):
        c = Client(**config.erppeek)
        meter = '88300864'
        measures = measures_from_date(c, meter,
            beyond="2016-08-02 09:00:00",
            upto  ="2016-08-02 12:00:00")
        self.assertTimeSeriesEqual(measures,[])


class MetersFlux_Test(unittest.TestCase):
    from yamlns.testutils import assertNsEqual
    assertTimeSeriesEqual = assertTimeSeriesEqual

    def setUp(self):
        self.maxDiff=None
        self.flux_config = flux_config = ns.load(
                 'conf/modmap.yaml').plantmonitor[0].influx
        flux_config.influxdb_database = 'unittesting'
        self.flux_client = client_db(
                flux_config['influxdb_ip'],
                flux_config['influxdb_port'],
                flux_config['influxdb_user'],
                flux_config['influxdb_password'],
                flux_config['influxdb_database'],
                ssl=flux_config['influxdb_ssl'],
                verify_ssl=flux_config['influxdb_verify_ssl']
            )
        self.flux_client.drop_database(
                self.flux_config.influxdb_database
            )
        self.flux_client.create_database(
                flux_config.influxdb_database
            )

    def tearDown(self):
        return
        self.flux_client.drop_database(
                self.flux_config.influxdb_database
            )
 
    def test__upload_meter_measures__withNoMesures(self):
        result = uploaded_plantmonitor_measures(
             self.flux_client,'88300864')

        self.assertEqual(result,[
        ])

    def test__upload_meter_measures_(self):
        upload_measures(self.flux_client, '88300864', [
            ('2019-10-02 10:00:00', 1407),
            ('2019-10-02 11:00:00', 1687),
        ])
        result = uploaded_plantmonitor_measures(
             self.flux_client,'88300864')

        self.assertTimeSeriesEqual(result,[
            ('2019-10-02 10:00:00', 1407),
            ('2019-10-02 11:00:00', 1687),
        ])

    def test__upload_meter_measures__isolatesMeters(self):
        upload_measures(self.flux_client, '88300864', [
            ('2019-10-02 10:00:00', 1407),
            ('2019-10-02 11:00:00', 1687),
        ])
        result = uploaded_plantmonitor_measures(
             self.flux_client,'66666666')

        self.assertTimeSeriesEqual(result,[
        ])

