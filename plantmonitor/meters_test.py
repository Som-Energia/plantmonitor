import unittest
from erppeek import Client
from yamlns import namespace as ns
from conf import config
from influxdb import InfluxDBClient as client_db 
from .meters import (
    telemeasure_meter_names,
    measures_from_date,
    upload_measures,
    uploaded_plantmonitor_measures,
    last_uploaded_plantmonitor_measures,
    transfer_meter_to_plantmonitor,
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

    def test__measures_from_date__whenEmpty(self):
        c = Client(**config.erppeek)
        meter = '88300864'
        measures = measures_from_date(c, meter,
            beyond="2016-08-02 09:00:00",
            upto  ="2016-08-02 12:00:00")
        self.assertTimeSeriesEqual(measures,[])

    def test__measures_from_date__withNoBeyondDate__takesFromBegining(self):
        c = Client(**config.erppeek)
        meter = '88300864'
        measures = measures_from_date(c, meter,
            beyond=None,
            upto  ="2019-06-01 03:00:00")
        self.assertTimeSeriesEqual(measures,[
            # First measure is on 2019-06-01 (Madrid) all zeros (still not producing)
            ('2019-05-31 22:00:00', 0),
            ('2019-05-31 23:00:00', 0),
            ('2019-06-01 00:00:00', 0),
            ('2019-06-01 01:00:00', 0),
            ('2019-06-01 02:00:00', 0),
            ])


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

    def test__last_date_uploaded_measure_withNoMeasures(self):
        upload_measures(self.flux_client, '88300864', [
        ])

        result = uploaded_plantmonitor_measures(
             self.flux_client,'88300864')

        self.assertTimeSeriesEqual(result,[
        ])

    def test__last_date_uploaded_measure_withNoMeasures(self):
        upload_measures(self.flux_client, '88300864', [
        ])

        result = last_uploaded_plantmonitor_measures(
             self.flux_client,'88300864')

        self.assertEqual(result,None)

    def test__last_date_uploaded_measure(self):
        upload_measures(self.flux_client, '88300864', [
            ('2019-10-02 10:00:00', 1407),
            ('2019-10-02 11:00:00', 1687),
        ])

        result = last_uploaded_plantmonitor_measures(
             self.flux_client,'88300864')

        self.assertEqual(result,'2019-10-02 11:00:00')
        
    def test__transfer_meter_to_plantmonitor__brandNewMeter(self):
        c = Client(**config.erppeek)
        meter = '88300864'

        transfer_meter_to_plantmonitor(c, self.flux_client, meter, upto='2019-06-01 03:00:00')

        result = uploaded_plantmonitor_measures(
             self.flux_client, meter)

        self.assertTimeSeriesEqual(result,[
            ('2019-05-31 22:00:00', 0),
            ('2019-05-31 23:00:00', 0),
            ('2019-06-01 00:00:00', 0),
            ('2019-06-01 01:00:00', 0),
            ('2019-06-01 02:00:00', 0),
        ])

    def test__transfer_meter_to_plantmonitor__withAlreadyTransferedMeasures(self):
        c = Client(**config.erppeek)
        meter = '88300864'
        upload_measures(self.flux_client, meter, [
            ('2019-10-02 10:00:00', 1407),
        ])

        transfer_meter_to_plantmonitor(c, self.flux_client, meter, upto='2019-10-02 12:00:00')

        result = uploaded_plantmonitor_measures(
             self.flux_client, meter)

        self.assertTimeSeriesEqual(result,[
            ('2019-10-02 10:00:00', 1407),
            ('2019-10-02 11:00:00', 1687),
        ])

    def _test__transfer_time_measure_to_influx__disticntMeter(self):
        c = Client(**config.erppeek)
        for meter in telemeasure_meter_names(c):
            upload_measures(self.flux_client, meter, [
                ('2019-10-02 10:00:00', 1407),
            ])

        transfer_time_measure_to_influx(c, self.flux_client,upto='2019-10-02 12:00:00')

        meter = '501815908',
        result = uploaded_plantmonitor_measures(self.flux_client, meter) 
        self.assertTimeSeriesEqual(result,[
            ('2019-10-02 10:00:00', 1407),
            ('2019-10-02 11:00:00', 1687),
        ])




