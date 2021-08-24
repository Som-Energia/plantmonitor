import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

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
    meter_connection_protocol,
)

def assertTimeSeriesEqual(self, result, expected):
    self.assertNsEqual(ns(data=result), ns(data=expected))

class Meters_Test(unittest.TestCase):
    from yamlns.testutils import assertNsEqual
    assertTimeSeriesEqual = assertTimeSeriesEqual

    def setUp(self):
        self.maxDiff=None

    def test__meter_connection_protocol_(self):
        c = Client(**config.erppeek)
        meter_ip = '88300864'
        meter_moxa = '8814894104'
        result = meter_connection_protocol(c, [meter_ip, meter_moxa])
        self.assertEqual(result, {
            meter_ip: 'ip',
            meter_moxa: 'moxa',
        })

    def test__meter_connection_protocol__whenMeterMissing(self):
        c = Client(**config.erppeek)
        meter_ip = '88300864'
        meter_missing = 'NotAMeter'
        result = meter_connection_protocol(c, [meter_ip, meter_missing])
        self.assertEqual(result, {
            meter_ip: 'ip',
        })

    @unittest.skipIf(True, "requires data in test erp database")
    def test__measures_from_date(self):
        c = Client(**config.erppeek)
        meter = '88300864'
        measures = measures_from_date(c, meter,
            beyond="2019-10-02 09:00:00",
            upto  ="2019-10-02 12:00:00")
        self.assertTimeSeriesEqual(measures,[
            ('2019-10-02 10:00:00', 1407, 0, 0, 9, 0, 0),
            ('2019-10-02 11:00:00', 1687, 0, 0, 22, 0, 0),
        ])

    @unittest.skipIf(True, "requires data in test erp database")
    def test__quarterly_filter_time(self):
        c = Client(**config.erppeek)
        meter = '44711885'
        measures = measures_from_date(c, meter,
            beyond="2020-09-09 08:00:00",
            upto  ="2020-09-09 09:00:00")
        self.assertListEqual(measures,[])

    @unittest.skipIf(True, "requires data in test erp database")
    def test__measures_from_date__whenEmpty(self):
        c = Client(**config.erppeek)
        meter = '88300864'
        measures = measures_from_date(c, meter,
            beyond="2016-08-02 09:00:00",
            upto  ="2016-08-02 12:00:00")
        self.assertTimeSeriesEqual(measures,[])

    @unittest.skipIf(True, "requires data in test erp database")
    def test__measures_from_date__withNoBeyondDate__takesFromBegining(self):
        c = Client(**config.erppeek)
        meter = '88300864'
        measures = measures_from_date(c, meter,
            beyond=None,
            upto  ="2019-06-01 03:00:00")
        self.assertTimeSeriesEqual(measures,[
            # First measure is on 2019-06-01 (Madrid) all zeros (still not producing)
            ('2019-05-31 22:00:00', 0, 0, 0, 0, 0, 0),
            ('2019-05-31 23:00:00', 0, 0, 0, 0, 0, 0),
            ('2019-06-01 00:00:00', 0, 0, 0, 0, 0, 0),
            ('2019-06-01 01:00:00', 0, 0, 0, 0, 0, 0),
            ('2019-06-01 02:00:00', 0, 0, 0, 0, 0, 0),
            ])

    @unittest.skipIf(True, "requires production db and gisce has to fix utc problems in erp database")
    def test__measures_from_date__summerToWintertime_noDuppedDates(self):
        c = Client(**config.erppeek)
        meter = '501600324'
        measures = measures_from_date(c, meter,
            beyond="2020-10-24 20:00:00",
            upto  ="2020-10-25 03:00:00",
        )
        self.assertTimeSeriesEqual(measures,[
            ('2020-10-24 21:00:00', 0, 4, 0, 0, 0, 4),
            ('2020-10-24 22:00:00', 0, 3, 0, 0, 0, 5),
            ('2020-10-24 23:00:00', 0, 3, 0, 0, 0, 6),
            #('2020-10-25 00:00:00', 0, 2, 0, 0, 0, 5), # Without wrong data this should be the one
            #('2020-10-25 01:00:00', 0, 2, 0, 0, 0, 5), # Appears this one, filtered as workaround
            ('2020-10-25 01:00:00', 0, 3, 0, 0, 0, 6),
            ('2020-10-25 02:00:00', 0, 3, 0, 0, 0, 5),
            ])

@unittest.skipIf(True, "requires influx database")
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
            ('2019-10-02 10:00:00', 1407, 8 ,0, 10, 20, 30),
            ('2019-10-02 11:00:00', 1687, 7, 0, 30, 20, 10),
        ])
        result = uploaded_plantmonitor_measures(
             self.flux_client,'88300864')

        self.assertTimeSeriesEqual(result,[
            ('2019-10-02 10:00:00', 1407, 8, 0, 10, 20, 30),
            ('2019-10-02 11:00:00', 1687, 7, 0, 30, 20, 10),
        ])

    def test__upload_meter_measures__isolatesMeters(self):
        upload_measures(self.flux_client, '88300864', [
            ('2019-10-02 10:00:00', 1407, 8, 0, 0, 0, 0),
            ('2019-10-02 11:00:00', 1687, 7, 0, 0, 0, 0),
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
            ('2019-10-02 10:00:00', 1407, 8, 0, 0, 0, 0),
            ('2019-10-02 11:00:00', 1687, 7, 0, 0, 0, 0),
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
            ('2019-05-31 22:00:00', 0, 0, 0, 0, 0, 0),
            ('2019-05-31 23:00:00', 0, 0, 0, 0, 0, 0),
            ('2019-06-01 00:00:00', 0, 0, 0, 0, 0, 0),
            ('2019-06-01 01:00:00', 0, 0, 0, 0, 0, 0),
            ('2019-06-01 02:00:00', 0, 0, 0, 0, 0, 0),
        ])

    def test__transfer_meter_to_plantmonitor__withAlreadyTransferedMeasures(self):
        c = Client(**config.erppeek)
        meter = '88300864'
        upload_measures(self.flux_client, meter, [
            ('2019-10-02 10:00:00', 1407, 8, 0, 0, 0, 0),
        ])

        transfer_meter_to_plantmonitor(c, self.flux_client, meter, upto='2019-10-02 12:00:00')

        result = uploaded_plantmonitor_measures(
             self.flux_client, meter)

        self.assertTimeSeriesEqual(result,[
            ('2019-10-02 10:00:00', 1407, 8, 0, 0, 0, 0),
            ('2019-10-02 11:00:00', 1687, 0, 0, 22, 0, 0),
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

    # TODO fake/mock erp database
    def _test__insert_meter_readings_to_plants(self):
        c = Client(**config.erppeek)
        for meter in telemeasure_meter_names(c):
            upload_measures(self.flux_client, meter, [
                ('2019-10-02 10:00:00', 1407),
            ])

        insert_meter_readings_to_plants(c, meter_name, upto='2019-10-02 12:00:00')

        meter = '501815908',
        result = uploaded_plantmonitor_measures(self.flux_client, meter)
        self.assertTimeSeriesEqual(result,[
            ('2019-10-02 10:00:00', 1407),
            ('2019-10-02 11:00:00', 1687),
        ])

# vim: et sw=4 ts=4
