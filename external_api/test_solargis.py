import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')
import unittest
from datetime import datetime, timezone

from external_api.api_solargis import ApiSolargis

from meteologica.utils import todtaware

@unittest.skipIf(False, "we're querying an external api, let's disable tests by default")
class ApiSolargis_Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        from conf import envinfo
        solargis_conf = envinfo.SOLARGIS

        cls.api = ApiSolargis(**solargis_conf)
        cls.api.create_xsd_schema()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def sample_reading(self):
        # Datetime GHI DIF DNI PVOUT
        return [
            (datetime(2015,1,1, 0,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 1,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 2,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 3,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 4,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 5,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 6,30,tzinfo=timezone.utc), 5.919966280460358, 5.390461027622223, 10.358650207519531, 0.0),
            (datetime(2015,1,1, 7,30,tzinfo=timezone.utc), 68.19317054748535, 50.710779666900635, 150.0716438293457, 0.0),
            (datetime(2015,1,1, 8,30,tzinfo=timezone.utc), 126.30745887756348, 94.16604423522949, 137.40283393859863, 0.0),
            (datetime(2015,1,1, 9,30,tzinfo=timezone.utc), 105.13788652420044, 84.07496500015259, 74.75265902280807, 0.0),
            (datetime(2015,1,1,10,30,tzinfo=timezone.utc), 93.83942031860352, 93.83942031860352, 0.0, 0.0),
            (datetime(2015,1,1,11,30,tzinfo=timezone.utc), 113.57184791564941, 106.95848846435547, 22.652618885040283, 0.0),
            (datetime(2015,1,1,12,30,tzinfo=timezone.utc), 91.86378479003906, 83.2255973815918, 33.165565490722656, 0.0),
            (datetime(2015,1,1,13,30,tzinfo=timezone.utc), 52.65634632110596, 51.37733268737793, 9.948320776224136, 0.0),
            (datetime(2015,1,1,14,30,tzinfo=timezone.utc), 6.830964244902134, 6.830964244902134, 0.0, 0.0),
            (datetime(2015,1,1,15,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,16,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,17,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,18,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,19,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,20,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,21,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,22,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,23,30,tzinfo=timezone.utc), 0, 0, 0, 0),
        ]

    def test__check_connection__arbitrary(self):
        request_xml = '''
            <ws:dataDeliveryRequest dateFrom="2014-04-28" dateTo="2014-04-28"
            xmlns="http://geomodel.eu/schema/data/request"
            xmlns:ws="http://geomodel.eu/schema/ws/data"
            xmlns:geo="http://geomodel.eu/schema/common/geo"
            xmlns:pv="http://geomodel.eu/schema/common/pv"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <site id="demo_site" name="Demo site" lat="48.61259" lng="20.827079">
            </site>
            <processing key="GHI" summarization="HOURLY" terrainShading="true">
            </processing>
            </ws:dataDeliveryRequest>
        '''

        status_code, text_response = self.api.get_arbitrary_payload(request_xml)

        self.assertEqual(status_code, 200)

        self.assertNotEqual(text_response, '')

    def test__get_arbitrary_payload__no_name(self):
        request_xml = '''
            <ws:dataDeliveryRequest dateFrom="2014-04-28" dateTo="2014-04-28"
            xmlns="http://geomodel.eu/schema/data/request"
            xmlns:ws="http://geomodel.eu/schema/ws/data"
            xmlns:geo="http://geomodel.eu/schema/common/geo"
            xmlns:pv="http://geomodel.eu/schema/common/pv"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <site id="demo_site" lat="48.61259" lng="20.827079">
            </site>
            <processing key="GHI" summarization="HOURLY" terrainShading="true">
            </processing>
            </ws:dataDeliveryRequest>
        '''

        status_code, text_response = self.api.get_arbitrary_payload(request_xml)

        self.assertEqual(status_code, 200)

        self.assertNotEqual(text_response, '')

    def test__create_xsd_schema__base(self):
        self.api.create_xsd_schema()
        self.assertIsNotNone(self.api.schema)

    def test__text_response_to_readings__base(self):

        with open('test_data/solargis_test_response.xml', 'r') as file:
            text_response = file.read()

        readings = self.api.text_response_to_readings(text_response)

        expected = [
            (datetime(2015,1,1, 0,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 1,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 2,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 3,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 4,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 5,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1, 6,30,tzinfo=timezone.utc), 5.919966280460358, 5.390461027622223, 10.358650207519531, 0.0),
            (datetime(2015,1,1, 7,30,tzinfo=timezone.utc), 68.19317054748535, 50.710779666900635, 150.0716438293457, 0.0),
            (datetime(2015,1,1, 8,30,tzinfo=timezone.utc), 126.30745887756348, 94.16604423522949, 137.40283393859863, 0.0),
            (datetime(2015,1,1, 9,30,tzinfo=timezone.utc), 105.13788652420044, 84.07496500015259, 74.75265902280807, 0.0),
            (datetime(2015,1,1,10,30,tzinfo=timezone.utc), 93.83942031860352, 93.83942031860352, 0.0, 0.0),
            (datetime(2015,1,1,11,30,tzinfo=timezone.utc), 113.57184791564941, 106.95848846435547, 22.652618885040283, 0.0),
            (datetime(2015,1,1,12,30,tzinfo=timezone.utc), 91.86378479003906, 83.2255973815918, 33.165565490722656, 0.0),
            (datetime(2015,1,1,13,30,tzinfo=timezone.utc), 52.65634632110596, 51.37733268737793, 9.948320776224136, 0.0),
            (datetime(2015,1,1,14,30,tzinfo=timezone.utc), 6.830964244902134, 6.830964244902134, 0.0, 0.0),
            (datetime(2015,1,1,15,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,16,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,17,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,18,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,19,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,20,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,21,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,22,30,tzinfo=timezone.utc), 0, 0, 0, 0),
            (datetime(2015,1,1,23,30,tzinfo=timezone.utc), 0, 0, 0, 0),
        ]

        self.assertListEqual(readings, expected)

    def test__get_current_solargis_irradiance_readings_only_GHI(self):

        self.maxDiff = None

        from_date = todtaware('2021-11-01 13:00:00')
        to_date = todtaware('2021-11-01 15:00:00')
        lat = 48.61259
        long = 20.827079
        processing_keys = 'GHI'

        status, readings = self.api.get_current_solargis_irradiance_readings(lat, long, from_date, to_date, processing_keys)

        self.assertEqual(status, 200)

        expected = [(t, ghi) for t,ghi,gti,temp,pvout in self.sample_reading()]

        self.assertEqual(status, 200)
        self.assertListEqual(readings, expected)

    def test__get_current_solargis_irradiance_readings_GHI_GTI_TEMP(self):

        self.maxDiff = None

        from_date = todtaware('2021-11-01 13:00:00')
        to_date = todtaware('2021-11-01 15:00:00')
        lat = 48.61259
        long = 20.827079
        processing_keys = 'GHI GTI TEMP'

        status, result = self.api.get_current_solargis_irradiance_readings(lat, long, from_date, to_date, processing_keys)

        self.assertEqual(status, 200)

        expected = [(t, ghi, gti, temp) for t,ghi,gti,temp,pvout in self.sample_reading()]

        self.assertEqual(status, 200)
        self.assertTrue('data' in result)
