import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')
import unittest
import time

from conf import envinfo

from lxml import etree

from external_api.api_solargis import ApiSolargis, Site

from meteologica.utils import todtaware

@unittest.skipIf(False, "we're only allowed N requests per month, so disable tests by default")
class ApiSolargis_Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        api_base_url = " https://solargis.info/ws/rest"
        api_key = 'demo'

        cls.api = ApiSolargis(api_base_url, api_key)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def sample_instant_reading(self):
        return  {}

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

    def test__get_current_solargis_irradiance_readings(self):

        self.maxDiff = None

        from_date = todtaware('2021-11-01 13:00:00')
        to_date = todtaware('2021-11-01 15:00:00')
        site = Site(id='demo_site', name='Demo site', lat=48.61259, long=20.827079)

        status, result = self.api.get_current_solargis_irradiance_readings(site, from_date, to_date)

        self.assertEqual(status, 200)

        # expected = self.sample_instant_reading()

        self.assertEqual(status, 200)
        self.assertTrue('data' in result)
        # self.assertEqual(len(readings['data']), 1)
        # self.assertListEqual(list(readings['data'][0].keys()), list(expected['data'][0].keys()))
