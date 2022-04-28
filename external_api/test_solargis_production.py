import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')
import unittest

from external_api.api_solargis import ApiSolargis

from meteologica.utils import todtaware

@unittest.skipIf(True, "we're querying an external api, let's disable tests by default")
class ApiSolargis_ProductionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        from conf import envinfo
        solargis_conf = envinfo.SOLARGIS

        cls.api = ApiSolargis(**solargis_conf)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__get_current_solargis_irradiance_readings__production_credentials(self):
        self.maxDiff = None

        from_date = todtaware('2021-11-01 13:00:00')
        to_date = todtaware('2021-11-01 15:00:00')

        lat=48.61259
        long=20.827079

        status, result = self.api.get_current_solargis_irradiance_readings(lat, long, from_date, to_date)

        self.assertEqual(status, 200)

        # expected = self.sample_instant_reading()

        self.assertEqual(status, 200)
        self.assertTrue('data' in result)
        # self.assertEqual(len(readings['data']), 1)
        # self.assertListEqual(list(readings['data'][0].keys()), list(expected['data'][0].keys()))
