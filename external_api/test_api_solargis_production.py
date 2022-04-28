import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')
import unittest

from external_api.api_solargis import ApiSolargis, Site

from meteologica.utils import todtaware

# run with pytest external_api.test_api_solargis_production
# to ensure this script sets the PLANTMONITOR_MODULE_SETTINGS to devel
# before any other test module
@unittest.skipIf(True, "we're querying an external api, let's disable tests by default")
class ApiSolargis_ProductionTest(unittest.TestCase):

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

    def test__is_production(self):
        self.assertNotEqual(self.api.api_key, 'demo')

    def test__get_current_solargis_irradiance_readings__production_credentials(self):
        self.maxDiff = None

        from_date = todtaware('2022-04-01 13:00:00')
        to_date = todtaware('2022-04-01 15:00:00')

        self.api.sites = {
            3: Site(
                id=3,
                name='Fontivsolar',
                latitude=40.932389,
                longitude=-4.968694,
                peak_power_w=990,
                installation_type='FREE_STANDING',
            )
        }

        readings = self.api.get_current_solargis_irradiance_readings(from_date, to_date)

        self.assertEqual(len(readings), 24)
        # self.assertListEqual(list(readings['data'][0].keys()), list(expected['data'][0].keys()))
