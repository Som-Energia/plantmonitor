import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')
import unittest

from conf import envinfo

from external_api.api_monsol import ApiMonsol

from meteologica.utils import todtaware

@unittest.skipIf(True, "we're only allowed N requests per month, so disable tests by default")
class ApiMonsol_Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        api_base_url = envinfo.MONSOL['api_base_url']
        api_auth_url = envinfo.MONSOL['api_auth_url']
        version = envinfo.MONSOL['version']
        assert envinfo.MONSOL_CREDENTIALS
        username, password = list(envinfo.MONSOL_CREDENTIALS.items())[0]
        cls.config = {
            'api_base_url': api_base_url,
            'api_auth_url': api_auth_url,
            'version': version,
            'username': username,
            'password': password
        }

        cls.api = ApiMonsol(cls.config)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__get_token(self):
        token = self.api.reuse_token()

        self.assertTrue(token)

    def test__get_meter__two_hours(self):

        date_from = todtaware('2021-11-01 13:00:00')
        date_to = todtaware('2021-11-01 15:00:00')
        granularity = '60'
        device = None

        status, token = self.api.reuse_token()

        self.assertEqual(status, 200)

        readings = self.api.get_meter(date_from, date_to, granularity, device)

        expected = {'something': 'foo'}

        self.assertDictEqual(readings, expected)

    # TODO handle rate limited