import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')
import unittest
import time

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

    def sample_instant_reading(self):
        return  {
            'message': '[OK][QUERY][CONTADORES][DATOS ACTUALES] ',
            'data': [{
                'nick': 'VALLEHERMOSO',
                'id_dispositivo': 2201,
                'numserie': 2201,
                'id_tipo_dispositivo': 3,
                'fecha_completa': '2021-12-01 14:28:59',
                'pat': 1293.5,
                'pac': 1293.5,
                'pac1': 430.3800048828125,
                'pac2': 432.260009765625,
                'pac3': 430.8299865722656,
                'iac1': 225,
                'iac2': 229,
                'iac3': 225,
                'vac1': 9563.4,
                'vac2': 9416.3,
                'vac3': 9567.3,
                'pr1': 22.53,
                'pr2': 29.95,
                'pr3': 39.59,
                'prt': 92.08,
                'fp1': -0.98,
                'fp2': -0.98,
                'fp3': -0.98,
                'fpt': 0,
                'eedia': 5795,
                'eidia': 0,
                'eae': 18530277,
                'eai': 105719,
                'er1': 279,
                'er2': 1690641,
                'er3': 4930,
                'er4': 109365,
                'horas': None,
                'enerdiaPonderada': 3.066137566137566
            }],
            'code': 200,
            'timestamp': '01-12-2021 14:34:24'
        }

    def sample_historic_reading(self):
        return  {
            'message': '[OK][QUERY][CONTADORES][DATOS ACTUALES] ',
            'data': [{
                'elemento': 'VALLEHERMOSO',
                'id_dispositivo': 2201,
                'id_tipo_dispositivo': 3,
                'fecha_completa': '2021-11-01 00:05:00', # TODO a bug in the api ignores hour_ini
                'kw_pico': 1890,
                'pac': -6.900000095367432,
                'pac1': -1.9900000095367432,
                'pac2': -2.0299999713897705,
                'pac3': -2.869999885559082,
                'iac1': 1,
                'iac2': 1,
                'iac3': 1,
                'vac1': 9529.6,
                'vac2': 9435.7,
                'vac3': 9557.6,
                'pr1': -0.68,
                'pr2': 0.25,
                'pr3': -0.27,
                'prt': -0.71,
                'fp1': 0.94,
                'fp2': 0.99,
                'fp3': 0.99,
                'fpt': 0,
                'eedia': 0,
                'eidia': 3,
                'eae': 18291955,
                'eai': 104279,
                'er1': 274,
                'er2': 1670501,
                'er3': 4885,
                'er4': 107377,
            }],
            'code': 200,
            'timestamp': '01-12-2021 14:34:24'
        }

    def test__get_token(self):
        status, token = self.api.reuse_token()

        self.assertEqual(status, 200)
        self.assertTrue(token)

    def test__get_current_monsol_meter_readings(self):

        self.maxDiff = None

        date_from = todtaware('2021-11-01 13:00:00')
        date_to = todtaware('2021-11-01 15:00:00')
        granularity = '60'
        device = None

        status, token = self.api.reuse_token()

        self.assertEqual(status, 200)

        status, readings = self.api.get_current_monsol_meter_readings(device)

        # TODO remove this when monsol fix the one second rate limit
        if status == 401:
            time.sleep(1)
            status, readings = self.api.get_current_monsol_meter_readings(device)

        expected = self.sample_instant_reading()

        self.assertEqual(status, 200)
        self.assertTrue('data' in readings)
        self.assertEqual(len(readings['data']), 1)
        self.assertListEqual(list(readings['data'][0].keys()), list(expected['data'][0].keys()))

    # TODO handle rate limited


    def test__get_monsol_meter_readings__two_hours(self):

        self.maxDiff = None

        date_from = todtaware('2021-11-01 13:00:00')
        date_to = todtaware('2021-11-01 15:00:00')
        granularity = '60'
        device = None

        status, token = self.api.reuse_token()

        self.assertEqual(status, 200)

        status, readings = self.api.get_monsol_meter_readings(date_from, date_to, granularity, device)

        # TODO remove this when monsol fix the one second rate limit
        if status == 401:
            time.sleep(1)
            status, readings = self.api.get_monsol_meter_readings(date_from, date_to, granularity, device)

        expected = self.sample_historic_reading()

        self.assertEqual(status, 200)
        self.assertTrue('data' in readings)
        self.assertGreater(len(readings['data']), 0)
        self.assertListEqual(list(readings['data'][0].keys()), list(expected['data'][0].keys()))
        self.assertDictEqual(readings['data'][0], expected['data'][0])
