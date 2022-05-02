import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')
import unittest
from unittest import mock

from datetime import datetime, timezone

from maintenance.db_manager import DBManager
from maintenance.db_test_factory import DbPlantFactory

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
        # Datetime GHI GTI TEMP PVOUT
        return [
            (datetime(2015,1,1, 0,30,tzinfo=timezone.utc), 0., 0., -6.2, 0.),
            (datetime(2015,1,1, 1,30,tzinfo=timezone.utc), 0., 0., -5.9, 0.),
            (datetime(2015,1,1, 2,30,tzinfo=timezone.utc), 0., 0., -5.6, 0.),
            (datetime(2015,1,1, 3,30,tzinfo=timezone.utc), 0., 0., -5.3, 0.),
            (datetime(2015,1,1, 4,30,tzinfo=timezone.utc), 0., 0., -5.2, 0.),
            (datetime(2015,1,1, 5,30,tzinfo=timezone.utc), 0., 0., -5.1, 0.),
            (datetime(2015,1,1, 6,30,tzinfo=timezone.utc), 5., 5., -4.8, 0.),
            (datetime(2015,1,1, 7,30,tzinfo=timezone.utc), 66., 66., -4.9, 0.),
            (datetime(2015,1,1, 8,30,tzinfo=timezone.utc), 123., 123., -4.7, 0.),
            (datetime(2015,1,1, 9,30,tzinfo=timezone.utc), 110., 110., -4.3, 0.),
            (datetime(2015,1,1,10,30,tzinfo=timezone.utc), 91., 91., -3.9, 0.),
            (datetime(2015,1,1,11,30,tzinfo=timezone.utc), 102., 102., -3.4, 0.),
            (datetime(2015,1,1,12,30,tzinfo=timezone.utc), 86., 86., -3.4, 0.),
            (datetime(2015,1,1,13,30,tzinfo=timezone.utc), 52., 52., -3.3, 0.),
            (datetime(2015,1,1,14,30,tzinfo=timezone.utc), 9., 9., -3.5, 0.),
            (datetime(2015,1,1,15,30,tzinfo=timezone.utc), 0., 0., -3.7, 0.),
            (datetime(2015,1,1,16,30,tzinfo=timezone.utc), 0., 0., -3.8, 0.),
            (datetime(2015,1,1,17,30,tzinfo=timezone.utc), 0., 0., -4.0, 0.),
            (datetime(2015,1,1,18,30,tzinfo=timezone.utc), 0., 0., -4.2, 0.),
            (datetime(2015,1,1,19,30,tzinfo=timezone.utc), 0., 0., -4.7, 0.),
            (datetime(2015,1,1,20,30,tzinfo=timezone.utc), 0., 0., -5.6, 0.),
            (datetime(2015,1,1,21,30,tzinfo=timezone.utc), 0., 0., -6.1, 0.),
            (datetime(2015,1,1,22,30,tzinfo=timezone.utc), 0., 0., -6.0, 0.),
            (datetime(2015,1,1,23,30,tzinfo=timezone.utc), 0., 0., -5.9, 0.),
        ]

    def sample_reading_2(self):
        # Datetime GHI DIF DNI PVOUT
        return [
            (datetime(2015,1,1, 0,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1, 1,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1, 2,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1, 3,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1, 4,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1, 5,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1, 6,30,tzinfo=timezone.utc), 5.919966280460358, 5.390461027622223, 10.358650207519531, 0.0),
            (datetime(2015,1,1, 7,30,tzinfo=timezone.utc), 68.19317054748535, 50.710779666900635, 150.0716438293457, 0.0),
            (datetime(2015,1,1, 8,30,tzinfo=timezone.utc), 126.30745887756348, 94.16604423522949, 137.40283393859863, 0.0),
            (datetime(2015,1,1, 9,30,tzinfo=timezone.utc), 105.13788652420044, 84.07496500015259, 74.75265902280807, 0.0),
            (datetime(2015,1,1,10,30,tzinfo=timezone.utc), 93.83942031860352, 93.83942031860352, 0.0, 0.0),
            (datetime(2015,1,1,11,30,tzinfo=timezone.utc), 113.57184791564941, 106.95848846435547, 22.652618885040283, 0.0),
            (datetime(2015,1,1,12,30,tzinfo=timezone.utc), 91.86378479003906, 83.2255973815918, 33.165565490722656, 0.0),
            (datetime(2015,1,1,13,30,tzinfo=timezone.utc), 52.65634632110596, 51.37733268737793, 9.948320776224136, 0.0),
            (datetime(2015,1,1,14,30,tzinfo=timezone.utc), 6.830964244902134, 6.830964244902134, 0.0, 0.0),
            (datetime(2015,1,1,15,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1,16,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1,17,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1,18,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1,19,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1,20,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1,21,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1,22,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
            (datetime(2015,1,1,23,30,tzinfo=timezone.utc), 0., 0., 0., 0.),
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
        self.maxDiff = None
        with open('test_data/solargis_test_response.xml', 'r') as file:
            text_response = file.read()

        request_time = datetime(2022,1,1,12,30,tzinfo=timezone.utc)
        readings = self.api.text_response_to_readings(text_response, request_time=request_time)

        expected = [
            (datetime(2015,1,1, 0,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1, 1,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1, 2,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1, 3,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1, 4,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1, 5,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1, 6,30,tzinfo=timezone.utc), 5.919966280460358, 5.390461027622223, 10.358650207519531, 0.0, 'solargis', request_time),
            (datetime(2015,1,1, 7,30,tzinfo=timezone.utc), 68.19317054748535, 50.710779666900635, 150.0716438293457, 0.0, 'solargis', request_time),
            (datetime(2015,1,1, 8,30,tzinfo=timezone.utc), 126.30745887756348, 94.16604423522949, 137.40283393859863, 0.0, 'solargis', request_time),
            (datetime(2015,1,1, 9,30,tzinfo=timezone.utc), 105.13788652420044, 84.07496500015259, 74.75265902280807, 0.0, 'solargis', request_time),
            (datetime(2015,1,1,10,30,tzinfo=timezone.utc), 93.83942031860352, 93.83942031860352, 0.0, 0.0, 'solargis', request_time),
            (datetime(2015,1,1,11,30,tzinfo=timezone.utc), 113.57184791564941, 106.95848846435547, 22.652618885040283, 0.0, 'solargis', request_time),
            (datetime(2015,1,1,12,30,tzinfo=timezone.utc), 91.86378479003906, 83.2255973815918, 33.165565490722656, 0.0, 'solargis', request_time),
            (datetime(2015,1,1,13,30,tzinfo=timezone.utc), 52.65634632110596, 51.37733268737793, 9.948320776224136, 0.0, 'solargis', request_time),
            (datetime(2015,1,1,14,30,tzinfo=timezone.utc), 6.830964244902134, 6.830964244902134, 0.0, 0.0, 'solargis', request_time),
            (datetime(2015,1,1,15,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1,16,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1,17,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1,18,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1,19,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1,20,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1,21,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1,22,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
            (datetime(2015,1,1,23,30,tzinfo=timezone.utc), 0., 0., 0., 0., 'solargis', request_time),
        ]

        self.assertListEqual(readings, expected)

    def test__get_current_solargis_irradiance_readings_location__only_GHI(self):

        self.maxDiff = None

        from_date = todtaware('2015-1-01 13:00:00')
        to_date = todtaware('2015-1-01 15:00:00')
        lat = 48.61259
        lon = 20.827079
        processing_keys = 'GHI'

        status, readings = self.api.get_current_solargis_irradiance_readings_location(lat, lon, from_date, to_date, processing_keys)

        self.assertEqual(status, 200)

        expected = [(t, ghi, 'solargis', mock.ANY) for t,ghi,gti,temp,pvout in self.sample_reading()]

        self.assertEqual(status, 200)
        self.assertListEqual(readings, expected)

    def test__get_current_solargis_irradiance_readings_location__GHI_GTI_TEMP(self):

        self.maxDiff = None

        from_date = todtaware('2015-01-01 13:00:00')
        to_date = todtaware('2015-01-01 15:00:00')
        lat = 48.61259
        lon = 20.827079
        processing_keys = 'GHI GTI TEMP'

        status, readings = self.api.get_current_solargis_irradiance_readings_location(lat, lon, from_date, to_date, processing_keys)

        self.assertEqual(status, 200)

        expected = [(t, ghi, gti, temp, 'solargis', mock.ANY) for t,ghi,gti,temp,pvout in self.sample_reading()]

        print([(ghi, gti, temp) for _, ghi, gti, temp, *_ in readings])

        self.assertEqual(status, 200)
        self.assertListEqual(readings, expected)

    # TMOD is not working
    def _test__get_current_solargis_irradiance_readings_location__GHI_GTI_TMOD(self):

        self.maxDiff = None

        from_date = todtaware('2015-01-01 13:00:00')
        to_date = todtaware('2015-01-01 15:00:00')
        lat = 48.61259
        lon = 20.827079
        processing_keys = 'GHI GTI TMOD'

        readings = self.api.get_current_solargis_irradiance_readings_location(lat, lon, from_date, to_date, processing_keys)

        expected = [(t, 1, ghi, gti, temp, 'solargis', mock.ANY) for t,ghi,gti,temp,pvout in self.sample_reading()]

        self.assertListEqual(readings, expected)

    def test__get_current_solargis_irradiance_readings__real_processing_keys(self):

        self.maxDiff = None

        from_date = todtaware('2015-01-01 13:00:00')
        to_date = todtaware('2015-01-01 15:00:00')
        #TODO pvout doesnt work atm
        #processing_keys = 'GHI GTI TEMP PVOUT'
        processing_keys = 'GHI GTI TEMP'

        self.api.plant_locations = {1: (48.61259,20.827079)}

        readings = self.api.get_current_solargis_irradiance_readings(from_date, to_date, processing_keys)

        expected = [(t, 1, ghi, gti, temp, 'solargis', mock.ANY) for t,ghi,gti,temp,pvout in self.sample_reading()]

        self.assertListEqual(readings, expected)


    def test__get_current_solargis_readings_standarized__base(self):

        self.maxDiff = None

        from_date = todtaware('2015-01-01 13:00:00')
        to_date = todtaware('2015-01-01 15:00:00')
        #processing_keys = 'GHI GTI TEMP'

        self.api.plant_locations = {1: (48.61259,20.827079)}

        readings = self.api.get_current_solargis_readings_standarized(from_date, to_date)

        expected = [(t, 1, int(ghi), int(gti), int(temp*10), None, 'solargis', mock.ANY) for t,ghi,gti,temp,pvout in self.sample_reading()]

        self.assertListEqual(readings, expected)


class ApiSolargis_DB_Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from conf import envinfo

        database_info = envinfo.DB_CONF
        solargis_conf = envinfo.SOLARGIS

        cls.api = ApiSolargis(**solargis_conf)
        cls.api.create_xsd_schema()

        debug = False

        cls.dbmanager = DBManager(**database_info, echo=debug).__enter__()

        cls.plantfactory = DbPlantFactory(cls.dbmanager)

    @classmethod
    def tearDownClass(cls):
        cls.dbmanager.__exit__()

    def setUp(self):
        self.session = self.dbmanager.db_con.begin()
        self.dbmanager.db_con.execute('SET TIME ZONE UTC;')

    def tearDown(self):
        self.session.rollback()
        self.session.close()

    def create_plant(self):
        query = '''

        '''
        self.dbmanager.execute(query)

    def test__create_table__base(self):

        self.plantfactory.create_plant_with_location()

        self.api.create_table(self.dbmanager.db_con)

        table_created = self.dbmanager.db_con.execute('''
            select exists ( select from pg_tables where tablename = 'satellite_readings' ) '''
        ).fetchone()

        self.assertTrue(table_created[0])

    def test__create_table__already_exists(self):

        self.plantfactory.create_plant_with_location()

        self.api.create_table(self.dbmanager.db_con)
        self.api.create_table(self.dbmanager.db_con)

    def test__save_to_db__base(self):

        self.plantfactory.create_plant_with_location()
        self.api.create_table(self.dbmanager.db_con)

        request_time = todtaware('2021-11-02 13:00:00')

        readings = [
            (todtaware('2021-11-01 13:00:00'), 1, 100, 900, 6000, 10000, 'solargis', request_time),
            (todtaware('2021-11-01 14:00:00'), 1, 200, 1000, 6000, 10000, 'solargis', request_time),
            (todtaware('2021-11-01 13:00:00'), 2, 10000, 90000, 600000, 1000000, 'solargis', request_time),
            (todtaware('2021-11-01 14:00:00'), 2, 20000, 100000, 600000, 1000000, 'solargis', request_time),
        ]

        num_rows = self.api.save_to_db(self.dbmanager.db_con, readings)

        self.assertEqual(num_rows, len(readings))

        result = self.dbmanager.db_con.execute('select * from satellite_readings;').fetchall()

        self.assertListEqual(result, readings)