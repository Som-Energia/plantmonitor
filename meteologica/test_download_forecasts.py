import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from meteologica.daily_download_from_api import download_meter_data

from meteologica.meteologica_api_utils import (
    MeteologicaApi_Mock,
    MeteologicaApi,
    MeteologicaApiError,
)

from meteologica.utils import todt, todtaware

from pony import orm
from ORM.orm_util import setupDatabase

#from django.conf import settings
from yamlns import namespace as ns
from pathlib import Path
from unittest.mock import patch, ANY
import unittest
import datetime

from ORM.models import database
from ORM.models import (
    Plant,
    Meter,
    MeterRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("test")

setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)

class DailyDownload_Test(unittest.TestCase):

    def createApi(self, **kwds):
        config = self.createConfig()
        params=dict(
            wsdl=config['meteo_test_url'],
            username=config['meteo_user'],
            password=config['meteo_password'],
            lastDateFile=self.getLastDateFile(),
            lastDateDownloadFile=self.getLastDateDownloadFile(),
            showResponses=False,
        )
        params.update(kwds)
        return MeteologicaApi(**params)

    def createConfig(self):
        return ns.load('conf/configdb_test.yaml')

    # TODO refactor the need of a lastDate download in MeteologicaApi, db should suffice
    def getLastDateFile(self):
        return 'lastDateFile-test.yaml'

    def getLastDateDownloadFile(self):
        return 'lastDateDownloadFile-test.yaml'

    def setUp(self):
        config = self.createConfig()

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        database.drop_all_tables(with_all_data=True)

        self.maxDiff=None
        # orm.set_sql_debug(True)

        database.create_tables()

        # database.generate_mapping(create_tables=True)
        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()

    def mainFacility(self):
        return 'Alibaba'

    def otherFacility(self):
        return 'Arigato'

    def sampleForecastMeta(self, plant_name, forecastdate):
        return {
            'facilityId': self.mainFacility(),
            'variableId': 'prod',
            'predictorId': 'aggregated',
            'granularity': '60',
            'forecastDate': forecastdate,
            'errorcode': None,
        }

    def samplePlantData(self, plant_name, forecastdate, forecasts, packet_time):
        return {
            'plant': plant_name,
            'version': '1.0',
            'time': packet_time,
            'devices': [{
                'id': 'ForecastMetadata:',
                'errorcode': None,
                'variable': 'prod',
                'predictor': 'aggregated',
                'forecastdate': forecastdate,
                'granularity': 60,
                'forecasts': forecasts,
            }]
        }

    def test__meteologicaToPlantsData__onePlant(self):

        alibaba = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        forecastdate = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)

        forecastMeta = self.sampleForecastMeta(self.mainFacility(), forecastdate)

        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        plants_data = Forecast.meteologicaToPlantsData(forecastMeta, data)

        forecasts = [
            {
                'time': datetime.datetime(2040, 1, 2, 0, 0),
                'percentil50': 10
            },
            {
                'time': datetime.datetime(2040, 1, 2, 1, 0),
                'percentil50': 20
            }
        ]

        expected = self.samplePlantData(self.mainFacility(), forecastdate, forecasts, ANY)
        self.assertEqual(len(plants_data), 1)
        self.assertDictEqual(plants_data[0], expected)

    def test__meteologicaToPlantsData__noData(self):
        forecastdate = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)

        forecastMeta = self.sampleForecastMeta(self.mainFacility(), forecastdate)

        plants_data = Forecast.meteologicaToPlantsData(forecastMeta, {})

        self.assertEqual(plants_data, [])

    def test__meteologicaToPlantsData__manyPlants(self):

        forecastdate = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)

        forecastMeta = self.sampleForecastMeta(self.mainFacility(), forecastdate)

        forecastsdata = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
           self.otherFacility(): [
                (todt("2040-01-02 00:00:00"), 30),
                (todt("2040-01-02 01:00:00"), 40),
            ],
        }

        plants_data = Forecast.meteologicaToPlantsData(forecastMeta, forecastsdata)

        forecastsMain = [{'time': f[0], 'percentil50': f[1]} for f in forecastsdata[self.mainFacility()]]
        forecastsOther = [{'time': f[0], 'percentil50': f[1]} for f in forecastsdata[self.otherFacility()]]

        expectedMain = self.samplePlantData(self.mainFacility(), forecastdate, forecastsMain, ANY)
        expectedOther = self.samplePlantData(self.otherFacility(), forecastdate, forecastsOther, ANY)

        self.assertEqual(len(plants_data), 2)
        self.assertDictEqual(plants_data[0], expectedMain)
        self.assertDictEqual(plants_data[1], expectedOther)

    def test__insertForecast(self):

        alibaba = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.otherFacility()),
        )

        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)

        forecastMeta = {
            'facilityId': self.mainFacility(),
            'variableId': 'prod',
            'predictorId': 'aggregated',
            'granularity': '60',
            'forecastDate': time,
        }

        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        plants_data = Forecast.meteologicaToPlantsData(forecastMeta, data)

        alibaba.insertPlantData(plants_data)

    def test__lastForecastDownloadDate__NoData(self):

        alibaba = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        forecastDate = alibaba.lastForecastDownloaded()

        self.assertIsNone(forecastDate)

    def test__lastForecastDownloadDate__SomeData(self):

        alibaba = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.otherFacility()),
        )

        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)

        forecastMeta = self.sampleForecastMeta(self.mainFacility(), time)

        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        plants_data = Forecast.meteologicaToPlantsData(forecastMeta, data)

        alibaba.insertPlantData(plants_data[0])

        forecastDate = alibaba.lastForecastDownloaded()

        expectedForecastDate = time

        self.assertIsEqual(forecastDate, expectedForecastDate)

    def test_downloadForecast(self):
        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        fromDate = todt("2020-01-01 00:00:00")
        toDate = todt("2020-01-03 00:00:00")

        with self.createApi() as api:
            for facility, values in data.items():
                resultForecast = api.getForecast(facility, fromDate, toDate)
                self.assertEqual(resultForecast[0][0], todtaware("2020-01-01 00:00:00"))

    # TODO old tests, are they still useful to refactor?

    def _test_downloadForecastToEmptyDB_checkResponses(self):
        configdb = self.createConfig()

        downloadResponses = download_meter_data(configdb, test_env=True)

        self.assertTrue((self.mainFacility(), "OK") in downloadResponses.items())

    def _test_downloadForecastToEmptyDB(self):
        configdb = self.createConfig()

        download_meter_data(configdb, test_env=True)

        forecastDB = {}
        with self.createPlantmonitorDB() as db:
            forecastDB = db.getForecast()

        now = dt.datetime.now(dt.timezone.utc)
        fromDate = now - dt.timedelta(days=14)
        toDate = now
        forecast = []
        with self.createApi() as api:
            forecast = api.getForecast(self.mainFacility(), fromDate, toDate)

        self.assertListEqual(forecastDB[self.mainFacility()], forecast)

    def __test_downloadForecastToEmptyDB_manyfacilities(self):
        configdb = self.createConfig()

        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        fromDate = todt("2020-01-01 00:00:00")
        toDate = todt("2020-01-01 00:00:00")
        forecasts = {}
        with self.createApi() as api:
            for facility in data.keys():
                forecast = api.getForecast(facility, fromDate, toDate)
                forecasts[facility] = forecast

        downloadResponses = download_meter_data(configdb, test_env=True)

        self.assertTrue((self.mainFacility(), "OK") in downloadResponses.items())

        forecastDBData = {}
        with self.createPlantmonitorDB() as db:
            forecastDBData = db.getForecast(self.mainFacility())

        logger.info(forecastDBData)
        logger.info(forecasts)

        self.assertDictEqual(forecastDBData, forecasts[self.mainFacility()])

    def __test_downloadForecastToDB(self):
        configdb = self.createConfig()

        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        fromDate = todt("2020-01-01 00:00:00")
        toDate = todt("2020-01-01 00:00:00")

        with self.createApi() as api:
            for facility in data.keys():
                response = api.getForecast(facility, fromDate, toDate)

        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            tworeadings = {
                self.mainFacility(): [
                    (todt("2040-01-02 00:00:00"), 10),
                    (todt("2040-01-02 01:00:00"), 20),
                ],
            }
            db.addMeterData(tworeadings)

        responses = download_meter_data(configdb, test_env=True)
        self.assertDictEqual({self.mainFacility(): "OK"}, responses)

        # TODO check uploaded content from api if they ever add GetObservations
        with self.createApi() as api:
            self.assertEqual(api.lastDateUploaded(self.mainFacility()), todt("2040-01-02 01:00:00"))

    def __test_downloadForecastToDB_ManyFacilities(self):
        config = self.createConfig()

        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.otherFacility(), '432104321')
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            data = {
                self.mainFacility(): [
                    (todt("2040-01-02 00:00:00"), 10),
                    (todt("2040-01-02 01:00:00"), 20),
                ],
                self.otherFacility(): [
                    (todt("2040-01-02 00:00:00"), 210),
                    (todt("2040-01-02 01:00:00"), 340),
                ],
            }
            db.addMeterData(data)

        responses = download_meter_data(config, test_env=True)

        self.assertDictEqual(
            {self.mainFacility(): "OK", self.otherFacility(): "OK"},
            responses
        )

    def __test_downloadForecastToDB_mismatchFacilities(self):
        config = self.createConfig()

        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.unexistantFacility(), '432104321')
            data = {
                self.unexistantFacility(): [
                    (todt("2040-01-02 00:00:00"), 10),
                    (todt("2040-01-02 01:00:00"), 20),
                ],
            }
            db.addMeterData(data)

        responses = download_meter_data(configdb=config, test_env=True)

        self.assertDictEqual(
            {self.unexistantFacility(): "INVALID_FACILITY_ID: {}".format(self.unexistantFacility())},
            responses
        )
        with self.createApi() as api:
            self.assertEqual(api.lastDateUploaded(self.unexistantFacility()), None)

    def __test_downloadForecastToDB_mixedFacilities(self):
        config = self.createConfig()

        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            db.addFacilityMeterRelation(self.unexistantFacility(), '432104321')
            data = {
                self.mainFacility(): [
                    (todt("2040-01-02 00:00:00"), 50),
                    (todt("2040-01-02 01:00:00"), 70),
                ],
                self.unexistantFacility(): [
                    (todt("2040-01-02 00:00:00"), 10),
                    (todt("2040-01-02 01:00:00"), 20),
                ],
            }
            db.addMeterData(data)

        responses = download_meter_data(configdb=config, test_env=True)

        self.assertDictEqual({
                self.mainFacility(): "OK",
                self.unexistantFacility(): "INVALID_FACILITY_ID: {}".format(self.unexistantFacility())
            },
            responses
        )
        with self.createApi() as api:
            self.assertEqual(api.lastDateUploaded(self.mainFacility()), todt("2040-01-02 01:00:00"))
            self.assertEqual(api.lastDateUploaded(self.unexistantFacility()), None)


# vim: ts=4 sw=4 et
