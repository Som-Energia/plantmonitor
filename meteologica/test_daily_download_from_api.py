import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from meteologica.daily_download_from_api import download_meter_data

from meteologica.meteologica_api_utils import (
    MeteologicaApi_Mock,
    MeteologicaApi,
    MeteologicaApiError,
)

from meteologica.plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBMock,
    PlantmonitorDBError,
)

from meteologica.utils import todt, todtaware


#from django.conf import settings
from yamlns import namespace as ns
from pathlib import Path
from unittest.mock import patch
import unittest
import datetime as dt

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("test")

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

    def createPlantmonitorDB(self):
        configdb = ns.load('conf/configdb_test.yaml')
        return PlantmonitorDB(configdb)

    def createConfig(self):
        return ns.load('conf/configdb_test.yaml')

    def getLastDateFile(self):
        return 'lastDateFile-test.yaml'

    def getLastDateDownloadFile(self):
        return 'lastDateDownloadFile-test.yaml'

    def cleanLastDateFile(self):
        f = Path(self.getLastDateFile())
        if f.exists(): f.unlink()

    def setUp(self):
        config = self.createConfig()
        PlantmonitorDB(config).demoDBsetup(config)
        self.cleanLastDateFile()

    def tearDown(self):
        config = self.createConfig()
        PlantmonitorDB(config).dropDatabase(config)
        self.cleanLastDateFile()

    def mainFacility(self):
        return 'SomEnergia_Picanya'

    def otherFacility(self):
        return 'SomEnergia_Torrefarrera'

    def unexistantFacility(self):
        return "UnexistantPlant"

    def mainMockFacility(self):
        return 'SomEnergia_Amapola'

    def otherMockFacility(self):
        return 'SomEnergia_Tulipan'

    def test_downloadForecast(self):
        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        # meteologica returns NO_FORECASTS if requested date is further than 5 months
        today = dt.date.today()
        fromDate = todt("{}-{}-01 00:00:00".format(today.year, today.month))
        toDate = todt("{}-{}-03 00:00:00".format(today.year, today.month))

        with self.createApi() as api:
            for facility, values in data.items():
                resultForecast = api.getForecast(facility, fromDate, toDate)
                self.assertEqual(resultForecast[0][0], todtaware("{}-{}-01 00:00:00".format(today.year, today.month)))

    def test_downloadForecastToEmptyDB_checkResponses(self):
        configdb = self.createConfig()

        downloadResponses = download_meter_data(configdb, test_env=True)

        self.assertTrue((self.mainFacility(), "OK") in downloadResponses.items())

    def test_downloadForecastToEmptyDB(self):
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

        fromDate = todt("{}-01-01 00:00:00".format(dt.datetime.now().year))
        toDate = todt("{}-01-01 00:00:00".format(dt.datetime.now().year))
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

        fromDate = todt("{}-01-01 00:00:00".format(dt.datetime.now().year))
        toDate = todt("{}-01-01 00:00:00".format(dt.datetime.now().year))

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


unittest.TestCase.__str__ = unittest.TestCase.id


# vim: ts=4 sw=4 et
