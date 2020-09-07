from meteologica.daily_upload_to_api import upload_meter_data

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

from meteologica.utils import todt

#from django.conf import settings
from yamlns import namespace as ns
from pathlib import Path
from unittest.mock import patch
import unittest

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("test")


class DailyUpload_Test(unittest.TestCase):

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

    def test_createDB(self):
        with self.createPlantmonitorDB() as db:
            result = db.getMeterData()
            self.assertEqual(result, {})

    def test_facilitiesExist(self):
        api = self.createApi()
        facilities = []
        with api:
            facilities = api.getAllFacilities()
        self.assertIn(self.mainFacility(), facilities)
        self.assertIn(self.otherFacility(), facilities)

    def test_import(self):
        config = self.createConfig()
        upload_meter_data(configdb=config, test_env=True)

    def test_uploadProduction(self):
        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        with self.createApi() as api:
            for facility, values in data.items():
                response = api.uploadProduction(facility, values)
                self.assertEqual(response, "OK")

    def test_uploadProductionFromDB(self):
        configdb = self.createConfig()

        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            tworeadings = {
                self.mainFacility(): [
                    (todt("2040-01-02 00:00:00"), 10),
                    (todt("2040-01-02 01:00:00"), 20),
                ],
            }
            db.addMeterData(tworeadings)

        responses = upload_meter_data(configdb, test_env=True)
        self.assertDictEqual({self.mainFacility(): "OK"}, responses)

        # TODO check uploaded content from api if they ever add GetObservations
        with self.createApi() as api:
            self.assertEqual(api.lastDateUploaded(self.mainFacility()), todt("2040-01-02 01:00:00"))

    def test_uploadProductionFromDB_ExcludedFacilities(self):
        config = self.createConfig()

        #change this once the erp hourly-quarterly is fixed
        excludedFacility = 'SCSOM06'

        with self.createPlantmonitorDB() as db:
            db.addFacilityMeterRelation(excludedFacility, '432104321')
            db.addFacilityMeterRelation(self.mainFacility(), '123401234')
            data = {
                self.mainFacility(): [
                    (todt("2040-01-02 00:00:00"), 10),
                    (todt("2040-01-02 01:00:00"), 20),
                ],
                excludedFacility: [
                    (todt("2040-01-02 00:00:00"), 210),
                    (todt("2040-01-02 01:00:00"), 340),
                ],
            }
            db.addMeterData(data)

        responses = upload_meter_data(config, test_env=True)

        self.assertDictEqual(
            {self.mainFacility(): "OK"},
            responses
        )

    def test_uploadProductionFromDB_ManyFacilities(self):
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

        responses = upload_meter_data(config, test_env=True)

        self.assertDictEqual(
            {self.mainFacility(): "OK", self.otherFacility(): "OK"},
            responses
        )

    def test_uploadProduction_mismatchFacilities(self):
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

        responses = upload_meter_data(configdb=config, test_env=True)

        self.assertDictEqual(
            {self.unexistantFacility(): "INVALID_FACILITY_ID: {}".format(self.unexistantFacility())},
            responses
        )
        with self.createApi() as api:
            self.assertEqual(api.lastDateUploaded(self.unexistantFacility()), None)

    def test_uploadProduction_mixedFacilities(self):
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

        responses = upload_meter_data(configdb=config, test_env=True)

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
