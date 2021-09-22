import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from meteologica.daily_download_from_api import download_meter_data

from meteologica.forecasts import (
    downloadMeterForecasts,
    ForecastStatus,
    getMeterReadings,
    getMeterReadingsFromLastUpload,
    uploadFacilityMeterReadings,
    uploadMeterReadings
)

from meteologica.meteologica_api_utils import (
    MeteologicaApi_Mock,
    MeteologicaApi,
    MeteologicaApiError,
)

from meteologica.utils import todt, todtaware

from pony import orm
from ORM.db_utils import setupDatabase

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

from meteologica.plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBMock,
    PlantmonitorDBError,
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

    def cleanLastDateFile(self):
        f = Path(self.getLastDateFile())
        if f.exists(): f.unlink()

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
        # comment this if you want to check the date format
        self.cleanLastDateFile()

    def mainFacility(self):
        return 'Alcolea'

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

    def createPlantmonitorDB(self):
        configdb = ns.load('conf/configdb_test.yaml')
        return PlantmonitorDB(configdb)

    def test__createForecast__OnePlant(self):
        alcolea = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)

        forecastMeta = {
            'variable': 'prod',
            'predictor': 'aggregated',
            'granularity': '60',
            'forecastdate': time,
            'errorcode': '',
        }

        forecastData = {
            self.mainFacility(): [
                (todtaware("2040-01-02 00:00:00"), 10),
                (todtaware("2040-01-02 01:00:00"), 20),
            ],
        }

        forecastMetadata = ForecastMetadata.create(plant=alcolea, **forecastMeta)

        self.assertIsNotNone(forecastMetadata)

        for f in forecastData[self.mainFacility()]:
            forecastMetadata.insertForecast(
                time=f[0],
                percentil10=None,
                percentil50=f[1],
                percentil90=None,
            )
        orm.flush()

        expected = [{
                'forecastMetadata': 1,
                'percentil10': None,
                'percentil50': 10,
                'percentil90': None,
                'time': datetime.datetime(2040, 1, 2, 0, 0, tzinfo=datetime.timezone.utc)
            },
            {
                'forecastMetadata': 1,
                'percentil10': None,
                'percentil50': 20,
                'percentil90': None,
                'time': datetime.datetime(2040, 1, 2, 1, 0, tzinfo=datetime.timezone.utc)
            }
        ]
        forecasts = [f.to_dict() for f in Forecast.select()]
        self.assertListEqual(forecasts, expected)

    def test__lastForecastDownloadDate__NoData(self):

        alibaba = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        forecastDate = alibaba.lastForecastDownloaded()

        self.assertIsNone(forecastDate)

    def test__lastForecastDownloadDate__SomeData(self):

        alcolea = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)

        forecastMeta = self.sampleForecastMeta(self.mainFacility(), time)

        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        status = 'OK'
        forecastDate = time

        forecastMetadata = ForecastMetadata.create(plant=alcolea, forecastdate=forecastDate, errorcode=status)
        forecastMetadata.addForecasts(data[self.mainFacility()])

        orm.flush()

        forecastDate = alcolea.lastForecastDownloaded()

        expectedForecastDate = time

        self.assertEqual(forecastDate, expectedForecastDate)

    def test_getForecast(self):
        data = {
            "SomEnergia_{}".format(self.mainFacility()): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        fromDate = todt("{}-01-01 00:00:00".format(datetime.datetime.now().year))
        toDate = todt("{}-01-03 00:00:00".format(datetime.datetime.now().year))

        with self.createApi() as api:
            for facility, values in data.items():
                resultForecast = api.getForecast(facility, fromDate, toDate)
                self.assertEqual(resultForecast[0][0], todtaware("{}-01-01 00:00:00".format(datetime.datetime.now().year)))

    def test__downloadMeterForecasts__uptodate(self):

        alcolea = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        forecastDate = datetime.datetime.now(datetime.timezone.utc)
        oldForecastMetadata = ForecastMetadata.create(
            plant=alcolea,
            forecastdate=forecastDate,
            errorcode='OK'
        )

        p50 = 1000
        time = datetime.datetime.now(datetime.timezone.utc)
        oldForecastMetadata.insertForecast(percentil10=None, percentil50=p50, percentil90=None, time=time)
        orm.flush()

        statuses = downloadMeterForecasts(self.createConfig())

        forecastMetadata = ForecastMetadata.select().order_by(orm.desc(ForecastMetadata.forecastdate)).first()

        self.assertEqual(forecastMetadata, oldForecastMetadata)
        forecasts = forecastMetadata.registries

        self.assertEqual(statuses['SomEnergia_Alcolea'], ForecastStatus.UPTODATE)

    def test__downloadMeterForecasts__newForecasts(self):

        alcolea = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = datetime.datetime.now(datetime.timezone.utc)
        forecastDate = time - datetime.timedelta(hours=1)
        oldForecastMetadata = ForecastMetadata.create(
            plant=alcolea,
            forecastdate=forecastDate,
            errorcode='OK'
        )

        p50 = 1000

        oldForecastMetadata.insertForecast(percentil10=None, percentil50=p50, percentil90=None, time=time)
        orm.flush()

        statuses = downloadMeterForecasts(self.createConfig())

        forecastMetadata = ForecastMetadata.select().order_by(orm.desc(ForecastMetadata.forecastdate)).first()

        self.assertNotEqual(forecastMetadata, oldForecastMetadata)
        self.assertEqual(statuses['SomEnergia_Alcolea'], ForecastStatus.OK)
        forecasts = orm.select(f for f in forecastMetadata.registries).order_by(lambda: f.time)[:]
        print(forecasts)
        expectedTime = time.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(days=14, hours=1)
        expectedNumForecasts = 14*24+1
        self.assertEqual(len(forecasts), expectedNumForecasts)
        self.assertEqual(forecasts[-1].time, expectedTime)

    def test__getMeterReadings__None(self):

        plant = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        Meter(plant=plant, name="1234")

        orm.flush()

        with orm.db_session:
            data = getMeterReadings(plant.codename)

        self.assertEqual(data, [])

    def test__getMeterReadings__UnexistingFacility(self):
        with orm.db_session:
            data = getMeterReadings(facility="fake")

        # TODO return None or [] ?
        self.assertEqual(data, None)

    def test__getMeterReadings(self):

        plant = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = todtaware('2020-01-01 00:00:00')

        meter = Meter(plant=plant, name='1234')

        export_energy_wh = 210

        meter.insertRegistry(
            time=time,
            export_energy_wh=export_energy_wh,
            import_energy_wh=0,
            r1_VArh=0,
            r2_VArh=0,
            r3_VArh=0,
            r4_VArh=0
        )

        orm.flush()

        expected = [
            (todtaware('2020-01-01 00:00:00'), 210),
        ]

        with orm.db_session:
            result = getMeterReadings(plant.codename)

        self.assertEqual(expected, result)

    def test__getMeterReadingsFromLastUpload__noMeter(self):

        plant = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = todtaware('2020-01-01 00:00:00')

        orm.flush()

        with orm.db_session:
            result = getMeterReadings(plant.codename)

        self.assertIsNone(result)

    def test__getMeterReadingsFromLastUpload__None(self):

        plant = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = todtaware('2020-01-01 00:00:00')

        meter = Meter(plant=plant, name='1234')

        orm.flush()

        with orm.db_session:
            result = getMeterReadings(plant.codename)

        self.assertEqual([], result)

    def test__getMeterReadingsFromLastUpload__OneReading(self):

        plant = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = todtaware('2020-01-01 00:00:00')

        meter = Meter(plant=plant, name='1234')

        export_energy_wh = 210

        meter.insertRegistry(
            time=time,
            export_energy_wh=export_energy_wh,
            import_energy_wh=0,
            r1_VArh=0,
            r2_VArh=0,
            r3_VArh=0,
            r4_VArh=0
        )

        orm.flush()

        expected = [
            (todtaware('2020-01-01 00:00:00'), 210),
        ]

        with orm.db_session:
            result = getMeterReadings(plant.codename)

        self.assertEqual(expected, result)

    def test__uploadFacilityMeterReadings__checkResponses(self):

        plant = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = todtaware('2020-01-01 00:00:00')

        meter = Meter(plant=plant, name='1234')

        export_energy_wh = 210

        meter.insertRegistry(
            time=time,
            export_energy_wh=export_energy_wh,
            import_energy_wh=0,
            r1_VArh=0,
            r2_VArh=0,
            r3_VArh=0,
            r4_VArh=0
        )

        orm.flush()

        with self.createApi() as api:
            response = uploadFacilityMeterReadings(api, plant.codename)

        expectedResponse = 'OK'
        self.assertEqual(response, expectedResponse)

    def test__uploadMeterReadings__checkResponses(self):

        plant = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = todtaware('2020-01-01 00:00:00')

        meter = Meter(plant=plant, name='1234')

        export_energy_wh = 210

        meter.insertRegistry(
            time=time,
            export_energy_wh=export_energy_wh,
            import_energy_wh=0,
            r1_VArh=0,
            r2_VArh=0,
            r3_VArh=0,
            r4_VArh=0
        )

        orm.flush()

        with self.createApi() as api:
            responses = uploadMeterReadings(self.createConfig())

        expectedResponse = 'OK'
        self.assertEqual(responses[plant.codename], expectedResponse)

    def test__lastForecastDownloaded__checkLastDateAdded(self):

        alcolea = Plant(
            name=self.mainFacility(),
            codename='SomEnergia_{}'.format(self.mainFacility()),
        )

        time = datetime.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        time2 = datetime.datetime(2020, 12, 10, 16, 5, 10, 588861, tzinfo=datetime.timezone.utc)

        data = {
            self.mainFacility(): [
                (todt("2040-01-02 00:00:00"), 10),
                (todt("2040-01-02 01:00:00"), 20),
            ],
        }

        data2 = {
            self.mainFacility(): [
                (todt("2040-01-02 02:00:00"), 30),
                (todt("2040-01-02 03:00:00"), 40),
            ],
        }

        status = 'OK'
        forecastDate = time
        forecastDate2 = time2

        forecastMetadata = ForecastMetadata.create(
            plant=alcolea, forecastdate=forecastDate, errorcode=status)
        forecastMetadata = ForecastMetadata.create(
            plant=alcolea, forecastdate=forecastDate2, errorcode=status)

        forecastMetadata.addForecasts(data[self.mainFacility()])
        forecastMetadata.addForecasts(data2[self.mainFacility()])

        orm.flush()

        forecastDate = alcolea.lastForecastDownloaded()

        expectedForecastDate = time2

        self.assertEqual(forecastDate, expectedForecastDate)

    def __test_downloadForecast_setup(self):

        forecastMeta = self.sampleForecastMeta(self.mainFacility(), time)

        status = 'OK'
        forecastDate = time

        forecastMetadata = ForecastMetadata.create(plant=alcolea, forecastdate=forecastDate, errorcode=status)
        forecastMetadata.addForecasts(data[self.mainFacility()])

        orm.flush()

        expected = [
            (todtaware('2020-01-01 00:00:00'), 210),
            (todtaware('2020-01-01 01:00:00'), 320),
            (todtaware('2020-01-01 02:00:00'), 230),
            (todtaware('2020-01-01 03:00:00'), 340),
        ]


    # TODO old tests, are they still useful to refactor?

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
