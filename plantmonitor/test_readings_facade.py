# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
from unittest.mock import MagicMock

import datetime as dt

from yamlns import namespace as ns

from .readings_facade import ReadingsFacade

from pony import orm

from unittest.mock import patch, MagicMock

from ORM.models import database
from ORM.models import (
    importPlants,
    exportPlants,
    Plant,
    Meter,
    MeterRegistry,
)
from ORM.db_utils import setupDatabase

from meteologica.utils import todtaware

setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)

class ReadingsFacade_Test(unittest.TestCase):

    def setUp(self):
        from conf import envinfo
        self.assertIn(envinfo.SETTINGS_MODULE, ['conf.settings.testing','conf.settings.testing_public'])


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

    def samplePlantsNS(self, altMeter="12345678"):
        alcoleaPlantsNS = ns.loads("""\
            municipalities:
            - municipality:
                name: Figueres
                ineCode: '17066'
                countryCode: ES
                country: Spain
                regionCode: '09'
                region: Catalonia
                provinceCode: '17'
                province: Girona
            - municipality:
                name: Girona
                ineCode: '17079'
            plants:
            - plant:
                name: alcolea
                codename: SCSOM04
                description: la bonica planta
                municipality: '17066'
                meters:
                - meter:
                    name: '{}'
                inverters:
                - inverter:
                    name: '5555'
            - plant:
                name: figuerea
                codename: Som_figuerea
                description: la bonica planta
                municipality: '17079'
                meters:
                - meter:
                    name: '5432'
                - meter:
                    name: '9876'
                inverters:
                - inverter:
                    name: '4444'
                - inverter:
                    name: '2222'
                irradiationSensors:
                - irradiationSensor:
                    name: oriol
                temperatureModuleSensors:
                - temperatureModuleSensor:
                    name: joan
                temperatureAmbientSensors:
                - temperatureAmbientSensor:
                    name: benjami
                integratedSensors:
                - integratedSensor:
                    name: david""".format(altMeter))
        return alcoleaPlantsNS

    def samplePlantsData(self, time, dt, altMeter="12345678"):
        plantsData = [
              {
              "plant": "alcolea",
              "devices":
                sorted([{
                    "id": "Meter:{}".format(altMeter),
                    "readings": [{
                        "time": time,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                },
                {
                    "id": "Inverter:5555",
                    "readings": [{
                        "time": time,
                        "power_w": 156,
                        "energy_wh": 154,
                        "intensity_cc_mA": 222,
                        "intensity_ca_mA": 500,
                        "voltage_cc_mV": 200,
                        "voltage_ca_mV": 100,
                        "uptime_h": 15,
                        "temperature_dc": 170,
                    }]
                }],key=lambda d: d['id']),
              },
              {
              "plant": "figuerea",
              "devices":
                sorted([{
                    "id": "Meter:9876",
                    "readings": [{
                        "time": time,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                },
                {
                    "id": "Meter:5432",
                    "readings": [{
                        "time": time,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                }],key=lambda d: d['id']),
            }]
        return plantsData

    def samplePlantsData_DifferentTimes(self, basetime, differenttime, altMeter='12345678'):
        plantsData = [
              {
              "plant": "alcolea",
              "devices":
                sorted([{
                    "id": "Meter:{}".format(altMeter),
                    "readings": [{
                        "time": differenttime,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                },
                {
                    "id": "Inverter:5555",
                    "readings": [{
                        "time": basetime,
                        "power_w": 156,
                        "energy_wh": 154,
                        "intensity_cc_mA": 222,
                        "intensity_ca_mA": 500,
                        "voltage_cc_mV": 200,
                        "voltage_ca_mV": 100,
                        "uptime_h": 15,
                        "temperature_dc": 170,
                    }]
                }],key=lambda d: d['id']),
              },
              {
              "plant": "figuerea",
              "devices":
                sorted([{
                    "id": "Meter:9876",
                    "readings": [{
                        "time": basetime,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                },
                {
                    "id": "Meter:5432",
                    "readings": [{
                        "time": basetime,
                        "export_energy_wh": 1,
                        "import_energy_wh": 123,
                        "r1_VArh": 1234,
                        "r2_VArh": 124,
                        "r3_VArh": 1234,
                        "r4_VArh": 124,
                    }],
                }],key=lambda d: d['id']),
            }]
        return plantsData

    def setupPlants(self, time, alternativeMeter="12345678", plantsData = None):
        delta = dt.timedelta(minutes=30)
        plantsns = self.samplePlantsNS(altMeter=alternativeMeter)
        importPlants(plantsns)
        plantsData = plantsData or self.samplePlantsData(time, delta, altMeter=alternativeMeter)
        Plant.insertPlantsData(plantsData)
        orm.flush()

    def test_getNewMetersReadings__noMeterInERP(self):
        time = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)

        self.setupPlants(time)

        mock = MagicMock(side_effect=self.mockMeterReadingsSideEffects)
        with patch('plantmonitor.readings_facade.ReadingsFacade.measuresFromDate', mock):
            r = ReadingsFacade()
            r.erpMeters = []
            plantsData = r.getNewMetersReadings()

        # orm returns unordered
        for plantdata in plantsData:
            plantdata["devices"].sort(key=lambda d: d['id'])

        expectedPlantsData = [
              {
              "plant": "alcolea",
              "devices": [],
              },
              {
              "plant": "figuerea",
              "devices": [],
            }]

        self.assertListEqual(expectedPlantsData, plantsData)

    # TODO defragilize this tests via fake or mock
    def test_getNewMetersReadings__meterInERP_OldReadings(self):
        lastReadingTime = dt.datetime(2020, 12, 10, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        erpMeter = "88300864" # removed meter
        self.setupPlants(lastReadingTime, erpMeter)

        r = ReadingsFacade()

        plantsData = r.getNewMetersReadings()

        # orm returns unordered
        for plantdata in plantsData:
            plantdata["devices"].sort(key=lambda d: d['id'])

        expectedPlantsData = sorted([
              {
              "plant": "alcolea",
              "devices":
                [{
                    "id": "Meter:88300864",
                    "readings": [],
                }],
              },
              {
              "plant": "figuerea",
              "devices": [],
            }], key=lambda d: d['plant'])

        self.assertListEqual(expectedPlantsData, plantsData)

    def test_getNewMetersReadings__meterInERPmanyReadings(self):
        lastReadingTime = dt.datetime(2019, 10, 1, 15, 5, 10, 588861, tzinfo=dt.timezone.utc)
        erpMeter = "88300864"
        self.setupPlants(lastReadingTime, erpMeter)

        r = ReadingsFacade()

        upto = dt.datetime(2019, 10, 1, 17, 5, 10, 588861, tzinfo=dt.timezone.utc)
        plantsData = r.getNewMetersReadings(upto)

        # orm returns unordered
        for plantdata in plantsData:
            plantdata["devices"].sort(key=lambda d: d['id'])

        expectedPlantsData = [
              {
              "plant": "alcolea",
              "devices":
                [{
                    "id": "Meter:{}".format(erpMeter),
                    "readings": [{
                        "time": dt.datetime(2019,10,1,16,00,00, tzinfo=dt.timezone.utc),
                        "export_energy_wh": 1139000,
                        "import_energy_wh": 0,
                        "r1_VArh": 0,
                        "r2_VArh": 1000,
                        "r3_VArh": 2000,
                        "r4_VArh": 0,
                    },
                    {
                        "time": dt.datetime(2019,10,1,17,00,00, tzinfo=dt.timezone.utc),
                        "export_energy_wh": 568000,
                        "import_energy_wh": 0,
                        "r1_VArh": 0,
                        "r2_VArh": 0,
                        "r3_VArh": 8000,
                        "r4_VArh": 0,
                    }]
                }],
              },
              {
              "plant": "figuerea",
              "devices": [],
            }]

        self.assertListEqual(expectedPlantsData, plantsData)

    def _test_transfer_ERP_readings_to_model(self):
        r = ReadingsFacade()
        r.erpMeters = ['12345678']

        # TODO mock measures or fake meters
        r.transfer_ERP_readings_to_model(refreshERPmeters=False)

    def test__warnNewMeters(self):
        erpMeter = "88300864"
        self.setupPlants(dt.datetime.now(tz=dt.timezone.utc), erpMeter)

        r = ReadingsFacade()
        r.erpMeters = ['3141519']
        newMeters = r.warnNewMeters()
        expectedNewMeters = ['3141519']

        self.assertListEqual(newMeters, expectedNewMeters)

    def test_checkNewMeters(self):
        erpMeter = "88300864"
        self.setupPlants(dt.datetime.now(tz=dt.timezone.utc), erpMeter)
        newMeter = "3141519"

        meterNames = [erpMeter, newMeter]

        r = ReadingsFacade()
        newMeters = r.checkNewMeters(meterNames)

        self.assertListEqual(newMeters, [newMeter])

    def test__refreshERPmeters__mockTelemeasure(self):

        erpMeter = '88300864'
        self.setupPlants(dt.datetime.now(tz=dt.timezone.utc), erpMeter)

        erpMeters = ['12345678', '87654321']

        # mock telemeasure_meters_name
        r = ReadingsFacade()
        r.telemeasureMetersNames = MagicMock(return_value=['12345678', '87654321'])
        r.refreshERPmeters()
        newMeters = r.erpMeters

        self.assertListEqual(newMeters, erpMeters)

    # TODO this test is just to remind us that inputting timestamp instead of timestamptz
    # in an entity causes the obscure pony.orm.core.UnrepeatableReadError: Phantom object.
    # times should always have dt.datetime.now(tz=dt.timezone.utc)
    def test__setupPlants__registries(self):

        erpMeter = '88300864'
        self.setupPlants(dt.datetime.now(), erpMeter)

        with self.assertRaises(orm.core.UnrepeatableReadError):
            MeterRegistry.select().show()

    def _test__getDeviceReadings__differentLastDates(self):

        erpMeter = '88300864'
        self.setupPlants(dt.datetime.now(tz=dt.timezone.utc), erpMeter)

        plant = Plant.get(name='alcolea')
        meter = Meter.get(plant=plant, name=erpMeter)
        print(meter.to_dict())
        self.assertIsNotNone(meter)
        MeterRegistry.select().show()

        lastDate = meter.getLastReadingDate()
        # mock telemeasure_meters_name

        erp_meters = ['12345678', '87654321']

        # mockreturn = [(
        #         measure['utc_timestamp'],
        #         int(measure['ae']),
        #         int(measure['ai']),
        #         int(measure['r1']),
        #         int(measure['r2']),
        #         int(measure['r3']),
        #         int(measure['r4']),
        #     )
        #     for measure in sorted(measures,
        #         key=lambda x: x['utc_timestamp'])
        # ]
        mock = MagicMock(return_value=['12345678', '87654321'])
        with patch('plantmonitor.readings_facade.ReadingsFacade.measuresFromDate', mock):
            r = ReadingsFacade()
            newMeters = r.getDeviceReadings(meterName=erpMeter, lastDate=lastDate, upto=None)

        self.assertListEqual(newMeters, erp_meters)

    def assertPlantsData(self, plants_data, meterReadings):
        # print("meterreadings\n{}\n mock\n{}\n".format(plants_data, meterReadings))

        plants_data_meters = sorted([device['id'].split(':')[1] for plant_data in plants_data for device in plant_data['devices']])
        meters = sorted(meterReadings.keys())
        self.assertListEqual(plants_data_meters, meters)

        for plant_data in plants_data:
            for device in plant_data['devices']:
                meterName = device['id'].split(':')[1]
                data = (meterName, meterReadings[meterName])
                expected = (meterName, [
                        (
                            reading['time'].strftime("%Y-%m-%d %H:%M:%S"),
                            reading['export_energy_wh']/1000.0,
                            0/1000.0,0,0,0,0
                        ) for reading in device['readings']
                    ])
                self.assertTupleEqual(data, expected)


    # def assertMockReadings(self, plants_data, mock_readings):
    #     readings = [
    #         self.assertListEqual(
    #             mock_readings,
    #             [(
    #                 reading['time'].strftime("%Y-%m-%d %H:%M:%S"),
    #                 reading['export_energy_wh'],
    #                 0,0,0,0,0
    #             ) for reading in device['readings']]
    #         )
    #         for plant_data in plants_data for device in plant_data['devices']
    #     ]

    def mock_meter_readings(self, time, random=True):
        import random

        def toUTCstr(dtaware):
            return dtaware.astimezone(tz=dt.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

        time.replace(minute=0,second=0,microsecond=0)
        delta = dt.timedelta(hours=1)

        values = [0]*8 + [11, 80, 366, 716, 991, 1182, 1293, 1315, 1268, 1132, 894, 609, 323, 104, 1, 0]
        i = 0
        while True:
            # dr = random.randint(30,70) if random else 0
            dr = 0
            yield (toUTCstr(time + i*delta), values[(time.hour+i)%24] + dr,0,0,0,0,0)
            i = i + 1

    def setupReadingsDB(self, time, readingsDict=None):
        def toUTCstr(dtaware):
            return dtaware.astimezone(tz=dt.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

        h = dt.timedelta(hours=1)
        self.readingsDB = readingsDict or {
            '12345678': [
                (toUTCstr(time+0*h), 11,0,0,0,0,0),
                (toUTCstr(time+1*h), 11,0,0,0,0,0),
                (toUTCstr(time+2*h), 80,0,0,0,0,0),
                (toUTCstr(time+3*h), 366,0,0,0,0,0),
            ],
            '1234': [
                (toUTCstr(time+0*h), 11,0,0,0,0,0),
                (toUTCstr(time+1*h), 11,0,0,0,0,0),
                (toUTCstr(time+2*h), 80,0,0,0,0,0),
                (toUTCstr(time+3*h), 366,0,0,0,0,0),
            ],
            '9876': [
                (toUTCstr(time+0*h), 11,0,0,0,0,0),
                (toUTCstr(time+1*h), 11,0,0,0,0,0),
                (toUTCstr(time+2*h), 80,0,0,0,0,0),
                (toUTCstr(time+3*h), 366,0,0,0,0,0),
            ],
            '5432': [
                (toUTCstr(time+0*h), 11,0,0,0,0,0),
                (toUTCstr(time+1*h), 11,0,0,0,0,0),
                (toUTCstr(time+2*h), 80,0,0,0,0,0),
                (toUTCstr(time+3*h), 366,0,0,0,0,0),
            ]
        }

    def mockMeterReadingsSideEffects(self, meterName, beyond, upto):
        if not beyond:
            return [r for r in self.readingsDB[meterName] if todtaware(r[0]) < upto]
        else:
            return [r for r in self.readingsDB[meterName] if beyond < todtaware(r[0]) and todtaware(r[0]) < upto]

    def test__mock_meter_readings(self):
        time = dt.datetime(2019,10,1,16,00,00, tzinfo=dt.timezone.utc)
        readingGenerator = self.mock_meter_readings(time, random=False)
        readings = list(next(readingGenerator) for i in range(4))

        self.assertTrue(readings)

    def setupMockReadings(self, erpMeters, meterTimes):

        readingGenerators = {
            'Meter:{}'.format(name): self.mock_meter_readings(meterTime, random=False)
            for name, meterTime in zip(erpMeters, meterTimes)
        }
        mockMeterReadings = {
            id:list(next(readingGenerator) for i in range(4))
            for id, readingGenerator in readingGenerators.items()
        }
        mockSideEffect = [
            readings for id, readings in mockMeterReadings.items()
        ]

        return mockMeterReadings, mockSideEffect

    def test__getLastReadingDate(self):

        erpMeter = '12345678'
        time = dt.datetime.now(tz=dt.timezone.utc)
        self.setupPlants(time, erpMeter)

        lastDates = [meter.getLastReadingDate() for meter in Meter.select()]

        self.assertListEqual(lastDates, [time]*len(lastDates))

    def test__transferERPreadingsToModel__manyMetersSameDates_ManyNewReadings(self):

        erpMeter = '12345678'
        ormtime = dt.datetime(2019,10,1,16,00,00, tzinfo=dt.timezone.utc)
        erptime = dt.datetime(2020,10,1,16,00,00, tzinfo=dt.timezone.utc)
        self.setupPlants(ormtime, erpMeter)
        self.setupReadingsDB(erptime)

        erpMeters = ['12345678', '1234', '9876', '5432']

        mock = MagicMock(side_effect=self.mockMeterReadingsSideEffects)
        with patch('plantmonitor.readings_facade.ReadingsFacade.measuresFromDate', mock):
            r = ReadingsFacade()
            r.erpMeters = erpMeters
            plantsData = r.getNewMetersReadings()

        expectedErpMeters = ['12345678', '9876', '5432']
        expectedMeterReadings = {k:v for k,v in self.readingsDB.items() if k in expectedErpMeters}
        self.assertPlantsData(plantsData, expectedMeterReadings)

    def test__transferERPreadingsToModel__manyMetersDifferentDates_OneWithoutNewReadings(self):

        erpMeter = '12345678'
        obsoleteTime = dt.datetime(2020,10,1,16,00,00, tzinfo=dt.timezone.utc)
        updatedTime = dt.datetime(2021,4,1,16,00,00, tzinfo=dt.timezone.utc)
        erpTime = dt.datetime(2021,3,1,16,00,00, tzinfo=dt.timezone.utc)
        plantsData_different = self.samplePlantsData_DifferentTimes(basetime=obsoleteTime, differenttime=updatedTime)
        self.setupPlants(alternativeMeter=erpMeter, time=None, plantsData=plantsData_different)
        self.setupReadingsDB(erpTime)
        erpMeters = [erpMeter, '1234', '9876', '5432']

        mock = MagicMock(side_effect=self.mockMeterReadingsSideEffects)
        with patch('plantmonitor.readings_facade.ReadingsFacade.measuresFromDate', mock):
            r = ReadingsFacade()
            r.erpMeters = erpMeters
            plants_data = r.getNewMetersReadings()

        expectedMeters = [erpMeter, '9876', '5432']
        expectedMeterReadings = {k:v for k,v in self.readingsDB.items() if k in expectedMeters}
        expectedMeterReadings[erpMeter] = []
        self.assertPlantsData(plants_data, expectedMeterReadings)

    def test__transferERPreadingsToModel__ormMetersNotInERP(self):
        erpMeter = '12345678'
        ormtime = dt.datetime(2019,10,1,16,00,00, tzinfo=dt.timezone.utc)
        self.setupPlants(ormtime, alternativeMeter=erpMeter)
        erptime = dt.datetime(2018,10,1,16,00,00, tzinfo=dt.timezone.utc)
        self.setupReadingsDB(erptime)

        erpMeters = ['1234', '9876'] # missing erpMeter and 5432

        mock = MagicMock(side_effect=self.mockMeterReadingsSideEffects)
        with patch('plantmonitor.readings_facade.ReadingsFacade.measuresFromDate', mock):
            r = ReadingsFacade()
            r.erpMeters = erpMeters
            plants_data = r.getNewMetersReadings()

        expectedPlantsData = {'9876':[]}
        self.assertPlantsData(plants_data, expectedPlantsData)

    def test__transferERPreadingsToModel__ormEmpty(self):
        erpMeter = '12345678'
        emptyPlantsData = [{
              "plant": "alcolea",
              "devices": []
        }]
        self.setupPlants(None, alternativeMeter=erpMeter, plantsData=emptyPlantsData)
        erptime = dt.datetime(2020,10,1,16,00,00, tzinfo=dt.timezone.utc)
        self.setupReadingsDB(erptime)

        erpMeters = ['12345678', '1234', '9876', '5432']

        mock = MagicMock(side_effect=self.mockMeterReadingsSideEffects)
        with patch('plantmonitor.readings_facade.ReadingsFacade.measuresFromDate', mock):
            r = ReadingsFacade()
            r.erpMeters = erpMeters
            plants_data = r.getNewMetersReadings()

        expectedErpMeters = ['12345678', '9876', '5432']
        expectedPlantsData = {k:v for k,v in self.readingsDB.items() if k in expectedErpMeters}
        self.assertPlantsData(plants_data, expectedPlantsData)

    def _test__transferERPreadingsToModel__differentLastDates(self):

        erpMeter = '12345678'
        self.setupPlants(dt.datetime.now(tz=dt.timezone.utc), erpMeter)

        erpMeters = ['12345678', '87654321']

        lastDate = dt.datetime(2019,10,1,16,00,00, tzinfo=dt.timezone.utc)

        mockreturn = [(
                measure['utc_timestamp'],
                int(measure['ae']),
                int(measure['ai']),
                int(measure['r1']),
                int(measure['r2']),
                int(measure['r3']),
                int(measure['r4']),
            )
            for measure in sorted(measures,
                key=lambda x: x['utc_timestamp'])
        ]

        mock = MagicMock(return_value=['12345678', '87654321'])
        with patch('plantmonitor.readings_facade.ReadingsFacade.measuresFromDate', mock):
            r = ReadingsFacade()
            r.erpMeters = erpMeters
            newMeters = r.transfer_ERP_readings_to_model(refreshERPmeters=False)

        self.assertListEqual(newMeters, erpMeters)
