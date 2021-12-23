import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from unittest import TestCase
from ORM.models import (
    database,
    Plant,
    SensorIrradiation,
    SensorIrradiationRegistry,
    PlantModuleParameters,
)
from pony import orm
from pathlib import Path
from plantmonitor.operations import integrateExpectedPower
from yamlns import namespace as ns
import datetime

from meteologica.utils import todtaware
from ORM.db_utils import setupDatabase
setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)

class Queries_Test(TestCase):
    from b2btest.b2btest import assertB2BEqual

    @classmethod
    def setUpClass(cls):
        ''

    def setUp(self):
        self.maxDiff=None
        self.b2bdatapath='b2bdata'
        from conf import envinfo
        self.assertIn(envinfo.SETTINGS_MODULE, ['conf.settings.testing','conf.settings.testing_public'])

        orm.rollback()
        database.drop_all_tables(with_all_data=True)

        #orm.set_sql_debug(True)
        database.create_tables()

        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()

    def samplePlantNS(self): # Copied from test_models.py:samplePlantNS
        alcoleaPlantNS = ns.loads("""\
            name: myplant
            codename: PLANTCODE
            description: Plant Description
            meters:
            - meter:
                name: '1234578'
            irradiationSensors:
            - irradiationSensor:
                name: irradiationSensor1
        """)
        return alcoleaPlantNS

    # irradiance exported with
    # `\copy (select to_char(time at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS'), irradiation_w_m2
    # from sensorirradiationregistry order by time desc) to '/tmp/irradiance-2021-07-21-Alcolea.csv' with csv;`

    def readTimeseriesCSV(self, csv_filepath):
        with open(csv_filepath) as csv_file:
          content = csv_file.read()
          timeseries = [
            (
            # TODO this is wrong! astimezone converts the naive to system timezone before going utc
            # use pytz cettz.localize instead
              datetime.datetime.fromisoformat(l[0]).astimezone(datetime.timezone.utc),
              int(l[1])
            ) for l in (r.split(',') for r in content.split('\n') if r)
          ]

        return timeseries

    def setupPlant(self):
        plantDefinition = self.samplePlantNS()
        plant = Plant(
            name=plantDefinition.name,
            codename=plantDefinition.codename,
        )
        plant.importPlant(plantDefinition)
        plant.flush()
        self.plant = plant.id
        self.sensor = SensorIrradiation.select().first().id

    def importData(self, sensor, filename):
        irradianceTimeSeries = self.readTimeseriesCSV(filename)
        for time, irradiation_w_m2 in irradianceTimeSeries:
            SensorIrradiationRegistry(
                sensor=sensor,
                time=time,
                irradiation_w_m2=int(round(irradiation_w_m2)),
                temperature_dc=0,
            )
        database.flush()

        return [
            (time, self.plant, sensor, irradiation_w_m2)
            for time, irradiation_w_m2 in irradianceTimeSeries
        ]

    def assertOutputB2B(self, result):
        result = "\n".join((
            "{}, {}, {:.9f}".format(time.isoformat() if time else None, sensor or int('nan'), irradiation or float('nan'))
            for time, sensor, irradiation in result
        ))
        self.assertB2BEqual(result)

    # TODO which timezone is the csv data we have there? We're assuming Europe Madrid
    def test__naive_timezone_conversion(self):

        # The correct way of setting a timezone to a naive datetime
        foo = datetime.datetime.fromisoformat("2021-06-01T00:00:06.773")
        import pytz
        cettz = pytz.timezone("Europe/Madrid")
        boo = cettz.localize(foo) # naive to aware CET
        goo = boo.astimezone(datetime.timezone.utc) # aware CET to utc
        time = "2021-05-31T22:00:06.773000+00:00"
        print(foo)
        print(boo)
        print(goo)
        self.assertEqual(goo.isoformat(), time)

        # foo = datetime.datetime.fromisoformat("2021-06-01T00:00:06.773")
        # print(foo)
        # import pytz
        # cettz = pytz.timezone("Europe/Madrid")
        # goo = foo.astimezone(cettz)
        # print(goo)
        # gooUTC = goo.astimezone(datetime.timezone.utc)
        # CETtime = "2021-05-31T22:00:06.773000+00:00"
        # self.assertEqual(gooUTC.isoformat(), CETtime)
        # boo = foo.astimezone(datetime.timezone.utc)
        # print(boo)
        # ts = self.readTimeseriesCSV('b2bdata/irradiance-2021-07-21-Alcolea-one-registry.csv')
        # print(ts)
        # time = "2021-05-31T22:00:06.773000+00:00"
        # self.assertEqual(ts[0][0].isoformat(), time)
        self.assertTrue(False)

    # TODO which timezone is the csv data we have there? We're assuming Europe Madrid
    # TODO this test fails if system timezone is not CET
    def test__readTimeseriesCSV(self):
        ts = self.readTimeseriesCSV('b2bdata/irradiance-2021-07-21-Alcolea-one-registry.csv')
        print(ts)
        time = "2021-05-31T22:00:06.773000+00:00"
        self.assertEqual(ts[0][0].isoformat(), time)

    def test__irradiance__write_read_one_registry(self):
        self.setupPlant()
        ts = self.importData(self.sensor,
            'b2bdata/irradiance-2021-07-21-Alcolea-one-registry.csv'
        )
        result = database.select('select time, sensor, irradiation_w_m2 from sensorirradiationregistry limit 1;')

        print(ts)
        print(result)
        print(result[0][0])
        print(result[0][0].isoformat())
        print(datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo)

        self.assertOutputB2B(result)


    def test_irradiation(self):
        self.setupPlant()
        self.importData(self.sensor,
            'b2bdata/irradiance-2021-07-21-Alcolea.csv'
        )
        query = Path('queries/view_irradiation.sql').read_text(encoding='utf8')
        result = database.select(query)

        self.assertOutputB2B(result)

    def test_irradiation_oneDay(self):
        self.setupPlant()
        self.importData(self.sensor,
            'b2bdata/irradiance-2021-07-21-Alcolea.csv'
        )
        query = Path('queries/view_irradiation.sql').read_text(encoding='utf8')
        result = [r for r in database.select(query)
            if todtaware('2021-06-01 00:00:00') <= r.time
            and r.time < todtaware('2021-06-02 00:00:00')
            and r.sensor == 1
        ]

        self.assertOutputB2B(result)

    def test_irradiation_incompleteLastDay(self):
        self.setupPlant()
        self.importData(self.sensor,
            'b2bdata/irradiance-2021-07-21-Alcolea_test_cases.csv'
        )
        query = Path('queries/view_irradiation.sql').read_text(encoding='utf8')
        result = [r.irradiation_w_m2_h for r in database.select(query)
            if todtaware('2021-07-14 9:00:00') <= r.time
            and r.time < todtaware('2021-07-14 10:00:00')
            and r.sensor == 1
        ][0]

        self.assertAlmostEqual(result, 551.254630277778, places=5)

    def test_irradiation_halfHourWithReadings(self):
        self.setupPlant()
        self.importData(self.sensor,
            'b2bdata/irradiance-2021-07-21-Alcolea_test_cases.csv'
        )
        query = Path('queries/view_irradiation.sql').read_text(encoding='utf8')
        result = [r.irradiation_w_m2_h for r in database.select(query)
            if todtaware('2021-07-14 8:00:00') <= r.time
            and r.time < todtaware('2021-07-14 9:00:00')
        ][0]

        self.assertAlmostEqual(result, 415.2791252777778, places=5)

    def test_irradiation_second_sensors(self):
        self.setupPlant()
        plant = self.plant
        sensor_florida = SensorIrradiation(plant=plant, name="irradiationSensor2")
        self.importData(sensor_florida,
            'b2bdata/irradiance-2021-07-21-Florida.csv'
        )
        query = Path('queries/view_irradiation.sql').read_text(encoding='utf8')
        result = database.select(query)

        self.assertOutputB2B(result)

    def test_irradiation_multiple_sensors(self):
        self.setupPlant()
        self.importData(self.sensor,
            'b2bdata/irradiance-2021-07-21-Alcolea.csv'
        )
        plant = self.plant
        sensor_florida = SensorIrradiation(plant=plant, name="irradiationSensor2")
        self.importData(sensor_florida,
            'b2bdata/irradiance-2021-07-21-Florida.csv'
        )
        query = Path('queries/view_irradiation.sql').read_text(encoding='utf8')
        result = database.select(query)

        self.assertOutputB2B(result)

    # TODO test output of one sensor equal to two sensors for the one sensor
    def _test_irradiation_multiple_sensors_same_as_one(self):
        self.setupPlant()
        self.importData(self.sensor,
            'b2bdata/irradiance-2021-07-21-Alcolea.csv'
        )
        plant = self.plant
        sensor_florida = SensorIrradiation(plant=plant, name="irradiationSensor2")
        self.importData(sensor_florida,
            'b2bdata/irradiance-2021-07-21-Florida.csv'
        )
        query = Path('queries/view_irradiation.sql').read_text(encoding='utf8')
        result = database.select(query)

        self.assertOutputB2B(result)


    def test_irradiation_oneHour_againstTrapezoidal(self):
        # remember that you have an trapezoidal approximation script for csv at scripts/integrate_csv
        from scripts.integrate_csv import integrate

        trapzIrradiationTS = integrate('b2bdata/irradiance-2021-07-21-Alcolea_test_cases.csv')

        self.assertAlmostEqual(trapzIrradiationTS[0][1], 448.17932250650483, places=3)
