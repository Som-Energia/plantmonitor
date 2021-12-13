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
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

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
                sensor=self.sensor,
                time=time,
                irradiation_w_m2=int(round(irradiation_w_m2)),
                temperature_dc=0,
            )
        database.flush()

        return [
            (time, self.plant, self.sensor, irradiation_w_m2)
            for time, irradiation_w_m2 in irradianceTimeSeries
        ]

    def assertOutputB2B(self, result):
        result = "\n".join((
            "{}, {:.9f}".format(time.isoformat() if time else None, irradiation if irradiation is not None else float('nan'))
            for time, sensor, irradiation in result
        ))
        self.assertB2BEqual(result)

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

        self.assertEqual(result, 551.254630277778)

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

    def test_irradiation_oneHour_againstTrapezoidal(self):
        # remember that you have an trapezoidal approximation script for csv at scripts/integrate_csv
        from scripts.integrate_csv import integrate

        trapzIrradiationTS = integrate('b2bdata/irradiance-2021-07-21-Alcolea_test_cases.csv')

        self.assertAlmostEqual(trapzIrradiationTS[0][1], 448.17932250650483, places=3)
