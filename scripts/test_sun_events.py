import os
import unittest
import datetime

ENVIRONMENT_VARIABLE = 'PLANTMONITOR_MODULE_SETTINGS'

os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from click.testing import CliRunner
from pony import orm
from ORM.pony_manager import PonyManager

from sun_events import SunEvents, plant_sun_events_update

class SunEvents_Test(unittest.TestCase):

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        os.environ['PGTZ'] = 'UTC'

        database_info = envinfo.DB_CONF

        self.pony = PonyManager(database_info)

        self.pony.define_solar_models()
        self.pony.binddb()

    def tearDown(self):

        '''
        binddb calls gneerate_mapping which creates the tables outside the transaction
        drop them
        '''
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()

    def createPlant(self):

        with orm.db_session:
            p = self.pony.db.Plant(name='roger', codename='Som_roger')
            self.pony.db.PlantLocation(plant=p, latitude=41.967599, longitude=2.837782)

    def createPlantWithoutLocation(self):

        with orm.db_session:
            p = self.pony.db.Plant(name='roger', codename='Som_roger')

    def test__checkEnvironment(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

    def test__sunevents_click(self):

        self.createPlant()

        runner = CliRunner()
        result = runner.invoke(
            plant_sun_events_update, '--start 2021-11-29 --end 2021-11-29 --plant roger'.split()
        )

        self.assertEqual(result.exit_code, 0)

    def test__sun_events_update__no_plant(self):

        # run sun_events

        start = datetime.datetime(2021, 11, 29, 3, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2021, 11, 30, 3, 0, 0, 0, tzinfo=datetime.timezone.utc)

        se = SunEvents()

        se.sun_events_update(start=start, end=end)

        with orm.db_session:
            result = orm.select((s.plant.name, s.sunrise, s.sunset) for s in self.pony.db.SolarEvent)[:].to_list()

        expected = []

        self.assertListEqual(result, expected)


    def test__sun_events_update__no_plant_no_location(self):

        self.createPlantWithoutLocation()

        # run sun_events

        start = datetime.datetime(2021, 11, 29, 3, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2021, 11, 30, 3, 0, 0, 0, tzinfo=datetime.timezone.utc)

        se = SunEvents()

        se.sun_events_update(start=start, end=end)

        with orm.db_session:
            result = orm.select((s.plant.name, s.sunrise, s.sunset) for s in self.pony.db.SolarEvent)[:].to_list()

        expected = []

        self.assertListEqual(result, expected)


    def test__sun_events_update__one_plant(self):

        self.createPlant()

        # run sun_events

        start = datetime.datetime(2021, 11, 29, 3, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2021, 11, 30, 3, 0, 0, 0, tzinfo=datetime.timezone.utc)

        se = SunEvents()

        se.sun_events_update(start=start, end=end)

        with orm.db_session:
            result = orm.select((s.plant.name, s.sunrise, s.sunset) for s in self.pony.db.SolarEvent)[:].to_list()

        expected_sunrise = datetime.datetime(2021, 11, 29, 6, 54, 52, 304659, tzinfo=datetime.timezone.utc)
        expected_sunset = datetime.datetime(2021, 11, 29, 16, 18, 49, 597633, tzinfo=datetime.timezone.utc)

        expected = [('roger', expected_sunrise, expected_sunset)]

        self.assertListEqual(result, expected)


