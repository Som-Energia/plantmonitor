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
        pass

    def tearDown(self):
        pass

    def test__checkEnvironment(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

    def test__sunevents_click(self):

        runner = CliRunner()
        result = runner.invoke(
            plant_sun_events_update, '--start 2021-11-29 --end 2021-11-29 --plant roger'.split()
        )

        self.assertEqual(result, 0)

    def test__schedule(self):

        # run sun_events

        start = datetime.datetime(2021, 11, 29, 3, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2021, 11, 30, 3, 0, 0, 0, tzinfo=datetime.timezone.utc)



        se = SunEvents()

        se.sun_events_update(start=start, end=end)


