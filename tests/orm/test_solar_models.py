import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
import datetime

from plantmonitor.ORM.pony_manager import PonyManager

from pony import orm

class SolarModels_Test(unittest.TestCase):

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        os.environ['PGTZ'] = 'UTC'

        database_info = envinfo.DB_CONF

        self.pony = PonyManager(database_info)

        self.pony.define_solar_models()
        self.pony.binddb(create_tables=True)

        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()

        '''
        binddb calls gneerate_mapping which creates the tables outside the transaction
        drop them
        '''
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()

    def createPlant(self, plant_name):
        self.pony.db.Plant(name=plant_name, codename='Som_{}'.format(plant_name))

    def test__emptyDB(self):
        pass

    def test__timescaleDB(self):
        self.pony.timescale_tables()

    def test__SolarEvent__insertList(self):
        self.maxDiff=None

        self.createPlant('roger')

        sunrise1 = datetime.datetime(2021, 11, 29, 6, 56, 10, 588861, tzinfo=datetime.timezone.utc)
        sunset1 = datetime.datetime(2021, 11, 29, 16, 19, 10, 588861, tzinfo=datetime.timezone.utc)
        sunrise2 = datetime.datetime(2021, 6, 24, 4, 19, 10, 588861, tzinfo=datetime.timezone.utc)
        sunset2 = datetime.datetime(2021, 6, 24, 19, 29, 10, 588861, tzinfo=datetime.timezone.utc)

        solarevents = [('roger', sunrise1, sunset1),('roger', sunrise2, sunset2)]
        self.pony.db.SolarEvent.insertSolarEvents(solarevents)
        self.pony.db.flush()

        solarevents_query = orm.select(
                (se.plant.name, se.sunrise, se.sunset)
                for se in self.pony.db.SolarEvent
            ).order_by(lambda: orm.desc(se.sunrise))
        solarevents_list = solarevents_query[:].to_list()

        solarevents_expected = [('roger', sunrise1, sunset1),('roger', sunrise2, sunset2)]

        self.assertListEqual(solarevents_list, solarevents_expected)


    # def test__createEntity(self):

    #     plant = self.pony.db.Plant(name='roger', codename='Som_Roger')
    #     location = self.pony.db.PlantLocation(plant=plant, latitude=41.967599, longitude= 2.837782)
    #     plant.location = location

    #     devices = orm.select(p.name for p in self.pony.db.Plant)[:].to_list()
    #     self.assertListEqual(devices, ['roger'])

    #     devices = orm.select((p.name, p.location.latitude, p.location.longitude) for p in self.pony.db.Plant)[:].to_list()
    #     self.assertListEqual(devices, [('roger', 41.967599, 2.837782)])


    # TODO test that production database continues to generate_mapping without errors
    # TODO make solar migration on general models
    def test__db_integrity(self):
        pass