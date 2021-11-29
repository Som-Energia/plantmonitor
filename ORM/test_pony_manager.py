import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
import datetime

from .pony_manager import PonyManager

from pony import orm

class PonyManager_Test(unittest.TestCase):

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        os.environ['PGTZ'] = 'UTC'

        database_info = envinfo.DB_CONF

        self.pony = PonyManager(database_info)

        self.pony.define_solar_models()
        self.pony.binddb()

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

    def test__emptyDB(self):
        pass

    def test__timescaleDB(self):
        self.pony.timescale_tables()

    def test__createEntity(self):

        plant = self.pony.db.Plant(name='roger', codename='Som_Roger')
        location = self.pony.db.PlantLocation(plant=plant, latitude=41.967599, longitude= 2.837782)
        plant.location = location

        devices = orm.select(p.name for p in self.pony.db.Plant)[:].to_list()
        self.assertListEqual(devices, ['roger'])

        devices = orm.select((p.name, p.location.latitude, p.location.longitude) for p in self.pony.db.Plant)[:].to_list()
        self.assertListEqual(devices, [('roger', 41.967599, 2.837782)])


@unittest.skipIf(True, 'run this test against an existing database')
class PonyManagerReadonly_Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__mapping_existing_db(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        os.environ['PGTZ'] = 'UTC'

        database_info = envinfo.DB_CONF

        self.pony = PonyManager(database_info)

        self.pony.define_solar_models()
        self.pony.binddb()

        orm.db_session.__enter__()



        orm.db_session.__exit__()

        '''
        binddb calls gneerate_mapping which creates the tables outside the transaction
        drop them
        '''
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()
