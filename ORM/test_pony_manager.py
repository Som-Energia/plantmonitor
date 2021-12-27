import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
import datetime
from yamlns import namespace as ns
from .pony_manager import PonyManager

from pony import orm

# Tests that mingle the database declaration
class PonyManager_bind_Test(unittest.TestCase):

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        os.environ['PGTZ'] = 'UTC'

        database_info = envinfo.DB_CONF

        self.pony = PonyManager(database_info)

    def tearDown(self):

        '''
        binddb calls gneerate_mapping which creates the tables outside the transaction
        drop them
        '''
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()

    def samplePlantNS(self):
        alcoleaPlantNS = ns.loads("""\
            name: alcolea
            codename: SCSOM04
            description: la bonica planta
            meters:
            - meter:
                name: '1234578'
            inverters:
            - inverter:
                name: '5555'
            - inverter:
                name: '6666'
            irradiationSensors:
            - irradiationSensor:
                name: alberto
            temperatureModuleSensors:
            - temperatureModuleSensor:
                name: pol
            temperatureAmbientSensors:
            - temperatureAmbientSensor:
                name: joana
        """)
        return alcoleaPlantNS

    def test__emptyDB(self):
        self.pony.binddb()

    def test__create_entity(self):
        self.pony.define_all_models()
        self.pony.binddb()
        with orm.db_session:
            alcolea_ns = self.samplePlantNS()

            alcolea = self.pony.db.Plant(name=alcolea_ns['name'], codename=alcolea_ns['codename'])
            alcolea = alcolea.importPlant(alcolea_ns)
            orm.flush()

            devices = orm.select(p.name for p in self.pony.db.Plant)[:].to_list()
            self.assertListEqual(devices, ['alcolea'])

            devices = orm.select((p.name, m.name, i.name) for p in self.pony.db.Plant for m in p.meters for i in p.inverters)[:].to_list()
            self.assertListEqual(devices, [('alcolea', '1234578', '6666'), ('alcolea', '1234578', '5555')])

            alcolea_in_db = orm.select(p for p in self.pony.db.Plant).limit(1)[0]
            alcolea_dict = alcolea_in_db.to_dict(with_collections=True, related_objects=False)

            expected = {
                'id': 1,
                'name': 'alcolea',
                'codename': 'SCSOM04',
                'description': 'la bonica planta',
                'meters': [1],
                'inverters': [1, 2],
                'sensors': [1,2,3]
            }

            alcolea_thin_dict = {k:d for k,d in alcolea_dict.items() if d}
            self.assertDictEqual(alcolea_thin_dict, expected)

            orm.rollback()


    def test__create_solar_entity(self):

        self.pony.define_solar_models()
        self.pony.binddb()

        with orm.db_session:
            plant = self.pony.db.Plant(name='roger', codename='Som_Roger')
            location = self.pony.db.PlantLocation(plant=plant, latitude=41.967599, longitude= 2.837782)
            plant.location = location

            devices = orm.select(p.name for p in self.pony.db.Plant)[:].to_list()
            self.assertListEqual(devices, ['roger'])

            devices = orm.select((p.name, p.location.latitude, p.location.longitude) for p in self.pony.db.Plant)[:].to_list()
            self.assertListEqual(devices, [('roger', 41.967599, 2.837782)])




class PonyManager_Test(unittest.TestCase):

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        os.environ['PGTZ'] = 'UTC'

        database_info = envinfo.DB_CONF

        self.pony = PonyManager(database_info)

        self.pony.define_all_models()
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
