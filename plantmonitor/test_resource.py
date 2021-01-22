# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

from yamlns import namespace as ns

from .resource import (
    ProductionPlant,
    ProductionDevice,
    ProductionDeviceModMap,
)

class Resource_Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def sensorDeviceNS(self):
        devicesns = ns.loads("""
  devices:
    - name: inversor1
      type: inversorA345Zx
      description: inversor1
      model: aros-solar
      enabled: True
      modmap:
        - type: holding_registers
          registers:
            0 :  1HR0
            1 :  1HR1
            2 :  1HR2
            3 :  2HR3
            4 :  2HR4
            5 :  2HR5
          scan:
            start: 0
            range: 3
        - type: coils
          registers:
            0 : xcx
            1 : sdf
            2 : fkl
          scan:
            start: 0
            range: 3
        - type: write_coils
          registers:
            0 : xcx
            1 : sdf
            2 : fkl
          scan:
            start: 0
            range: 3
      protocol:
        type:
        ip:
        port:
        slave:
        timeout:
""")
        print(devicesns['devices'][0])
        return devicesns['devices'][0]

    def modmapNS(self):
        return ns.load('conf/modmap.yaml')

    def inverter_registers(self):
        return ns([
                    ('daily_energy_h_wh', 0),
                    ('daily_energy_l_wh', 17556),
                    ('e_total_h_wh', 566),
                    ('e_total_l_wh', 49213),
                    ('h_total_h_h', 0),
                    ('h_total_l_h', 18827),
                    ('pac_r_w', 10000),
                    ('pac_s_w', 20000),
                    ('pac_t_w', 30000),
                    ('powerreactive_t_v', 0),
                    ('powerreactive_r_v', 0),
                    ('powerreactive_s_v', 0),
                    ('temp_inv_c', 320),
                    ('time', dt.datetime(2021, 1, 20, 10, 38, 14, 261754, tzinfo=dt.timezone.utc))
                ])

    def alcolea_registers(self):

        registers = [{'Alcolea': [{
            'name': 'Alice',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': self.inverter_registers(),
            }]
        }]

        return registers

    def plant_registers(self):
        return ns([
                ('plantmonitor', [ns([
                            ('name', 'Alcolea'),
                            ('description', 'Alcolea'),
                            ('enabled', True),
                            ('devices', self.device_registers()),
                        ])
                    ])
                ])

    def device_registers(self):
        return [ns([
                        ('name', 'inversor1'),
                        ('type', 'inversorA345Zx'),
                        ('description', 'inversor1'),
                        ('model', 'aros-solar'),
                        ('enabled', True),
                        ('modmap', self.modmap_registers),
                    ])
                ]

    def modmap_registers(self):
        return [ns([
                ('type', 'holding_registers'),
                ('registers', ns([
                    (10, '1HR0'),
                    (11, '1HR'),
                    (12, '2HR2'),
                    (13, '3HR3'),
                    (14, '4HR4')])),
                    ('scan', ns([
                            ('start', 10),
                            ('range', 30)
                        ])
                    )
                ])]

    def __test__productionplant_load(self):
        self.maxDiff = None
        expected_plant_yaml = self.plant_registers()
        plant_yaml = ProductionPlant.load('conf/modmap.yaml', 'Alcolea')
        self.assertDictEqual(plant_yaml, expected_plant_yaml)

    def test__ProductionDeviceModMap_factory__oneDevice_knownTypes(self):
        device_data = self.sensorDeviceNS()
        item_data = device_data.modmap[0]
        dev = ProductionDeviceModMap.factory(item_data)
        self.assertIsNotNone(dev)

    def test__ProductionDeviceModMap_factory__oneSensor_oneUnkownType(self):
        device_data = self.sensorDeviceNS()
        item_data = device_data.modmap[2]
        dev = ProductionDeviceModMap.factory(item_data)
        self.assertIsNone(dev)

    def test__ProductionDevice_load__oneSensor(self):
        aSensor = ProductionDevice()
        device_data = self.sensorDeviceNS()
        aSensor.load(device_data)

        expectedSensor = ProductionDevice()
        expectedSensor.id = None
        expectedSensor.name = 'inversor1'

        self.assertEqual(aSensor.name, expectedSensor.name)

    def __test__ProductionDevice_getRegisters__oneSensor(self):
        aSensor = ProductionDevice()
        device_data = ns()
        aSensor.load(device_data)
        aSensor.get_registers()