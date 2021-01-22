# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest
from unittest.mock import MagicMock

from yamlns import namespace as ns

#from pymodbus import client
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
name: sensor1
type: CellTemperatureSensor
description: sensor1wattia
model: Wattia
enabled: True
modmap:
  - type: input_registers
    registers:
        1 : TemperatureCell
    scan:
        start: 1
        range: 1
protocol:
    type: modbus
    ip: 192.168.1.100
    port: 502
    slave: 37
    timeout:
""")
        return devicesns

    def inverterDeviceNS(self):
        devicesns = ns.loads("""
      name: inversor1
      type: inverter
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
        return devicesns

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
                    ('name', 'Alcolea'),
                    ('description', 'Alcolea'),
                    ('enable', None),
                    ('location', None)
                    #('devices', self.device_registers()),
                    #('db', self.db_registers()),
                ])

    def device_registers(self):
        return [ns([
                        ('name', 'inversor1'),
                        ('type', 'inversorA345Zx'),
                        ('description', 'inversor1'),
                        ('model', 'aros-solar'),
                        ('enable', True),
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

    def test__productionplant_load(self):
        self.maxDiff = None
        expected_plant_ns = self.plant_registers()
        plant = ProductionPlant()
        result = plant.load('conf/modmap_testing.yaml', 'Alcolea')
        self.assertTrue(result)
        plant = plant.todict()
        expected_plant = dict(expected_plant_ns)

        self.assertDictEqual(plant, expected_plant)

    def test__ProductionDeviceModMap_factory__oneDevice_knownTypes(self):
        device_data = self.inverterDeviceNS()
        item_data = device_data.modmap[0]
        dev = ProductionDeviceModMap.factory(item_data)
        self.assertIsNotNone(dev)

    def test__ProductionDeviceModMap_factory__oneSensor_oneUnkownType(self):
        device_data = self.inverterDeviceNS()
        item_data = device_data.modmap[2]
        dev = ProductionDeviceModMap.factory(item_data)
        self.assertIsNone(dev)

    def test__ProductionDevice_load__oneSensor(self):
        aSensor = ProductionDevice()
        device_data = self.sensorDeviceNS()
        aSensor.load(device_data)

        expectedSensor = ProductionDevice()
        expectedSensor.name = 'sensor1'
        print(aSensor.modmap['input_registers'].registers)

        self.assertEqual(aSensor.name, expectedSensor.name)
        self.assertNotEqual(aSensor.modmap, {})
        self.assertDictEqual(
            dict(aSensor.modmap['input_registers'].registers),
            {1: 'TemperatureCell'}
        )

    # TODO mismatch type (detect if device ip changed)
    def __test__ProductionDevice_load__typeMismatch(self):
        aSensor = ProductionDevice()
        device_data = self.sensorDeviceNS()
        device_data.type = 'wrongType'
        result = aSensor.load(device_data)

        self.assertFalse(result)

    def test__ProductionDevice_getRegisters__oneInverter(self):
        aSensor = ProductionDevice()
        device_data = self.inverterDeviceNS()
        aSensor.load(device_data)

        dev = aSensor.modmap['holding_registers']
        dev.get_registers = MagicMock(return_value=[626])

        registers = aSensor.get_registers()

        # list not empty
        self.assertTrue(registers != [])

        metric = registers[0]
        expectedMetric = {
            'name': 'inversor1',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': [626],
        }
        self.assertDictEqual(metric, expectedMetric)

    def test__ProductionDevice_getRegisters__oneSensor(self):
        aSensor = ProductionDevice()
        device_data = self.sensorDeviceNS()
        aSensor.load(device_data)

        dev = aSensor.modmap['input_registers']
        dev.get_registers = MagicMock(return_value=[626])

        registers = aSensor.get_registers()

        # list not empty
        self.assertTrue(registers != [])

        metric = registers[0]
        expectedMetric = {
            'name': 'sensor1',
            'type': 'CellTemperatureSensor',
            'model': 'Wattia',
            'register_type': 'input_registers',
            'fields': [626],
        }
        self.assertDictEqual(metric, expectedMetric)

    # TODO: detect error or ban
    def __test__ProductionDevice_getRegisters__connectionError(self):
        aSensor = ProductionDevice()
        device_data = self.sensorDeviceNS()
        aSensor.load(device_data)

        dev = aSensor.modmap['input_registers']
        dev.get_registers = MagicMock(return_value=[-1])

        registers = aSensor.get_registers()

        self.assertTrue(registers == [])