# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

from yamlns import namespace as ns

from .resource import (
    ProductionPlant,
    ProductionDevice,
)

class Resource_Test(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

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

    def test__productionplant_load(self):
        self.maxDiff = None
        expected_plant_yaml = self.plant_registers()
        plant_yaml = ProductionPlant.load('conf/modmap.yaml', 'Alcolea')
        self.assertDictEqual(plant_yaml, expected_plant_yaml)
