# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

import datetime as dt

from yamlns import namespace as ns

from .standardization import (
    alcolea_registers_to_plantdata,
    alcolea_inverter_to_plantdata,
    registers_to_plant_data,
    alcolea_sensorIrradiation_to_plantadata,
    alcolea_sensorTemperature_to_plantadata,
)

class Standardization_Test(unittest.TestCase):

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

    def alibaba_registers(self):

        registers = [{'Alibaba': [{
            'name': 'Alice',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': self.inverter_registers(),
            }]
        }]

        return registers

    def sensorIrradiation_registers(self):
        return ns([
                ('time', dt.datetime(
                    2021, 1, 20, 10, 38, 14, 261754, tzinfo=dt.timezone.utc)),
                ('irradiation_w_m2', 3412),
                ('temperature_dc', 531),
            ])
    def sensorTemperature_registers(self):
        return ns([
                ('time', dt.datetime(
                    2021, 1, 20, 10, 38, 14, 261754, tzinfo=dt.timezone.utc)),
                ('temperature_dc', 531),
            ])

    def test__alcolea_inverter_to_plantdata(self):

        inverter_name = "Alice"

        inverter_registers = self.inverter_registers()

        time = inverter_registers['time']

        inverter_registries = alcolea_inverter_to_plantdata(inverter_name, inverter_registers)

        expected_inverter_registries = {
            'id': 'Inverter:{}'.format(inverter_name),
            'readings':
                [{
                    'energy_wh': 17556,
                    'power_w': 60000,
                    'intensity_cc_mA': None,
                    'intensity_ca_mA': None,
                    'voltage_cc_mV': None,
                    'voltage_ca_mV': None,
                    'uptime_h': None,
                    'temperature_dc': 32000,
                    'time': time,
                }]
            }

        self.maxDiff=None
        self.assertDictEqual(expected_inverter_registries, inverter_registries)

    def test__alcolea_registers_to_plantdata(self):

        plant_name = 'Alibaba'

        plants_registers = self.alibaba_registers()

        devices_registers = plants_registers[0][plant_name]

        time = devices_registers[0]['fields']['time']

        plant_data = alcolea_registers_to_plantdata(devices_registers)

        plant_data['plant'] = 'Alibaba'

        expected_plant_data = {
            'plant': plant_name,
            'devices':
            [{
                'id': 'Inverter:Alice',
                'readings':
                [{
                    'energy_wh': 17556,
                    'power_w': 60000,
                    'intensity_cc_mA': None,
                    'intensity_ca_mA': None,
                    'voltage_cc_mV': None,
                    'voltage_ca_mV': None,
                    'uptime_h': None,
                    'temperature_dc': 32000,
                    'time': time,
                }]
            }],
        }

        self.maxDiff=None
        self.assertDictEqual(expected_plant_data, plant_data)

    def test__registers_to_plant_data__wrongPlant(self):

        plant_name = "WrongPlant"

        plant_registers = {}

        plant_data = registers_to_plant_data(plant_name, plant_registers)

        expected_plant_data = {}

        self.maxDiff=None
        self.assertDictEqual(expected_plant_data, plant_data)


    def test__registers_to_plant_data__RightPlant(self):

        plant_name = "Alibaba"

        plant_registers = self.alibaba_registers()

        inverter_name = plant_registers[0][plant_name][0]['name']

        #TODO: refactor this so we don't have to use the "Alcolea" if
        plant_data = registers_to_plant_data("Alcolea", plant_registers)

        expected_plant_data = {
            'plant': 'Alcolea',
            'devices': []
        }

        self.maxDiff=None
        self.assertDictEqual(expected_plant_data, plant_data)

    def test__alcolea_sensorIrradiation_to_plantdata(self):

        sensor_name = "Bob"

        sensor_registers = self.sensorIrradiation_registers()

        time = sensor_registers['time']

        sensor_registries = alcolea_sensorIrradiation_to_plantadata(sensor_name, sensor_registers)

        expected_sensor_registries = {
            'id': 'Sensor:{}'.format(sensor_name),
            'readings':
                [{
                    'irradiation_w_m2': 341,
                    'temperature_dc': 2810,
                    'time': time,
                }]
            }

        self.maxDiff=None
        self.assertDictEqual(expected_sensor_registries, sensor_registries)

    def test__alcolea_sensorTemperature_to_plantdata(self):

        sensor_name = "Bob"

        sensor_registers = self.sensorTemperature_registers()

        time = sensor_registers['time']

        sensor_registries = alcolea_sensorTemperature_to_plantadata(sensor_name, sensor_registers)

        expected_sensor_registries = {
            'id': 'Sensor:{}'.format(sensor_name),
            'readings':
                [{
                    'temperature_dc': 2810,
                    'time': time,
                }]
            }

        self.maxDiff=None
        self.assertDictEqual(expected_sensor_registries, sensor_registries)