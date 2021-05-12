# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

import datetime as dt

from yamlns import namespace as ns

from .standardization import (
    registers_to_plant_data,
    alcolea_inverter_to_plantdata,
    registers_to_plant_data,
    wattia_sensor_to_plantadata,
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
                    ('temp_inv_dc', 320),
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

    def alibaba_registers_manyInverters(self):

        registers = [{'Alibaba': [{
            'name': 'Alice',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': self.inverter_registers(),
            }]
        },{'Alibaba': [{
            'name': 'Bob',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': self.inverter_registers(),
            }]
        },{'Alibaba': [{
            'name': 'Charlie',
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
                ('irradiance_w_m2', 3412),
                ('temperature_dc', 531),
            ])

    def sensorWattia_registers(self):
        return ns([
                ('irradiance_dw_m2', 5031),
                ('module_temperature_dc', 520),
                ('ambient_temperature_dc', 492)
            ])

    def sensorWattia_registers__withtime(self):
        return ns([
                ('time', dt.datetime(
                    2021, 1, 20, 10, 38, 14, 261754, tzinfo=dt.timezone.utc)),
                ('irradiance_dw_m2', 5031),
                ('module_temperature_dc', 520),
                ('ambient_temperature_dc', 492)
            ])

    def sensorTemperature_registers(self):
        return ns([
                ('time', dt.datetime(
                    2021, 1, 20, 10, 38, 14, 261754, tzinfo=dt.timezone.utc)),
                ('temperature_dc', 531),
            ])

    def alibaba_registers_WattiaSensor(self):

        registers = [{'Alibaba': [{
            'name': 'Alice',
            'type': 'wattiasensor',
            'model': 'aros-solar',
            'register_type': 'input_registers',
            'fields': self.sensorWattia_registers(),
            }]
        }]

        return registers

    def test__alcolea_inverter_to_plantdata(self):

        inverter_name = "Alice"

        inverter_registers = self.inverter_registers()

        time = inverter_registers['time']

        inverter_registries = alcolea_inverter_to_plantdata(inverter_name, inverter_registers)

        expected_inverter_registries = {
            'id': 'Inverter:{}'.format(inverter_name),
            'readings':
                [{
                    'energy_wh': 1755600,
                    'power_w': 600000,
                    'intensity_cc_mA': None,
                    'intensity_ca_mA': None,
                    'voltage_cc_mV': None,
                    'voltage_ca_mV': None,
                    'uptime_h': 18827,
                    'temperature_dc': 320,
                    'time': time,
                }]
            }

        self.maxDiff=None
        self.assertDictEqual(expected_inverter_registries, inverter_registries)

    def test__registers_to_plant_data(self):

        plant_name = 'Alibaba'

        plants_registers = self.alibaba_registers()

        registers_time = plants_registers[0][plant_name][0]['fields']['time']

        plant_data = registers_to_plant_data(plant_name, plants_registers, generic_plant=True)

        packet_time = plant_data['time']

        expected_plant_data = {
            'plant': plant_name,
            'version': '1.0',
            'time': packet_time,
            'devices':
            [{
                'id': 'Inverter:Alice',
                'readings':
                [{
                    'energy_wh': 1755600,
                    'power_w': 600000,
                    'intensity_cc_mA': None,
                    'intensity_ca_mA': None,
                    'voltage_cc_mV': None,
                    'voltage_ca_mV': None,
                    'uptime_h': 18827,
                    'temperature_dc': 320,
                    'time': registers_time,
                }]
            }],
        }

        self.maxDiff=None
        self.assertDictEqual(expected_plant_data, plant_data)

    def test__registers_to_plant_data__manyInverters(self):

        plant_name = 'Alibaba'

        plants_registers = self.alibaba_registers_manyInverters()

        registers_time = plants_registers[0][plant_name][0]['fields']['time']

        plant_data = registers_to_plant_data(plant_name, plants_registers, generic_plant=True)

        packet_time = plant_data['time']

        expected_plant_data = {
            'plant': plant_name,
            'version': '1.0',
            'time': packet_time,
            'devices':
            [{
                'id': 'Inverter:Alice',
                'readings':
                [{
                    'energy_wh': 1755600,
                    'power_w': 600000,
                    'intensity_cc_mA': None,
                    'intensity_ca_mA': None,
                    'voltage_cc_mV': None,
                    'voltage_ca_mV': None,
                    'uptime_h': 18827,
                    'temperature_dc': 320,
                    'time': registers_time,
                }]
            },{
                'id': 'Inverter:Bob',
                'readings':
                [{
                    'energy_wh': 1755600,
                    'power_w': 600000,
                    'intensity_cc_mA': None,
                    'intensity_ca_mA': None,
                    'voltage_cc_mV': None,
                    'voltage_ca_mV': None,
                    'uptime_h': 18827,
                    'temperature_dc': 320,
                    'time': registers_time,
                }]
            },{
                'id': 'Inverter:Charlie',
                'readings':
                [{
                    'energy_wh': 1755600,
                    'power_w': 600000,
                    'intensity_cc_mA': None,
                    'intensity_ca_mA': None,
                    'voltage_cc_mV': None,
                    'voltage_ca_mV': None,
                    'uptime_h': 18827,
                    'temperature_dc': 320,
                    'time': registers_time,
                }]
            }],
        }

        self.maxDiff=None
        self.assertDictEqual(expected_plant_data, plant_data)

    def test__registers_to_plant_data__notime(self):

        plant_name = 'Alibaba'

        plants_registers = self.alibaba_registers()

        del plants_registers[0][plant_name][0]['fields']['time']

        plant_data = registers_to_plant_data(plant_name, plants_registers, generic_plant=True)
        packet_time = plant_data['time']

        self.assertIn('time', plant_data['devices'][0]['readings'][0])

        time = plant_data['devices'][0]['readings'][0]['time']

        plant_data['plant'] = 'Alibaba'

        expected_plant_data = {
            'plant': plant_name,
            'version': '1.0',
            'time': packet_time,
            'devices':
            [{
                'id': 'Inverter:Alice',
                'readings':
                [{
                    'energy_wh': 1755600,
                    'power_w': 600000,
                    'intensity_cc_mA': None,
                    'intensity_ca_mA': None,
                    'voltage_cc_mV': None,
                    'voltage_ca_mV': None,
                    'uptime_h': 18827,
                    'temperature_dc': 320,
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

        packet_time = plant_data['time']

        expected_plant_data = {
            'plant': 'Alcolea',
            'version': '1.0',
            'time': packet_time,
            'devices': []
        }

        self.maxDiff=None
        self.assertDictEqual(expected_plant_data, plant_data)

    def test__wattia_sensor_to_plantadata(self):

        sensor_name = "WattMan"

        sensor_registers = self.sensorWattia_registers__withtime()

        time = sensor_registers['time']

        sensor_packets = wattia_sensor_to_plantadata(sensor_name, sensor_registers)

        expected_sensor_registries = [{
            'id': 'SensorIrradiation:{}'.format(sensor_name),
            'readings':
                [{
                    'irradiation_w_m2': 503,
                    'temperature_dc': 270,
                    'time': time,
                }]
            },{
            'id': 'SensorTemperatureModule:{}'.format(sensor_name),
            'readings':
                [{
                    'temperature_dc': 270,
                    'time': time,
                }]
            },{
            'id': 'SensorTemperatureAmbient:{}'.format(sensor_name),
            'readings':
                [{
                    'temperature_dc': 242,
                    'time': time,
                }]
            }]

        self.maxDiff=None
        for packet, expected_packet in zip(sensor_packets, expected_sensor_registries):
            self.assertDictEqual(expected_packet, packet)

    def test__wattia_sensor_to_plantadata__notime(self):

        sensor_name = "WattMan"

        sensor_registers = self.sensorWattia_registers()

        sensor_packets = wattia_sensor_to_plantadata(sensor_name, sensor_registers)
        time = sensor_packets[0]['readings'][0]['time']

        expected_sensor_registries = [{
            'id': 'SensorIrradiation:{}'.format(sensor_name),
            'readings':
                [{
                    'irradiation_w_m2': 503,
                    'temperature_dc': 270,
                    'time': time,
                }]
            },{
            'id': 'SensorTemperatureModule:{}'.format(sensor_name),
            'readings':
                [{
                    'temperature_dc': 270,
                    'time': time,
                }]
            },{
            'id': 'SensorTemperatureAmbient:{}'.format(sensor_name),
            'readings':
                [{
                    'temperature_dc': 242,
                    'time': time,
                }]
            }]

        self.maxDiff=None
        for packet, expected_packet in zip(sensor_packets, expected_sensor_registries):
            self.assertDictEqual(expected_packet, packet)

    def test__registers_to_plantdata__sensorWattia(self):

        sensor_name = "Alice"
        plant_name = 'Alibaba'

        plant_registers = self.alibaba_registers_WattiaSensor()

        plant_data = registers_to_plant_data(plant_name, plant_registers, generic_plant=True)
        packet_time = plant_data['time']

        expected_plant_data = {
                    'plant': plant_name,
                    'version': '1.0',
                    'time': packet_time,
                    'devices': [{
            'id': 'SensorIrradiation:{}'.format(sensor_name),
            'readings':
                [{
                    'irradiation_w_m2': 503,
                    'temperature_dc': 270,
                    'time': packet_time,
                }]
            },{
            'id': 'SensorTemperatureModule:{}'.format(sensor_name),
            'readings':
                [{
                    'temperature_dc': 270,
                    'time': packet_time,
                }]
            },{
            'id': 'SensorTemperatureAmbient:{}'.format(sensor_name),
            'readings':
                [{
                    'temperature_dc': 242,
                    'time': packet_time,
                }]
            }]
        }

        self.maxDiff=None
        self.assertDictEqual(plant_data, expected_plant_data)


    def test__registers_to_plantdata__sensorWattia_inverter(self):
        pass