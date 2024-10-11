# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

import datetime as dt

from yamlns import namespace as ns

from plantmonitor.monitor.standardization import (
    registers_to_plant_data,
    alcolea_inverter_to_plantdata,
    florida_inverter_to_plantdata,
    fontivsolar_inverter_to_plantdata,
    registers_to_plant_data,
    wattia_sensor_to_plantdata,
    string_inverter_registers_merge,
    getFVstrings,
    monsol_meter_to_plantdata
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
                    ('pac_r_dw', 10000),
                    ('pac_s_dw', 20000),
                    ('pac_t_dw', 30000),
                    ('powerreactive_t_v', 0),
                    ('powerreactive_r_v', 0),
                    ('powerreactive_s_v', 0),
                    ('temp_inv_dc', 320),
                    ('time', dt.datetime(2021, 1, 20, 10, 38, 14, 261754, tzinfo=dt.timezone.utc))
                ])

    def alibaba_registers(self):

        registers = {'Alibaba': [{
            'name': 'Alice',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': self.inverter_registers(),
            }]
        }

        return registers

    # TODO refactor how many devices are in seperate dictionaries instead of in the same list
    def alibaba_registers_manyInverters(self):

        registers = {'Alibaba': [{
            'name': 'Alice',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': self.inverter_registers(),
            },{
            'name': 'Bob',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': self.inverter_registers(),
            },{
            'name': 'Charlie',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': self.inverter_registers(),
            }]
        }

        return registers

    def alibaba_registers_inverterWithStrings(self):

        registers = {'Alibaba': [{
                'name': 'Alice',
                'type': 'inverter',
                'model': 'aros-solar',
                'register_type': 'holding_registers',
                'fields': self.inverter_registers(),
            },{
                'name': 'AliceStrings',
                'type': 'inverterStrings',
                'model': 'aros-solar',
                'register_type': 'holding_registers',
                'fields': ns([
                        ('string1:intensity_mA',100),
                        ('string2:intensity_mA',200)
                ])
            }]
        }

        return registers

    def alibaba_register_invertersWithStrings(self):

        registers = {'Alibaba': [{
                'name': 'Alice',
                'type': 'inverter',
                'model': 'aros-solar',
                'register_type': 'holding_registers',
                'fields': self.inverter_registers(),
            },{
                'name': 'AliceStrings',
                'type': 'inverterStrings',
                'model': 'aros-solar',
                'register_type': 'holding_registers',
                'fields': ns([
                        ('string1:intensity_mA',100),
                        ('string2:intensity_mA',200)
                ])
            },{
                'name': 'Bob',
                'type': 'inverter',
                'model': 'aros-solar',
                'register_type': 'holding_registers',
                'fields': self.inverter_registers(),
            },
            {
                'name': 'BobStrings',
                'type': 'inverterStrings',
                'model': 'aros-solar',
                'register_type': 'holding_registers',
                'fields': ns([
                        ('string1:intensity_mA',300),
                        ('string2:intensity_mA',400)
                ])
            }]
        }

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

        registers = {'Alibaba': [{
            'name': 'Alice',
            'type': 'wattiasensor',
            'model': 'aros-solar',
            'register_type': 'input_registers',
            'fields': self.sensorWattia_registers(),
            }]
        }

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


    def test__alcolea_inverter_with_strings_to_plantdata(self):

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

        registers_time = plants_registers[plant_name][0]['fields']['time']

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

        registers_time = plants_registers[plant_name][0]['fields']['time']

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

    def test__string_inverter_registers_merge__inverterWithStrings(self):

        plant_registers = self.alibaba_registers_inverterWithStrings()
        devices_registers = plant_registers['Alibaba']

        merged_devices_registers = string_inverter_registers_merge(devices_registers)
        expected_merged_devices_registers = [{
            'name': 'Alice',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': ns(**self.inverter_registers(), **ns(
                        [
                            ('string1:intensity_mA', 100),
                            ('string2:intensity_mA', 200)
                        ])
                )
        }]

        self.maxDiff=None
        self.assertEqual(len(expected_merged_devices_registers), 1)
        self.assertDictEqual(expected_merged_devices_registers[0], merged_devices_registers[0])


    def test__string_inverter_registers_merge__invertersWithStrings(self):

        plants_registers = self.alibaba_register_invertersWithStrings()
        devices_registers = plants_registers['Alibaba']

        merged_devices_registers = string_inverter_registers_merge(devices_registers)

        expected_merged_devices_registers = [{
            'name': 'Alice',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': ns(**self.inverter_registers(), **ns(
                        [
                            ('string1:intensity_mA', 100),
                            ('string2:intensity_mA', 200)
                        ])
                )
        },{
            'name': 'Bob',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields':
                ns(**self.inverter_registers(), **ns(
                        [
                            ('string1:intensity_mA', 300),
                            ('string2:intensity_mA', 400)
                        ])
                )
        }]

        self.maxDiff=None
        self.assertEqual(len(expected_merged_devices_registers), 2)
        self.assertListEqual(expected_merged_devices_registers, merged_devices_registers)


    def test__string_inverter_registers_merge__inverter_inverterWithStrings(self):

        plants_registers = self.alibaba_register_invertersWithStrings()
        devices_registers = plants_registers['Alibaba']
        del devices_registers[3]

        merged_devices_registers = string_inverter_registers_merge(devices_registers)

        expected_merged_devices_registers = [{
            'name': 'Alice',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': ns(**self.inverter_registers(), **ns(
                        [
                            ('string1:intensity_mA', 100),
                            ('string2:intensity_mA', 200)
                        ])
                )
        },{
            'name': 'Bob',
            'type': 'inverter',
            'model': 'aros-solar',
            'register_type': 'holding_registers',
            'fields': self.inverter_registers()
        }]

        self.maxDiff=None
        self.assertEqual(len(expected_merged_devices_registers), 2)
        self.assertListEqual(expected_merged_devices_registers, merged_devices_registers)

    def test__getFVstrings__inverter_one_string_per_registry(self):

        inverter_registers = {
            'string1:intensity_dA': 62,
            'string2:intensity_dA': 61,
            'string3:intensity_dA': 0
        }

        inverter_strings = getFVstrings(inverter_registers)

        expected_strings = {
            'String:string1:intensity_mA': 6200,
            'String:string2:intensity_mA': 6100,
            'String:string3:intensity_mA': 0,
        }

        self.maxDiff = None
        self.assertEqual(len(inverter_strings), len(expected_strings))
        self.assertDictEqual(expected_strings, inverter_strings)

    def test__getFVstrings__inverter_two_strings_per_registry(self):

        inverter_registers = {
            'string1_string2:intensity_dA': 15933,
            'string3_string4:intensity_dA': 61,
            'string5_string6:intensity_dA': 0
        }

        inverter_strings = getFVstrings(inverter_registers)

        expected_strings = {
            'String:string1:intensity_mA': 6100,
            'String:string2:intensity_mA': 6200,
            'String:string3:intensity_mA': 6100,
            'String:string4:intensity_mA': 0,
            'String:string5:intensity_mA': 0,
            'String:string6:intensity_mA': 0,
        }

        self.maxDiff = None
        self.assertEqual(len(inverter_strings), len(expected_strings))
        self.assertDictEqual(expected_strings, inverter_strings)

    def test__registers_to_plant_data__notime(self):

        plant_name = 'Alibaba'

        plants_registers = self.alibaba_registers()

        del plants_registers[plant_name][0]['fields']['time']

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

        inverter_name = plant_registers[plant_name][0]['name']
        register_time = plant_registers[plant_name][0]['fields']['time']

        plant_data = registers_to_plant_data(plant_name, plant_registers, generic_plant=True)

        packet_time = plant_data['time']

        expected_plant_data = {
            'plant': plant_name,
            'version': '1.0',
            'time': packet_time,
            'devices': [{
                'id': 'Inverter:Alice',
                'readings': [{'energy_wh': 1755600,
                    'intensity_ca_mA': None,
                    'intensity_cc_mA': None,
                    'power_w': 600000,
                    'temperature_dc': 320,
                    'time': register_time,
                    'uptime_h': 18827,
                    'voltage_ca_mV': None,
                    'voltage_cc_mV': None
                }]
            }]
        }

        self.maxDiff=None
        self.assertDictEqual(expected_plant_data, plant_data)

    def test__wattia_sensor_to_plantdata(self):

        sensor_name = "WattMan"

        sensor_registers = self.sensorWattia_registers__withtime()

        time = sensor_registers['time']

        sensor_packets = wattia_sensor_to_plantdata(sensor_name, sensor_registers)

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

    def test__wattia_sensor_to_plantdata__notime(self):

        sensor_name = "WattMan"

        sensor_registers = self.sensorWattia_registers()

        sensor_packets = wattia_sensor_to_plantdata(sensor_name, sensor_registers)
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

        sensor_name = 'Alice'
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

    def expected_inverter_plantdata(self, inverter_time):
        return {'energy_wh': 1755600,
            'intensity_ca_mA': None,
            'intensity_cc_mA': None,
            'power_w': 600000,
            'temperature_dc': 320,
            'time': inverter_time,
            'uptime_h': 18827,
            'voltage_ca_mV': None,
            'voltage_cc_mV': None,
            'String:string1:intensity_mA': 1000,
            'String:string2:intensity_mA': 2000
        }

    def test__alcolea_inverter_to_plantdata__strings(self):

        self.maxDiff=None
        inverter_registers = self.inverter_registers()
        inverter_time = inverter_registers['time']
        inverter_registers['string1:intensity_dA'] = 10
        inverter_registers['string2:intensity_dA'] = 20
        inverter_name = 'Alice'

        inverter_registers_plantdata = alcolea_inverter_to_plantdata(inverter_name, inverter_registers)

        expected_inverter_registers_plantdata = {
            'id': 'Inverter:Alice',
            'readings': [self.expected_inverter_plantdata(inverter_time)]
        }
        self.assertDictEqual(inverter_registers_plantdata, expected_inverter_registers_plantdata)

    # TODO inverter_registers from fontivsolar
    def _test__fontivsolar_inverter_to_plantdata__strings(self):

        inverter_registers = self.fontivsolar_inverter_registers()
        inverter_time = inverter_registers['time']
        inverter_registers['string1:intensity_dA'] = 10
        inverter_registers['string2:intensity_dA'] = 20
        inverter_name = 'Alice'

        inverter_registers_plantdata = alcolea_inverter_to_plantdata(inverter_name, inverter_registers)

        expected_inverter_registers_plantdata = {
            'id': 'Inverter:Alice',
            'readings': [self.expected_inverter_plantdata(inverter_time)]
        }
        self.assertDictEqual(inverter_registers_plantdata, expected_inverter_registers_plantdata)

    # TODO inverter_registers from florida
    def _test__florida_inverter_to_plantdata__strings(self):

        inverter_registers = self.florida_inverter_registers()
        inverter_time = inverter_registers['time']
        inverter_registers['string1:intensity_dA'] = 10
        inverter_registers['string2:intensity_dA'] = 20
        inverter_name = 'Alice'

        inverter_registers_plantdata = florida_inverter_to_plantdata(inverter_name, inverter_registers)

        expected_inverter_registers_plantdata = {
            'id': 'Inverter:Alice',
            'readings': [self.expected_inverter_plantdata(inverter_time)]
        }
        self.assertDictEqual(inverter_registers_plantdata, expected_inverter_registers_plantdata)

    def sample_historic_reading(self):
        return  {
            'message': '[OK][QUERY][CONTADORES][DATOS ACTUALES] ',
            'data': [{
                'elemento': 'VALLEHERMOSO',
                'id_dispositivo': 2201,
                'id_tipo_dispositivo': 3,
                'fecha_completa': '2021-11-01 00:05:00', # TODO a bug in the api ignores hour_ini
                'kw_pico': 1890,
                'pac': -6.900000095367432,
                'pac1': -1.9900000095367432,
                'pac2': -2.0299999713897705,
                'pac3': -2.869999885559082,
                'iac1': 1,
                'iac2': 1,
                'iac3': 1,
                'vac1': 9529.6,
                'vac2': 9435.7,
                'vac3': 9557.6,
                'pr1': -0.68,
                'pr2': 0.25,
                'pr3': -0.27,
                'prt': -0.71,
                'fp1': 0.94,
                'fp2': 0.99,
                'fp3': 0.99,
                'fpt': 0,
                'eedia': 0,
                'eidia': 3,
                'eae': 18291955,
                'eai': 104279,
                'er1': 274,
                'er2': 1670501,
                'er3': 4885,
                'er4': 107377,
            }],
            'code': 200,
            'timestamp': '01-12-2021 14:34:24'
        }

    def test__standarize_monsol_meter_readings(self):

        meter_name = '1234578'
        sample = self.sample_historic_reading()

        plant_data = monsol_meter_to_plantdata(meter_name, sample['data'])

        expected = {
            'id' : 'Meter:{}'.format(meter_name),
            'readings': [{
                'time' : dt.datetime(2021, 11, 1, 0, 20, tzinfo=dt.timezone.utc),
                'export_energy_wh': 0,
                'import_energy_wh': 3000,
                'r1_VArh': 274000,
                'r2_VArh': 1670501000,
                'r3_VArh': 4885000,
                'r4_VArh': 107377000,
            }]
        }

        self.assertDictEqual(plant_data, expected)

