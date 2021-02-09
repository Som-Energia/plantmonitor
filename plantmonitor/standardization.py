# -*- coding: utf-8 -*-
import sys
import psycopg2
import time
import datetime
import conf.config as config

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

import os

import datetime

def alcolea_sensorTemperature_to_plantadata(alcolea_sensorTemperature_registers):
    logger.error("Not implemented yet")

    temperature_registers = {}
    return temperature_registers

def alcolea_sensorIrradiation_to_plantadata(sensor_name, sensorIrradiation_registers):
    logger.error("Not implemented yet")

    irradiation_registers_plantdata = {
        'id': 'Sensor:{}'.format(sensor_name),
        'readings': []
    }
    for register in sensorIrradiation_registers:
        time = sensorIrradiation_registers['time']
        irradiation_w_m2 = sensorIrradiation_registers['irradiation_w_m2']
        temperature_dc = sensorIrradiation_registers['temperature_dc']
        irradiation_w_m2 = int(round(irradiation_w_m2*0.1+0))
        temperature_dc = int(round((temperature_dc*0.1-25)*100))
        reading = {
            'irradiation_w_m2': irradiation_w_m2,
            'temperature_dc': temperature_dc,
            'time': time,
        }
    irradiation_registers_plantdata['readings'].append(reading)
    return irradiation_registers_plantdata

def alcolea_sensorTemperature_to_plantadata(sensor_name, sensorTemperature_registers):
    logger.error("Not implemented yet")

    temperature_registers_plantdata = {
        'id': 'Sensor:{}'.format(sensor_name),
        'readings': []
    }
    for register in sensorTemperature_registers:
        time = sensorTemperature_registers['time']
        temperature_dc = sensorTemperature_registers['temperature_dc']
        temperature_dc = int(round((temperature_dc*0.1-25)*100))
        reading = {
            'temperature_dc': temperature_dc,
            'time': time,
        }
    temperature_registers_plantdata['readings'].append(reading)
    return temperature_registers_plantdata

def alcolea_inverter_to_plantdata(inverter_name, inverter_registers):
    inverter_registers_plantdata = {
        'id': 'Inverter:{}'.format(inverter_name),
        'readings': []
    }

    #TODO should we assume one single register instead of multiple?
    #TODO check that registers values are watts, and not kilowatts
    for register in inverter_registers:
        time = inverter_registers['time']
        pac_r_w = inverter_registers['pac_r_w']
        pac_s_w = inverter_registers['pac_s_w']
        pac_t_w = inverter_registers['pac_t_w']
        power_w = int(round(pac_r_w + pac_s_w + pac_t_w))
        energy_wh = int(round(inverter_registers['daily_energy_l_wh']))
        temperature_dc = int(round(inverter_registers['temp_inv_c']*100))

        reading = {
            'energy_wh': energy_wh,
            'power_w': power_w,
            'intensity_cc_mA': None,
            'intensity_ca_mA': None,
            'voltage_cc_mV': None,
            'voltage_ca_mV': None,
            'uptime_h': None,
            'temperature_dc': temperature_dc,
            'time': time,
        }

    inverter_registers_plantdata['readings'].append(reading)

    return inverter_registers_plantdata

def alcolea_registers_to_plantdata(devices_registers):

    plant_data = {
        'plant': 'Alcolea',
        'devices': []
    }

    for device_register in devices_registers:

        device_readings_packet = {}

        if 'name' not in device_register:
            logger.error("Device {} has no name".format(device_register))
            continue

        device_name = device_register['name']

        if 'type' not in device_register:
            logger.error("Device {} has no type".format(device_register))
            continue
        elif device_register['type'] == 'inverter':
            device_readings_packet = alcolea_inverter_to_plantdata(device_name, device_register['fields'])
        elif device_register['type'] == 'sensorTemperature':
            device_readings_packet = alcolea_sensorTemperature_to_plantadata(device_register['fields'])
        else:
            print("Unknown device type: {}".format(device_register['type']))
            continue

        plant_data['devices'].append(device_readings_packet)

    return plant_data

def registers_to_plant_data(plant_name, devices_registers):
    #TODO design this per-plant or per model
    if plant_name == 'Alcolea':
        return alcolea_registers_to_plantdata(devices_registers)
    else:
        print("Unknown plant {}".format(plant_name))

    return {}