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

def alcolea_sensorTemperature_to_plantadata(alcolea_sensorTemperature_registers):
    logger.error("Not implemented yet")

    temperature_registers = {}
    return temperature_registers

def wattia_sensor_to_plantadata(sensor_name, wattia_sensor_registers):
    # TODO generalize this for an arbitrary number of ORM sensors in the register
    MultiSensor_registers_plantdata = [{
            'id': 'SensorIrradiation:{}'.format(sensor_name),
            'readings': []
        },
        {
            'id': 'SensorTemperatureModule:{}'.format(sensor_name),
            'readings': []
        },
        {
            'id': 'SensorTemperatureAmbient:{}'.format(sensor_name),
            'readings': []
        }
    ]

    time = wattia_sensor_registers['time'] if 'time' in wattia_sensor_registers else datetime.datetime.utcnow()
    irradiation_w_m2 = int(round(wattia_sensor_registers['irradiance_dw_m2']*0.1))
    module_temperature_dc = wattia_sensor_registers['module_temperature_dc']
    module_temperature_dc = int(round((module_temperature_dc*0.1-25)*100))
    ambient_temperature_dc = wattia_sensor_registers['ambient_temperature_dc']
    ambient_temperature_dc = int(round((ambient_temperature_dc*0.1-25)*100))
    # TODO aclarir si la temperatura de mòdul és la temperatura de sonda
    irradiation_reading = {
        'irradiation_w_m2': irradiation_w_m2,
        'temperature_dc': module_temperature_dc,
        'time': time,
    }
    module_reading = {
        'temperature_dc': module_temperature_dc,
        'time': time,
    }
    ambient_reading ={
        'temperature_dc': ambient_temperature_dc,
        'time': time,
    }

    MultiSensor_registers_plantdata[0]['readings'].append(irradiation_reading)
    MultiSensor_registers_plantdata[1]['readings'].append(module_reading)
    MultiSensor_registers_plantdata[2]['readings'].append(ambient_reading)

    return MultiSensor_registers_plantdata

def alcolea_inverter_to_plantdata(inverter_name, inverter_registers):
    inverter_registers_plantdata = {
        'id': 'Inverter:{}'.format(inverter_name),
        'readings': []
    }

    #TODO should we assume one single register instead of multiple?
    #TODO check that registers values are watts, and not kilowatts
    time = inverter_registers['time'] if 'time' in inverter_registers else datetime.datetime.utcnow()
    pac_r_w = inverter_registers['pac_r_w']
    pac_s_w = inverter_registers['pac_s_w']
    pac_t_w = inverter_registers['pac_t_w']
    power_w = int(round(pac_r_w + pac_s_w + pac_t_w))
    energy_wh = int(round((inverter_registers['daily_energy_h_wh'] << 16) + inverter_registers['daily_energy_l_wh']))
    uptime_h = int(round((inverter_registers['h_total_h_h'] << 16) + inverter_registers['h_total_l_h']))
    temperature_dc = int(round(inverter_registers['temp_inv_dc']))

    reading = {
        'energy_wh': energy_wh,
        'power_w': power_w,
        'intensity_cc_mA': None,
        'intensity_ca_mA': None,
        'voltage_cc_mV': None,
        'voltage_ca_mV': None,
        'uptime_h': uptime_h,
        'temperature_dc': temperature_dc,
        'time': time,
    }

    inverter_registers_plantdata['readings'].append(reading)

    return inverter_registers_plantdata

def alcolea_registers_to_plantdata(plant_registers, plantName='Alcolea'):

    plant_data = {
        'plant': plantName,
        'version': '1.0',
        'time': datetime.datetime.utcnow(),
        'devices': []
    }

    for plant_register in plant_registers:
        if not plantName in plant_register:
           logger.error("Plant {} not in plant_register {}".format(plantName, plant_register))
        else:
            for device_register in plant_register[plantName]:
                device_readings_packet = {}

                if 'name' not in device_register:
                    logger.error("Device {} has no name".format(device_register))
                    continue

                device_name = device_register['name']

                if 'type' not in device_register:
                    logger.error("Device {} has no type".format(device_register))
                    continue
                elif device_register['type'] == 'inverter':
                    device_readings_packets = [alcolea_inverter_to_plantdata(device_name, device_register['fields'])]
                elif device_register['type'] == 'sensorTemperature':
                    logger.error("SensorTemperature Not implemented")
                elif device_register['type'] == 'wattiasensor':
                    if 'fields' not in device_register:
                        logger.error("Missing 'fields' key in registers {}".format(device_register))
                        continue
                    if not 'time' in device_register['fields']:
                        device_register['fields']['time'] = plant_data['time']
                    device_readings_packets = wattia_sensor_to_plantadata(device_name, device_register['fields'])
                else:
                    logger.error("Unknown device type: {}".format(device_register['type']))
                    continue

                plant_data['devices'] += device_readings_packets

    return plant_data

def fontivsolar_sensorTemperature_to_plantadata(sensor_name, sensorTemperature_registers):
    logger.error("To be implemented")


def fontivsolar_inverter_to_plantdata(inverter_name, inverter_registers):
    inverter_registers_plantdata = {
        'id': 'Inverter:{}'.format(inverter_name),
        'readings': []
    }

    #TODO should we assume one single register instead of multiple?
    #TODO check that registers values are watts, and not kilowatts
    for register in inverter_registers:
        time = inverter_registers['time'] if 'time' in inverter_registers else datetime.datetime.utcnow()
        #TODO meld toghether Uint16 _h _l into Uint32
        energy_wh = int(round((inverter_registers['DailyEnergy_dWh_h'] << 16) + inverter_registers['DailyEnergy_dWh_l']))*10
        power_w = int(round(inverter_registers['ActivePower_dW']/10))
        uptime_h = int(round((inverter_registers['Uptime_h_h'] << 16) + inverter_registers['Uptime_h_l']))
        temperature_dc = None

        reading = {
            'energy_wh': energy_wh,
            'power_w': power_w,
            'intensity_cc_mA': None,
            'intensity_ca_mA': None,
            'voltage_cc_mV': None,
            'voltage_ca_mV': None,
            'uptime_h': uptime_h,
            'temperature_dc': temperature_dc,
            'time': time,
        }

    inverter_registers_plantdata['readings'].append(reading)

    return inverter_registers_plantdata

# TODO this function will always be the same accros plants
def fontivsolar_registers_to_plantdata(plant_registers):

    plantName='Fontivsolar'

    plant_data = {
        'plant': plantName,
        'version': '1.0',
        'time': datetime.datetime.utcnow(),
        'devices': []
    }
    for plant_register in plant_registers:
        if not plantName in plant_register:
           logger.error("Plant {} not in plant_register {}".format(plantName, plant_register))
        else:
            for device_register in plant_register[plantName]:
                device_readings_packet = {}

                if 'name' not in device_register:
                    logger.error("Device {} has no name".format(device_register))
                    continue

                device_name = device_register['name']

                if 'type' not in device_register:
                    logger.error("Device {} has no type".format(device_register))
                    continue
                elif device_register['type'] == 'inverter':
                    device_readings_packet = fontivsolar_inverter_to_plantdata(device_name, device_register['fields'])
                elif device_register['type'] == 'sensorTemperature':
                    device_readings_packet = fontivsolar_sensorTemperature_to_plantadata(device_register['fields'])
                else:
                    print("Unknown device type: {}".format(device_register['type']))
                    continue

                plant_data['devices'].append(device_readings_packet)

    return plant_data

def erp_meter_readings_to_plant_data(measures):
    labels = ('time', 'export_energy_wh', 'import_energy_wh','r1_VArh','r2_VArh', 'r3_VArh','r4_VArh')
    return [{**{k:v for k,v in zip(labels, measure)},**{"time":datetime.datetime.strptime(
        measure[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)}} for measure in measures]

def registers_to_plant_data(plant_name, devices_registers):
    #TODO design this per-plant or per model
    if plant_name == 'Alcolea' or plant_name == 'Florida':
        return alcolea_registers_to_plantdata(devices_registers, plantName=plant_name)
    if plant_name == 'Fontivsolar':
        return fontivsolar_registers_to_plantdata(devices_registers)
    else:
        logger.error("Unknown plant {}".format(plant_name))

    return {}
