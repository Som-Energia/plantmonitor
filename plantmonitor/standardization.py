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

def monsol_meter_to_plantdata(meter_name, monsol_meter_readings):
    import pytz
    meter_plantdata = {'id': 'Meter:{}'.format(meter_name)}
    meter_plantdata['readings'] = [
        {
            'time':
                datetime.datetime.strptime(
                    reading['fecha_completa'], "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=pytz.timezone('Europe/Madrid')).astimezone(datetime.timezone.utc),
            'export_energy_wh': 1000*reading['eedia'],
            'import_energy_wh': 1000*reading['eidia'],
            'r1_VArh': 1000*reading['er1'],
            'r2_VArh': 1000*reading['er2'],
            'r3_VArh': 1000*reading['er3'],
            'r4_VArh': 1000*reading['er4'],
        }
        for reading in monsol_meter_readings
    ]
    return meter_plantdata

def alcolea_sensorTemperature_to_plantadata(alcolea_sensorTemperature_registers):
    logger.error("Not implemented yet")

    temperature_registers = {}
    return temperature_registers

def wattia_sensor_to_plantdata(sensor_name, wattia_sensor_registers):
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

    time = wattia_sensor_registers['time'] if 'time' in wattia_sensor_registers else datetime.datetime.now(datetime.timezone.utc)
    irradiation_w_m2 = int(round(wattia_sensor_registers['irradiance_dw_m2']*0.1))
    module_temperature_dc = wattia_sensor_registers['module_temperature_dc']
    module_temperature_dc = int(round((module_temperature_dc*0.1-25)*10))
    ambient_temperature_dc = wattia_sensor_registers['ambient_temperature_dc']
    ambient_temperature_dc = int(round((ambient_temperature_dc*0.1-25)*10))
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
    time = inverter_registers['time'] if 'time' in inverter_registers else datetime.datetime.now(datetime.timezone.utc)
    pac_r_dw = inverter_registers['pac_r_dw']
    pac_s_dw = inverter_registers['pac_s_dw']
    pac_t_dw = inverter_registers['pac_t_dw']
    power_w = 10*int(round(pac_r_dw + pac_s_dw + pac_t_dw))
    energy_wh = 100*int(round((inverter_registers['daily_energy_h_wh'] << 16) + inverter_registers['daily_energy_l_wh']))
    uptime_h = int(round((inverter_registers['h_total_h_h'] << 16) + inverter_registers['h_total_l_h']))
    temperature_dc = int(round(inverter_registers['temp_inv_dc']))
    strings = {
        'String:{}:intensity_mA'.format(r.split(':')[0]) : v*100
        for r,v in inverter_registers.items() if r.startswith('string')
    }

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
        **strings
    }

    inverter_registers_plantdata['readings'].append(reading)

    return inverter_registers_plantdata

def florida_inverter_to_plantdata(inverter_name, inverter_registers):
    inverter_registers_plantdata = {
        'id': 'Inverter:{}'.format(inverter_name),
        'readings': []
    }

    #TODO should we assume one single register instead of multiple?
    #TODO check that registers values are watts, and not kilowatts
    time = inverter_registers['time'] if 'time' in inverter_registers else datetime.datetime.now(datetime.timezone.utc)
    pac_r_dw = inverter_registers['pac_r_dw']
    pac_s_dw = inverter_registers['pac_s_dw']
    pac_t_dw = inverter_registers['pac_t_dw']
    power_w = 10*int(round(pac_r_dw + pac_s_dw + pac_t_dw))
    energy_wh = 100*int(round((inverter_registers['e_total_h_hwh'] << 16) + inverter_registers['e_total_l_hwh']))
    uptime_h = int(round((inverter_registers['h_total_h_h'] << 16) + inverter_registers['h_total_l_h']))
    temperature_dc = int(round(inverter_registers['temp_inv_dc']))
    strings = {
        'String:{}:intensity_mA'.format(r.split(':')[0]) : v*100
        for r,v in inverter_registers.items() if r.startswith('string')
    }

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
        **strings
    }

    inverter_registers_plantdata['readings'].append(reading)

    return inverter_registers_plantdata

def getFVstrings(inverter_registers):
    # TODO consider using sscanf to match string%d:intensity_dA
    strings = {}
    for r,v in inverter_registers.items():
        if r.startswith('string'):
            regname = r.split(':')[0]
            for i,sname in enumerate(regname.split('_')):
                intensity_dA = int((v >> 8*i) & 0xFF )
                fullsname = 'String:{}:intensity_mA'.format(sname)
                strings[fullsname] = intensity_dA*100
    return strings

def fontivsolar_inverter_to_plantdata(inverter_name, inverter_registers):
    inverter_registers_plantdata = {
        'id': 'Inverter:{}'.format(inverter_name),
        'readings': []
    }

    #TODO check that registers values are watts, and not kilowatts
    time = inverter_registers['time'] if 'time' in inverter_registers else datetime.datetime.now(datetime.timezone.utc)
    #TODO meld toghether Uint16 _h _l into Uint32
    energy_wh = 10*int(round((inverter_registers['DailyEnergy_dWh_h'] << 16) + inverter_registers['DailyEnergy_dWh_l']))
    power_w = int(round(inverter_registers['ActivePower_daW']*10))
    uptime_h = int(round((inverter_registers['Uptime_h_h'] << 16) + inverter_registers['Uptime_h_l']))
    temperature_dc = None

    strings = getFVstrings(inverter_registers)

    # we're filtering in the modmap, should we filter here instead?
    # disconnected_strings = ['string11', 'string12', 'string22', 'string23', 'string24']
    # connected_strings = {k:v for k,v in strings.items() if k not in disconnected_strings}
    # strings = connected_strings

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
        **strings
    }

    inverter_registers_plantdata['readings'].append(reading)

    return inverter_registers_plantdata

def erp_meter_readings_to_plant_data(measures):
    labels = ('time', 'export_energy_wh', 'import_energy_wh','r1_VArh','r2_VArh', 'r3_VArh','r4_VArh')
    return [
        {
            'time':datetime.datetime.strptime(
                    measure[0], "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=datetime.timezone.utc),
            'export_energy_wh': 1000*measure[1],
            'import_energy_wh': 1000*measure[2],
            'r1_VArh': 1000*measure[3],
            'r2_VArh': 1000*measure[4],
            'r3_VArh': 1000*measure[5],
            'r4_VArh': 1000*measure[6],
        }
        for measure in measures
    ]

# TODO improve this dictionaries merge
def string_inverter_registers_merge(devices_registers):
    '''
    for each inverter, look for their String readings and add them
    Discard the Strings devices

    You can assume one inverter will have one or None Strings device
    '''
    merged_devices_registers = []

    for dev1 in devices_registers:
        if dev1['type'] == 'inverter':
            for dev2 in devices_registers:
                if dev2['type'] == 'inverterStrings' and dev2['name'] == '{}Strings'.format(dev1['name']):
                    dev1['fields'].update(dev2['fields'])
            merged_devices_registers.append(dev1)
        elif dev1['type'] == 'inverterStrings':
            # filter out strings devices since we've merged them
            continue
        else:
            merged_devices_registers.append(dev1)

    return merged_devices_registers

def registers_to_plant_data(plant_name, plants_registers, generic_plant=False):

    #TODO design this per-plant or per model
    knownPlants = ['Fontivsolar', 'Alcolea', 'Florida', 'Matallana']
    if not generic_plant and plant_name not in knownPlants:
        logger.error("Unknown plant: {}. Known plants: 'Fontivsolar', 'Alcolea', 'Florida', 'Matallana'.".format(plant_name))
        return {}

    plant_data = {
        'plant': plant_name,
        'version': '1.0',
        'time': datetime.datetime.now(datetime.timezone.utc),
        'devices': []
    }

    if not plant_name in plants_registers:
        logger.error("Plant {} not in plant_register {}".format(plant_name, plants_registers))
        return {}

    # TODO continue here tomorrow
    # TODO make plant_data packet a class to reduce complexity
    plants_registers[plant_name] = string_inverter_registers_merge(plants_registers[plant_name])

    for device_register in plants_registers[plant_name]:

        if 'name' not in device_register:
            logger.error("Device {} has no name".format(device_register))
            continue

        if 'type' not in device_register:
            logger.error("Device {} has no type".format(device_register))
            continue

        if 'fields' not in device_register:
            logger.error("Missing 'fields' key in registers {}".format(device_register))
            continue

        device_name = device_register['name']

        # set plant_data time ni register if register doesn't have a time
        if not 'time' in device_register['fields']:
            device_register['fields']['time'] = plant_data['time']

        # TODO polymorphism and OO design here
        # TODO change from plant to inverter_model discriminator, since it's more real
        if device_register['type'] == 'inverter':
            if plant_name == 'Fontivsolar':
                device_readings_packets = [fontivsolar_inverter_to_plantdata(device_name, device_register['fields'])]
            elif plant_name == 'Florida':
                device_readings_packets = [florida_inverter_to_plantdata(device_name, device_register['fields'])]
            elif plant_name == 'Alcolea' or plant_name == 'Matallana' or generic_plant:
                device_readings_packets = [alcolea_inverter_to_plantdata(device_name, device_register['fields'])]
            else:
                logger.error("Unknown plant: {}. Known plants: 'Fontivsolar', 'Alcolea', 'Florida', 'Matallana'.".format(plant_name))
                continue
        elif device_register['type'] == 'sensorTemperature':
            logger.error("SensorTemperature Not implemented")
            continue
        elif device_register['type'] == 'wattiasensor':
            device_readings_packets = wattia_sensor_to_plantdata(device_name, device_register['fields'])
        else:
            logger.error("Unknown device type: {}".format(device_register['type']))
            continue

        plant_data['devices'] += device_readings_packets

    return plant_data