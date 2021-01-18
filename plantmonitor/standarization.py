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

#TODO move this function, create its tests and finish it
# WIP Prototype
def alcolea_registers_to_plantdata(registers):

    plant_data = {}

    for i, device in enumerate(plant.devices):
        inverter_name = plant.devices[i].name
        inverter_registers = result[i]['Alcolea'][0]['fields']

        pac_r_w = inverter_registers["pac_r_w"]
        ...
        power_w = pac_r_w + pac_s_w + pac_t_w

        plant_data[plant_name][inverter_name]["power_w"] = power_w

    return plant_data