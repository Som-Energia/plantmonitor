#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')

import datetime
import click
from yamlns import namespace as ns

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from pony import orm
from ORM.pony_manager import PonyManager
from ORM.models import (
    importPlants,
)

# yaml = ns.loads("""\
#                 municipalities:
#                 - municipality:
#                     name: Figueres
#                     countryCode : ES
#                     country : Spain
#                     regionCode : '09'
#                     region : Cataluña
#                     provinceCode : '17'
#                     province : Girona
#                     ineCode : '17066'
#                 plants:
#                 - plant:
#                     name: alcolea
#                     codename: SCSOM04
#                     description: la bonica planta
#                     meters:
#                     - meter:
#                         name: '1234578'
#                     inverters:
#                     - inverter:
#                         name: '5555'
#                     - inverter:
#                         name: '6666'
#                     irradiationSensors:
#                     - irradiationSensor:
#                         name: alberto
#                     temperatureSensors:
#                     - temperatureSensor:
#                         name: joana
#                     integratedSensors:
#                     - integratedSensor:
#                         name: voki""")

def importPlantsFromFile(db, yamlFilename):
    nsplants = ns.load(yamlFilename)

    with orm.db_session:
        importPlants(db, nsplants)

@click.command()
@click.argument('yaml', type=click.Path(exists=True))
def importPlantCLI(yaml):
    yamlFilename = click.format_filename(yaml)
    logger.debug(yamlFilename)

    from conf import envinfo
    pony = PonyManager(envinfo.DB_CONF)

    pony.define_all_models()
    pony.binddb(create_tables=False)

    importPlantsFromFile(pony.db, yamlFilename)

if __name__ == "__main__":
    importPlantCLI()
