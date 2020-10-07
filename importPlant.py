#!/usr/bin/env python
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

from ORM.models import database

from ORM.orm_util import setupDatabase, dropTables

from pony import orm

from ORM.models import (
    Plant,
)

# yaml = ns.loads("""\
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

def importPlant(yamlFilename):
    nsplants = ns.load(yamlFilename)

    with orm.db_session:
        alcoleaPlant = nsplants.plants[0].plant
        alcolea = Plant(name=alcoleaPlant.name, codename=alcoleaPlant.codename)
        alcolea = alcolea.importPlant(nsplants)

@click.command()
@click.argument('yaml', type=click.Path(exists=True))
def importPlantCLI(yaml):
    yamlFilename = click.format_filename(yaml)
    logger.debug(yamlFilename)

    setupDatabase(create_tables=False, timescale_tables=False, drop_tables=False)

    importPlant(yamlFilename)

if __name__ == "__main__":
    importPlantCLI()
