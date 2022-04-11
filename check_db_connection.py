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

from pony import orm
from ORM.pony_manager import PonyManager

def check_db_connection():
    from conf import envinfo
    pony = PonyManager(envinfo.DB_CONF)

    pony.define_all_models()
    pony.binddb()

    pony.db.check_tables()

    with orm.db_session:
        plants = pony.db.Plant.select()[:].to_list()

    print(plants)

@click.command()
def check_db_connection_CLI():
    check_db_connection()


if __name__ == "__main__":
    check_db_connection_CLI()
