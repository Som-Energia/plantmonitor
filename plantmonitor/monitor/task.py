# -*- coding: utf-8 -*-
from plantmonitor.monitor.resource import ProductionPlant
from yamlns import namespace as ns

from maintenance.maintenance import(
    bucketed_registry_maintenance,
    alarm_maintenance,
    cleaning_maintenance
)

from maintenance.db_manager import DBManager

from .operations import computeIntegralMetrics

from plantmonitor.forecasts import (
    downloadMeterForecasts,
    uploadMeterReadings
)

import sys
import psycopg2

from conf import envinfo

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from plantmonitor.monitor.storage import (
    PonyMetricStorage,
    ApiMetricStorage,
)

from pony import orm

from .standardization import registers_to_plant_data
from .readings_facade import ReadingsFacade

from plantmonitor.ORM.pony_manager import PonyManager

def publish_timescale(plant_name, inverter_name, metrics, db):
    with psycopg2.connect(
            user=db['user'], password=db['password'],
            host=db['host'], port=db['port'], database=db['database']
        ) as conn:
        with conn.cursor() as cur:
            measurement    = 'sistema_inversor'
            query_content  = ', '.join(metrics['fields'].keys())
            values_content = ', '.join(["'{}'".format(v) for v in metrics['fields'].values()])

            cur.execute(
                "INSERT INTO {}(time, inverter_name, location, {}) \
                VALUES (timezone('utc',NOW()), '{}', '{}', {});".format(
                    measurement,query_content,
                    inverter_name,plant_name,values_content
                )
            )

def get_plant_reading(plant):

    plants_registers = plant.get_registers()

    logger.debug("plants Registers: {}".format(plants_registers))

    if len(plants_registers) == 0:
        logger.error("plant Registers is empty: {}".format(plants_registers))
        return None

    if plant.name not in plants_registers:
        logger.error("plant {} is not in registers {}. Check the plant modmap.".format(plant.name, plants_registers.keys()))
        return None

    plant_data = registers_to_plant_data(plant.name, plants_registers)

    if not plant_data:
        logger.error("Registers to plant data returned {}".format(plant_data))
        return None

    return plant_data

def task():
    try:

        plantname = envinfo.ACTIVEPLANT_CONF['activeplant']

        plant = ProductionPlant()

        apiconfig = envinfo.API_CONFIG

        if not plant.load('conf/modmap.yaml', plantname):
            logger.error('Error loading yaml definition file...')
            sys.exit(-1)

        plant_data = get_plant_reading(plant)

        if not plant_data:
            logger.error("Getting reading from {} failed. Aborting.".format(plantname))
            return

        pony_manager = PonyManager(envinfo.DB_CONF)
        pony_manager.define_all_models()
        pony_manager.binddb()
        ponyStorage = PonyMetricStorage(pony_manager.db)
        apiStorage = ApiMetricStorage(apiconfig)

        logger.info("**** Saving data in database ****")
        logger.debug("plant_data: {}".format(plant_data))
        ponyStorage.insertPlantData(plant_data)

        logger.info("**** Saving data in Api ****")
        result = apiStorage.insertPlantData(plant_data)
        logger.debug("api response: {}".format(result))

    except Exception as err:
        logger.exception("[ERROR] %s" % err)

def task_meters_erp_to_orm():

    pony = PonyManager(envinfo.DB_CONF)

    pony.define_all_models()
    pony.binddb(create_tables=False)
    r = ReadingsFacade(pony.db)

    try:
        # TODO mock measures or fake meters
        r.transfer_ERP_readings_to_model()
    except Exception as err:
        logger.error("[ERROR] %s" % err)
        raise

def task_daily_upload_to_api_meteologica(test_env=True):

    pony = PonyManager(envinfo.DB_CONF)

    pony.define_all_models()
    pony.binddb(create_tables=False)

    configdb = ns.load('conf/config_meteologica.yaml')
    with orm.db_session:
        uploadMeterReadings(pony.db, configdb, test_env=test_env)


def task_daily_download_from_api_meteologica(test_env=True):

    pony = PonyManager(envinfo.DB_CONF)

    pony.define_all_models()
    pony.binddb(create_tables=False)

    configdb = ns.load('conf/config_meteologica.yaml')
    with orm.db_session:
        downloadMeterForecasts(pony.db, configdb, test_env=test_env)


def task_integral():
    with orm.db_session:
        computeIntegralMetrics()

def task_daily_download_from_api_solargis():
    from external_api.api_solargis import ApiSolargis
    ApiSolargis.daily_download_readings()

def task_maintenance():
    try:
        database_info = envinfo.DB_CONF
        with DBManager(**database_info) as dbmanager:
            with dbmanager.db_con.begin():
                bucketed_registry_maintenance(dbmanager.db_con)
            with dbmanager.db_con.begin():
                alarm_maintenance(dbmanager.db_con)
            with dbmanager.db_con.begin():
                cleaning_maintenance(dbmanager.db_con)
    except Exception as err:
        logger.error("[ERROR] %s" % err)
        raise
