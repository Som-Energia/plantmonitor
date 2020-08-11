# -*- coding: utf-8 -*-
import conf.logging_configuration
from influxdb import InfluxDBClient
from apscheduler.schedulers.blocking import BlockingScheduler
from plantmonitor.task import task
from plantmonitor.task import task_counter_erp
from plantmonitor.task import task_get_meteologica_forecast
from plantmonitor.task import task_daily_upload_to_api_meteologica
from plantmonitor.task import task_daily_download_from_api_meteologica

from conf.config import env, env_active

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

def build_app():
    try:

        scheduler = BlockingScheduler(
            timezone='Europe/Madrid'
        )

    except Exception as e:
        msg = "An error ocurred building plant monitor: %s"
        logger.exception(msg, e)
        raise e

    logger.debug("Build app finished")
    return scheduler

def add_jobs(app):
    logger.debug("Adding task")
    if env_active == env['in_plant']:
        app.add_job(task, 'cron', minute='*/5')
    elif env_active == env['plantmonitor_server']:
        app.add_job(task_counter_erp, 'interval', minutes=20)
        app.add_job(task_daily_upload_to_api_meteologica, 'cron', kwargs={'test_env':True}, hour=18, minute=5)
        app.add_job(task_daily_download_from_api_meteologica, 'cron', kwargs={'test_env':True}, hour=19, minute=5)
    else:
        logger.error("Environment not configured")
