# -*- coding: utf-8 -*-
import conf.logging_configuration
from apscheduler.schedulers.blocking import BlockingScheduler
from plantmonitor.task import (
    task,
    task_counter_erp,
    task_meters_erp_to_orm,
    task_daily_upload_to_api_meteologica,
    task_daily_download_from_api_meteologica,
    task_daily_download_from_api_solargis,
    task_integral,
    task_maintenance,
)
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
        app.add_job(task_meters_erp_to_orm, 'interval', minutes=20)
        app.add_job(task_daily_upload_to_api_meteologica, 'cron', kwargs={'test_env':False}, hour=17, minute=30)
        app.add_job(task_daily_download_from_api_meteologica, 'cron', kwargs={'test_env':False}, hour=18, minute=30)
        app.add_job(task_maintenance, 'cron', minute="*/5")
        app.add_job(task_daily_download_from_api_solargis, 'cron', hour=3, minute=0)
        # deprecated by ponderated average
        # app.add_job(task_integral, 'cron', minute=20) # run at every *:20 of every hour
    else:
        logger.error("Environment not configured")


