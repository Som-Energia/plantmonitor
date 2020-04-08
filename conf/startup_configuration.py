# -*- coding: utf-8 -*-
import logging
from influxdb import InfluxDBClient
from apscheduler.schedulers.blocking import BlockingScheduler
from plantmonitor.task import task
from plantmonitor.task import task_counter_erp

def build_app():
    try:

        scheduler = BlockingScheduler(
            timezone='GMT'
        )

    except Exception as e:
        msg = "An error ocurred building plant monitor: %s"
        logging.exception(msg, e)
        raise e

    logging.debug("Build app finished")
    return scheduler

def add_jobs(app):
    logging.debug("Adding task")
    app.add_job(task_counter_erp, 'interval', minutes=20)
    app.add_job(task_get_meteologica_forecast, 'interval', days=14)
