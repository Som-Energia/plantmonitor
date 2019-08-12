# -*- coding: utf-8 -*-
from pymodbus.client.sync import ModbusTcpClient
from influxdb import InfluxDBClient
from plantmonitor.resource import ProductionPlant 
from yamlns import namespace as ns
import sys
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def client_db(db):
    try:
        logging.info("[INFO] Connecting to Influxdb")
        flux_client = InfluxDBClient(db['influxdb_ip'],
                                db['influxdb_port'],
                                db['influxdb_user'],
                                db['influxdb_password'],
                                db['influxdb_database'],
                                ssl=db['influxdb_ssl'],
                                verify_ssl=db['influxdb_verify_ssl'])
        logging.debug("Load config %s" % flux_client)
    except:
        flux_client = None

    return flux_client

def publish_influx(metrics,flux_client):
    flux_client.write_points([metrics])
    logging.info("[INFO] Sent to InfluxDB")

def task():
    try:
        
        plant = ProductionPlant()
        
        if not plant.load('conf/modmap.yaml','Alcolea'):
            logging.error('Error loadinf yaml definition file...')
            sys.exit(-1)
        
        flux_client = client_db(plant.db)

        result = plant.get_registers()

        plant_name = plant.name

        for i, device in enumerate(plant.devices):
            inverter_name = plant.devices[i].name
            inverter_registers = result[i]['Alcolea'][0]['fields']

            logging.info("**** Saving data in database ****")
            logging.info("**** Metrics - tag - %s ****" %  inverter_name)
            logging.info("**** Metrics - tag - location %s ****" % plant_name)
            logging.info("**** Metrics - fields -  %s ****" % inverter_registers)

            if flux_client is not None:
                metrics = {}
                tags = {}
                fields = {}
                metrics['measurement'] = 'sistema_inversor'
                tags['location'] = plant_name
                tags['inverter_name'] = inverter_name
                metrics['tags'] = tags
                metrics['fields'] = inverter_registers

                publish_influx(metrics,flux_client)

    except Exception as err:
        logging.error("[ERROR] %s" % err)
