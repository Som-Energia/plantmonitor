# -*- coding: utf-8 -*-
from pymodbus.client.sync import ModbusTcpClient
from influxdb import InfluxDBClient
from plantmonitor.resource import ProductionPlant 
from yamlns import namespace as ns
import conf.config as config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def client_db():
    try:
        logging.info("[INFO] Connecting to Influxdb")
        flux_client = InfluxDBClient(config.influx['influxdb_ip'],
                                config.influx['influxdb_port'],
                                config.influx['influxdb_user'],
                                config.influx['influxdb_password'],
                                config.influx['influxdb_database'],
                                ssl=config.influx['influxdb_ssl'],
                                verify_ssl=config.influx['influxdb_verify_ssl'])
        logging.debug("Load config %s" % flux_client)
    except:
        flux_client = None

    return flux_client

def publish_influx(metrics,flux_client):
    target=flux_client.write_points([metrics])
    logging.info("[INFO] Sent to InfluxDB")

def task():
    try:
        
        import sys
        
        plant = ProductionPlant()
        
        if not plant.load('conf/modmap.yaml','Alcolea'):
            logging.error('Error loadinf yaml definition file...')
            sys.exit(-1)
        
        result = plant.get_registers()


        print(result['Alcolea'][0]['fields'])

        # flux_client = client_db()

        # if flux_client is not None:
        #     metrics = {}
        #     tags = {}
        #     fields = {}
        #     metrics['measurement'] = "Aros-solar"
        #     tags['location'] = "Alcolea"
        #     metrics['tags'] = tags
        #     metrics['fields'] = inverter

        #     publish_influx(metrics,flux_client)

        # client.close()

    except Exception as err:
        logging.error("[ERROR] %s" % err)
