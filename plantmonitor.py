#!/usr/bin/env python3
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
from pymodbus.client.sync import ModbusTcpClient
from influxdb import InfluxDBClient
import conf.config as config
import conf.modmap as modmap
import json
import time
import datetime
import logging
import requests
from concurrent import futures
from threading import Semaphore

MIN_SIGNED   = -2147483648
MAX_UNSIGNED =  4294967295

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

requests.packages.urllib3.disable_warnings() 

logging.debug("Load config %s" % config.alcolea['model'])

logging.info("Load ModbusTcpClient")

client = ModbusTcpClient(config.alcolea['inverter_ip'][0],
                         timeout=config.alcolea['timeout'],
                         RetryOnEmpty=True,
                         retries=3,
                         port=config.alcolea['inverter_port'])
logging.info("Connect")
client.connect()

try:
  flux_client = InfluxDBClient(config.influx['influxdb_ip'],
                               config.influx['influxdb_port'],
                               config.influx['influxdb_user'],
                               config.influx['influxdb_password'],
                               config.influx['influxdb_database'],
                               ssl=config.influx['influxdb_ssl'],
                               verify_ssl=config.influx['influxdb_verify_ssl'])
except:
  flux_client = None

inverter = {}
bus = json.loads(modmap.alcolea['scan'])

def load_registers(type,start,COUNT=100):
  try:
    if type == "read":
      rr = client.read_input_registers(int(start), 
                                       count=int(COUNT), 
                                       unit=config.alcolea['slave'][0])
    elif type == "holding":
      rr = client.read_holding_registers(int(start), 
                                         count=int(COUNT), 
                                         unit=config.alcolea['slave'][0])
    for num in range(0, int(COUNT)):
      run = int(start) + num + 1
      if type == "read" and modmap.alcolea['read_register'].get(str(run)):
        if '_10' in modmap.alcolea['read_register'].get(str(run)):
          inverter[modmap.alcolea['read_register'].get(str(run))[:-3]] = float(rr.registers[num])/10
        else:
          inverter[modmap.alcolea['read_register.get'](str(run))] = rr.registers[num]
      elif type == "holding" and modmap.alcolea['holding_register'].get(str(run)):
        inverter[modmap.alcolea['holding_register'].get(str(run))] = rr.registers[num]
      run = run + 1
  except Exception as err:
      logging.error("[ERROR] %s" % err)

def publish_influx(metrics,sem):
  with sem:
    target=flux_client.write_points([metrics])
    logging.info("[INFO] Sent to InfluxDB")

while True:
  try:
    inverter = {}
 
    for i in bus['holding']:
      load_registers("holding",i['start'],i['range']) 
      
    logging.info(inverter)

    if flux_client is not None:
      metrics = {}
      tags = {}
      fields = {}
      metrics['measurement'] = "Aros-solar"
      tags['location'] = "Alcolea"
      metrics['tags'] = tags
      metrics['fields'] = inverter

      with futures.ThreadPoolExecutor(max_workers=1) as executor:
        to_do = []
        sem = Semaphore()
        future = executor.submit(publish_influx, metrics, sem)
        to_do.append(future)
        msg = 'Scheduled for {}: {}'
        logging.debug(msg.format(metrics, future))
        results = []
        for future in futures.as_completed(to_do):
          res = future.result()
          msg = '{} result: {!r}'
          logging.info(msg.format(future, res))
          results.append(res)

  except Exception as err:
    logging.error("[ERROR] %s" % err)
    client.close()
    client.connect()
  time.sleep(config.alcolea['scan_interval'])