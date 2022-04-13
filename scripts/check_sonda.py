#!/home/plantmonitor/Envs/logger/bin/python3
import os, sys

from pymodbus.client.sync import ModbusTcpClient

client = ModbusTcpClient('192.168.2.217', port=502)
connection = client.connect()

if connection:
    # TODO check if recive values from address, new sys alert
    # request = client.read_holding_registers(37,count=3,unit=1)
    request = client.read_input_registers(0,count=3,unit=37)
    result = request.registers
    print("OK")
    sys.exit(0)
else:
    print("KO")
    sys.exit(2)


#if used_space < "85%":
#   print ("OK - %s of disk space used." % used_space)
#   sys.exit(0)
#elif used_space == "85%":
#   print ("WARNING - %s of disk space used." % used_space)
#   sys.exit(1)
#elif used_space > "85%":
#   print ("CRITICAL - %s of disk space used." % used_space)
#   sys.exit(2)
#else:
#   print ("UKNOWN - %s of disk space used." % used_space)
#   sys.exit(3)
