from yamlns import namespace as ns
from pymodbus.client.sync import ModbusTcpClient
import sys
from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

class ProductionPlant():
    def __init__(self):
        self.id = None
        self.name = None
        self.description = None
        self.enable = None
        self.location = None
        self.devices = []

    def load(self, yamlFile,plant_name):
        data = ns.load(yamlFile)
        for plant_data in data.plantmonitor:
            if plant_data.enabled and plant_data.name == plant_name:
                self.name = plant_data.name
                self.description = plant_data.description
                for device_data in plant_data.devices:
                    new_device = ProductionDevice()
                    if new_device.load(device_data):
                        self.devices.append(new_device)

        return True

    def get_registers(self):
        all_metrics = []
        for device in self.devices:
            try:
                data = {}
                metrics = device.get_registers()
                data[self.name] = metrics
                all_metrics.append(data)
            except Exception as e:
                msg = "An error ocurred getting registers from inverter: %s"
                logger.exception(msg, e)
        return all_metrics


class ProductionDevice():
    def __init__(self):
        self.id = None
        self.name = None
        self.description = None
        self.modelo = None
        self.enable = None
        self.modmap = {}
        self.protocol = None

    def load(self, device_data):
        self.name = device_data.name
        self.description = device_data.description
        self.model = device_data.model
        self.enabled = device_data.enabled
        for item_data in device_data.modmap:
            dev = ProductionDeviceModMap.factory(item_data)
            key = dev.load(item_data)
            if key:
                self.modmap[key] = dev

        self.protocol = ProductionProtocol.factory(device_data.protocol)
        return self.enabled

    def get_registers(self):
        registers = []
        for key,dev in self.modmap.items():
            inverter = dev.get_registers(self.protocol)
            metrics = {}
            metrics['inverter'] = self.name
            metrics['model'] = self.model
            metrics['register_type'] = dev.type
            metrics['fields'] = inverter
            registers.append(metrics)
        return registers

class ProductionProtocol():
    def __init__(self):
        self.type = None
        self.description = None

    def factory(protocol_data):
        if protocol_data.type == "TCP":
            new_protocoltcp = ProductionProtocolTcp()
            new_protocoltcp.load(protocol_data)
            return new_protocoltcp

        if protocol_data.type == "RS485":
            new_protocolRs = ProductionProtocolRs()
            new_protocolRs.load(protocol_data)
            return new_protocolRs

        return None

    factory = staticmethod(factory)

class ProductionProtocolTcp(ProductionProtocol):
    def __init__(self):
        super().__init__()
        self.ip = None
        self.port = None
        self.slave = None
        self.timeout = None
        self.client = None

    def load(self, protocol_data):
        self.ip = protocol_data.ip
        self.port = protocol_data.port
        self.timeout = protocol_data.timeout
        self.slave = protocol_data.slave
        self.client = None
        return True

    def connect(self):
        if not self.client:
            logger.info("stablishing connection - %s" % self.ip)
            client = ModbusTcpClient(self.ip,
                                    timeout=self.timeout,
                                    RetryOnEmpty=True,
                                    retries=3,
                                    port=self.port)
            logger.info("Connect")
            client.connect()
            self.client = client
        else:
            logger.info("connection already stablished")

    def disconnect(self):
        logger.info("closing connection")
        if self.client:
           self.client.close()
        self.client = None

class ProductionProtocolRs(ProductionProtocol):
    def __init__(self):
        super().__init__()
        self.baudrate = None

    def load(self, protocol_data):
        self.baud_rate = protocol_data.baud_rate
        return True

class ProductionDeviceModMap():
    def __init__(self):
        self.registers = {}
        self.scan = {}

    def factory(modmap_data):
        if modmap_data.type == "holding_registers":
            new_modmap = ProductionDeviceModMapHoldingRegisters()
            new_modmap.load(modmap_data)
            return new_modmap

        if modmap_data.type == "coils":
            new_modmap = ProductionDeviceModMapCoils()
            new_modmap.load(modmap_data)
            return new_modmap

        if modmap_data.type == "discrete_input":
            new_modmap = ProductionDeviceModMapDiscreteInput()
            new_modmap.load(modmap_data)
            return new_modmap

        if modmap_data.type == "input_registers":
            new_modmap = ProductionDeviceModMapInputRegisters()
            new_modmap.load(modmap_data)
            return new_modmap

        return None

    factory = staticmethod(factory)

    def load(self, modmap_data):
        self.type = modmap_data.type
        self.registers = modmap_data.registers
        self.scan = modmap_data.scan
        return self.type

    def extract_rr(self, rr_obj, offset):
        logger.info("registers values from inverter %s" % rr_obj.registers)
        register_values = rr_obj.registers

        return ns(
            (name, register_values[index-offset])
            for index, name in self.registers.items()
            )

    def get_registers(self, connection):
        connection.connect()
        logger.info("getting registers from inverter")
        rr = self.get_register_values(connection)
        logger.debug(rr)
        connection.disconnect()
        return self.extract_rr(rr, self.scan.start)

    def get_register_values(self, connection):
        raise NotImplementedError()


class ProductionDeviceModMapHoldingRegisters(ProductionDeviceModMap):
    def get_register_values(self, connection):
        return connection.client.read_holding_registers(
            self.scan.start,
            count=self.scan.range,
            unit=connection.slave,
            )

class ProductionDeviceModMapInputRegisters(ProductionDeviceModMap):
    def get_registers(self, connection):
        logger.info("Input Register is not implemented")
        return {}

class ProductionDeviceModMapCoils(ProductionDeviceModMap):
    def get_registers(self, connection):
        logger.info("Coils is not implemented")
        return {}

class ProductionDeviceModMapDiscreteInput(ProductionDeviceModMap):
    def get_registers(self, connection):
        logger.info("Discrete Input is not implemented")
        return {}
