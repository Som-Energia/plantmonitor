from yamlns import namespace as ns
import conf.modmap


class ProductionPlant():
    def __init__(self):
        self.id = 
        self.name =
        self.description =
        self.enable =
        self.location =
        devices = []

class ProductionDevice():
    def __init__(self):
        self.id =
        self.name =
        self.description =
        self.model =
        self.enable =

class ProductionProtocol():
    def __init__(self):
        self.id =
        self.name =
        self.description =
        self.puerto =

class ProductionProtocolTcp(ProductionProtocol):
    def __init__(self):
        super().__init__(id,name,description,puerto)
        self.ip =
        self.puerto =

class ProductionProtocolRs(ProductionProtocol):
    def __init__(self):
        self.baudrate =
