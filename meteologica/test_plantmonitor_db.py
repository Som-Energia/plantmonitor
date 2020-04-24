from yamlns import namespace as ns

class PlantmonitorDB:

    def __init__(self, **kwds):
        self._config = ns(kwds)


    