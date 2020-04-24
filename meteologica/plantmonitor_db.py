from yamlns import namespace as ns

class PlantmonitorDBMock(object):

    def __init__(self):
        self._data = {}
        self._session = {}
        self._login = {}
        self._response = {}
        self._validPlants = "MyPlant OtherPlant".split()

    def login(self, username, password):
        self._login = dict(
            username = username,
            password = password,
        )
        if self._login['username'] != 'alberto' or self._login['password'] != '1234':
            self._session = {
                    'errorCode': 'INVALID_USERNAME_OR_PASSWORD',
                    'header': {
                        'sessionToken': None,
                        'errorCode': 'NO_SESSION'
                        }
                    }
            return self._session
        else:
            self._session = {
                    'errorCode': 'OK',
                    'header': {
                    'sessionToken': '73f19a710bbced25fb2e46e5a0d65b126716a8d19bb9f9038d2172adc14665a5f0c65a30e9fb677e5654b2f59f51abdb',
                    'errorCode': 'OK'
                        }
                    }
            return self._session
    