from yamlns import namespace as ns

import psycopg2

from psycopg2 import Error
import psycopg2.extras

class PlantmonitorDBMock(object):

    #data structure: {facility: [('time', value)]}

    def __init__(self, config):
        self._data = {}
        self._config = config
        self._client = {'errorCode': 'DISCONNECTED'}

    def login(self):
        if self._config['psql_user'] != 'alberto' or self._config['psql_password'] != '1234':
            self._client = {
                    'errorCode': 'INVALID_USERNAME_OR_PASSWORD',
                    }
            return self._client
        else:
            self._client = {
                    'errorCode': 'OK',
                    }
            return self._client

    def _clientOk(self):
        return self._client['errorCode'] == 'OK'

    def getMeterData(self):
        if not self._clientOk():
            return None
        return self._data

    def add(self, facility, MeterData):
        self._data[facility] = MeterData
        return {facility, self._data[facility]}

class PlantmonitorDB:

    def __init__(self, config): 
        self._config = config
        self._client = None

    def login(self):
        conn = psycopg2.connect(user = self._config['psql_user'], password = self._config['psql_password'],
        host = self._config['psql_host'], port = self._config['psql_port'], database = self._config['psql_db'])
        self._client = conn
        if conn.close == 1:
            return {'errorCode': 'CONNECTION CLOSED'}
        else:
            return {'errorCode': 'OK'}

    def close(self):
        self._client.close()

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def add(self, facilityMeterData):
        pass

    def getMeterData(self):
        if not self._client:
            return None
        cur = self._client.cursor()
        cur.execute("select facility, time, production from sistema.contador;")
        data = cur.fetchall()
        return data
        