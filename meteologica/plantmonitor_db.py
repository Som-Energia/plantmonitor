from yamlns import namespace as ns

import psycopg2

from psycopg2 import Error
import psycopg2.extras


class PlantmonitorDBError(Exception): pass

class PlantmonitorDBMock(object):

    #data structure: {facility: [('time', value)]}

    def __init__(self, config):
        self._data = {}
        self._facilityMeter = []
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

    def close(self):
        pass

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _clientOk(self):
        return self._client['errorCode'] == 'OK'

    def meterToFacility(self, meter):
        facilities = [f for f, m in self._facilityMeter if m is meter]
        return facilities[0] if facilities else None

    def facilityToMeter(self, facility):
        meters = [m for f, m in self._facilityMeter if f is facility]
        return meters[0] if meters else None

    def getMeterData(self):
        if not self._clientOk():
            return None
        return self._data

    def addMeterData(self, facilityMeterData):
        try:
            for f, meterData in facilityMeterData.items():
                meter = self.facilityToMeter(f)
                if not meter:
                    raise PlantmonitorDBError('Facility has no associated meter')
                self._data.setdefault(f, []).append(meterData)
        except:
            return {}
            #rollback?

        return facilityMeterData

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

    def getFacilityMeterDict(self):
        if not self._client:
            return None

        cur = self._client.cursor()
        cur.execute("select facilityid, meter from facility_meter;")
        facility_meter_db = cur.fetchall()

        facility_meter = set(facility_meter_db)
        return facility_meter

    def meterToFacility(self, meter):
        facility_meter = self.getFacilityMeterDict()
        facilities = [f for f, m in facility_meter if m is meter]
        return facilities[0] if facilities else None

    def facilityToMeter(self, facility):
        facility_meter = self.getFacilityMeterDict()
        meters = [m for f, m in facility_meter if f is facility]
        return meters[0] if meters else None

    def addMeterData(self, facilityMeterData):
        if not self._client:
            return None
        cur = self._client.cursor()

        try:
            for f,v in facilityMeterData.items():
                m = self.facilityToMeter(f)
                if not m:
                    raise PlantmonitorDBError('Facility has no associated meter')
                for t, e in v:
                    cur.execute(f"insert into sistema_contador VALUES('{m}', '{t}', {e});")
        except:
            return {}
            cur.rollback()
        
    def getMeterData(self):
        if not self._client:
            return None
        cur = self._client.cursor()
        cur.execute("select facilityid, time, export_energy from sistema_contador \
        inner join facility_meter on meter = name;")
        dbData = cur.fetchall()

        return self.dbToDictionary(dbData)
    
    def dbToDictionary(self, dbData):    
        data = {}
        for f,t,e in dbData:
            data.setdefault(f, []).append((t,e))

        return data