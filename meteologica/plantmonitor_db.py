from yamlns import namespace as ns

import psycopg2

from psycopg2 import OperationalError, Error
import psycopg2.extras

import decorator

import copy

class PlantmonitorDBError(Exception): pass

class PlantmonitorDBMock(object):

    #data structure: {facility: [('time', value)]}

    def __init__(self, config):
        self._data = {}
        self._enterdata = {}
        self._facilityMeter = set()
        self._config = config
        self._client = None
        self._withinContextManager = False

    def login(self):
        if self._config['psql_user'] != 'alberto' or self._config['psql_password'] != '1234':
            raise PlantmonitorDBError("Invalid username or password")
        return self._client

    def close(self):
        pass

    def __enter__(self):
        self._withinContextManager = True
        self.login()
        self._enterdata = self._data
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self._data = self._enterdata
        self.close()

    @decorator.decorator
    def withinContextManager(f, self, *args, **kwds):
        if self._withinContextManager:
            return f(self, *args, **kwds)

        try:
            result = f(self, *args, **kwds)
            return result
        except Exception as exp:
            self._client.rollback()
            raise exp
        finally:
            self.close()

    def meterToFacility(self, meter):
        facilities = [f for f, m in self._facilityMeter if m is meter]
        return facilities[0] if facilities else None

    def facilityToMeter(self, facility):
        meters = [m for f, m in self._facilityMeter if f is facility]
        return meters[0] if meters else None

    def getMeterData(self):
        return self._data

    @withinContextManager
    def addFacilityMeterRelation(self, facility, meter):
        self._facilityMeter.add((facility, meter))

    def getFacilityMeter(self):
        return self._facilityMeter

    def facilityMeterRelationExists(self, facility, meter):
        facility_meter = self.getFacilityMeter()
        return (facility, meter) in facility_meter

    def addMeterData(self, facilityMeterData):
        data_uncommited = copy.deepcopy(self._data)
        for f, meterData in facilityMeterData.items():
            meter = self.facilityToMeter(f)
            if not meter:
                raise PlantmonitorDBError('Facility has no associated meter')
            data_uncommited.setdefault(f, []).extend(meterData)
        self._data = copy.deepcopy(data_uncommited)
        return facilityMeterData

class PlantmonitorDB:

    #lingua franca: {facility: [('time', value)]}

    def __init__(self, config):
        self._config = config
        self._client = None
        self._withinContextManager = False

    def login(self):
        try:
            conn = psycopg2.connect(
                user = self._config['psql_user'], 
                password = self._config['psql_password'],
                host = self._config['psql_host'], 
                port = self._config['psql_port'], 
                database = self._config['psql_db']
            ) # raises exception on error
            self._client = conn
        except OperationalError:
            raise PlantmonitorDBError(OperationalError.args)
        if conn.close == 1:
            raise PlantmonitorDBError('CONNECTION CLOSED')

    def close(self):
        if self._client:
            self._client.close()
    
    def commit(self):
        if self._client:
            self._client.commit()

    def __enter__(self):
        self._withinContextManager = True
        self.login()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            if self._client:
                self._client.rollback()
            self.close()
            raise
        else:
            if self._client:
                self._client.commit()
            self.close()

    @decorator.decorator
    def withinContextManager(f, self, *args, **kwds):
        if self._withinContextManager:
            return f(self, *args, **kwds)

        try:
            result = f(self, *args, **kwds)
            return result
        except Exception:
            self._client.rollback()
            self.close()
            raise
        finally:
            self.close()

    @withinContextManager
    def addFacilityMeterRelation(self, facility, meter):
        cur = self._client.cursor()
        cur.execute(f"insert into facility_meter(facilityid,meter)\
            values('{facility}','{meter}');")

    def facilityMeterRelationExists(self, facility, meter):
        facility_meter = self.getFacilityMeter() 
        return (facility, meter) in facility_meter

    # facilityMeterDict := set((facility1, meter1), ..., (facilityN, meterN))
    def getFacilityMeter(self):
        cur = self._client.cursor()
 
        cur.execute("select facilityid, meter from facility_meter;")
        facility_meter_db = cur.fetchall()

        facility_meter = set(facility_meter_db)
        return facility_meter

    def meterToFacility(self, meter):
        facility_meter = self.getFacilityMeter()
        facilities = [f for f, m in facility_meter if m == meter]
        return facilities[0] if facilities else None

    def facilityToMeter(self, facility):
        facility_meter = self.getFacilityMeter()
        meters = [m for f, m in facility_meter if f == facility]
        return meters[0] if meters else None

    @withinContextManager
    def addMeterData(self, facilityMeterData):
        cur = self._client.cursor()

        for f,v in facilityMeterData.items():
            m = self.facilityToMeter(f)
            if not m:
                raise PlantmonitorDBError(f'Facility {f} has no associated meter')
            for t, e in v:
                cur.execute(f"insert into sistema_contador \
                    (time, name, export_energy)\
                    VALUES('{t}', '{m}', {e});")

    @withinContextManager
    def getMeterData(self):
        cur = self._client.cursor()
        cur.execute(
            "select facilityid, to_char(time,'YYYY-MM-DD HH24:MI:SS'),\
                 export_energy from sistema_contador \
            inner join facility_meter on meter = name;"
        )
        dbData = cur.fetchall()

        return self.dbToDictionary(dbData)

    def dbToDictionary(self, dbData):
        data = {}
        for f,t,e in dbData:
            data.setdefault(f, []).append((t,e))
        return data
