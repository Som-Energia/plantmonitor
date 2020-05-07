import psycopg2

from psycopg2 import OperationalError
import psycopg2.extras
from datetime import datetime
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import decorator

import copy


class PlantmonitorDBError(Exception):
    pass


class PlantmonitorDBMock(object):

    # data structure: {facility: [('time', value)]}

    def __init__(self, config):
        self._data = {}
        self._enterdata = {}
        self._facilityMeter = set()
        self._config = config
        self._client = None
        self._withinContextManager = False
        self._loggedIn = False

    def login(self):
        if self._config['psql_user'] != 'alberto' or self._config['psql_password'] != '1234':
            raise PlantmonitorDBError("Invalid username or password")
        self._loggedIn = True
        return self._client

    def close(self):
        self._loggedIn = False
        pass

    def __enter__(self):
        self._withinContextManager = True
        self.login()
        self._enterdata = self._data
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        if exc_type is not None:
            self._data = self._enterdata
            raise exc_type

    @decorator.decorator
    def withinContextManager(f, self, *args, **kwds):
        if self._withinContextManager:
            return f(self, *args, **kwds)

        try:
            result = f(self, *args, **kwds)
            return result
        except Exception as exp:
            # TODO ? self._client.rollback()
            raise exp
        finally:
            self.close()

    @decorator.decorator
    def checkLoggedIn(f, self, *args, **kwds):
        if not self._loggedIn:
            raise PlantmonitorDBError('Not logged in. Permission denied.')
        return f(self, *args, **kwds)

    @checkLoggedIn
    def meterToFacility(self, meter):
        facilities = [f for f, m in self._facilityMeter if m is meter]
        return facilities[0] if facilities else None

    @checkLoggedIn
    def facilityToMeter(self, facility):
        meters = [m for f, m in self._facilityMeter if f is facility]
        return meters[0] if meters else None

    @checkLoggedIn
    def getMeterData(self):
        return self._data

    @withinContextManager
    @checkLoggedIn
    def addFacilityMeterRelation(self, facility, meter):
        self._facilityMeter.add((facility, meter))

    @checkLoggedIn
    def getFacilityMeter(self):
        return self._facilityMeter

    def getFacilities(self):
        facility_meter = self.getFacilityMeter()
        return [facility for facility, meter in facility_meter]

    @checkLoggedIn
    def facilityMeterRelationExists(self, facility, meter):
        facility_meter = self.getFacilityMeter()
        return (facility, meter) in facility_meter

    @checkLoggedIn
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

    # lingua franca: {facility: [('time', value)]}

    def __init__(self, config):
        self._config = config
        self._client = None
        self._withinContextManager = False

    @staticmethod
    def demoDBsetup(configdb):
        with psycopg2.connect(
            user=configdb['psql_user'],
            password=configdb['psql_password'],
            host=configdb['psql_host'],
            port=configdb['psql_port']
        ) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                cursor.execute(
                    "DROP DATABASE IF EXISTS {}".format(configdb['psql_db'])
                )
                cursor.execute(
                    "CREATE DATABASE {};".format(configdb['psql_db'])
                )
        with psycopg2.connect(
            user=configdb['psql_user'],
            password=configdb['psql_password'],
            host=configdb['psql_host'],
            port=configdb['psql_port'],
            database=configdb['psql_db']
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE sistema_contador(
                    time TIMESTAMP NOT NULL,
                    name VARCHAR(50),
                    export_energy bigint);
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE facility_meter (id serial primary key,
                    facilityid character varying(200),
                    meter character varying(200));
                    """
                )

    @staticmethod
    def dropDatabase(configdb):
        with psycopg2.connect(
            user=configdb['psql_user'],
            password=configdb['psql_password'],
            host=configdb['psql_host'],
            port=configdb['psql_port']
        ) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                cursor.execute("DROP DATABASE {}".format(configdb['psql_db']))

    def login(self):
        try:
            # raises exception on error
            conn = psycopg2.connect(
                user=self._config['psql_user'],
                password=self._config['psql_password'],
                host=self._config['psql_host'],
                port=self._config['psql_port'],
                database=self._config['psql_db']
            )
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
            raise exc_type
        else:
            if self._client:
                self._client.commit()
                self.close()

    @decorator.decorator
    def withinContextManager(f, self, *args, **kwds):
        if self._withinContextManager:
            return f(self, *args, **kwds)

        result = None
        try:
            result = f(self, *args, **kwds)
            return result
        except:
            if self._client:
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

    def getFacilities(self):
        facility_meter = self.getFacilityMeter()
        return [facility for facility, meter in facility_meter]

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
        # if not self._client:
        #     raise PlantmonitorDBError("Db client is None, have you logged in?")

        cur = self._client.cursor()

        for f, v in facilityMeterData.items():
            m = self.facilityToMeter(f)
            if not m:
                raise PlantmonitorDBError(f'Facility {f} has no associated meter')
            for t, e in v:
                cur.execute(f"insert into sistema_contador \
                    (time, name, export_energy)\
                    VALUES('{t}', '{m}', {e});")

    @withinContextManager
    def getMeterData(self, facility=None, fromDate=None, toDate=None):
        # if not self._client:
        #     raise PlantmonitorDBError("Db client is None, have you logged in?")

        if not toDate:
            toDate = datetime.now()
        
        condition = ''
        if facility:
            condition = f"where facilityid = '{facility}'"
        if fromDate:
            condition = f"where time between '{fromDate}' and '{toDate}'"
        if facility and fromDate:
            condition = f"where facilityid = '{facility}' and time between '{fromDate}' and '{toDate}'"

        cur = self._client.cursor()
        cur.execute(
            f"select facilityid, to_char(time,'YYYY-MM-DD HH24:MI:SS'),\
                export_energy from sistema_contador \
            inner join facility_meter on meter = name {condition};"
        )
        dbData = cur.fetchall()

        return self.dbToDictionary(dbData)

    def dbToDictionary(self, dbData):
        data = {}
        for f, t, e in dbData:
            data.setdefault(f, []).append((t, e))
        
        return data
