import psycopg2

from psycopg2 import OperationalError
import psycopg2.extras
from datetime import datetime
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from meteologica.utils import todt
import decorator

import copy


class PlantmonitorDBError(Exception):
    pass


class PlantmonitorDBMock(object):

    # data structure: {facility: [(datetime, value)]}

    def __init__(self, config):
        self._data = {}
        self._forecastdata = {}
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
    def getMeterData(self, facility=None, fromDate=None, toDate=None):

        if not toDate:
            toDate = datetime.now()

        selectedData = {}
        if facility and not fromDate:
            selectedData[facility] = self._data.get(facility, [])
        elif fromDate and not facility:
            for facility, values in self._data.items():
                facilityData = self._data.get(facility, [])
                selectedData[facility] = list(filter(lambda v: fromDate <= v[0] and v[0] <= toDate, facilityData))
        elif facility and fromDate:
            facilityData = self._data.get(facility, [])
            selectedData[facility] = list(filter(lambda v: fromDate <= v[0] and v[0] <= toDate, facilityData))
        else:
            return self._data

        return selectedData

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

    # {facility: [(datetime, percentil50)]}
    def addForecast(self, data, forecastDate):

        for facility, oldforecast in self._forecastdata.items():
            newforecast = data.get(facility, [])

            doldforecast = dict(oldforecast)
            doldforecast.update(dict(newforecast))

            self._forecastdata[facility] = list(doldforecast.items())

        # if facility exists only in data
        for facility, forecast in data.items():
            if not facility in self._forecastdata:
                self._forecastdata[facility] = forecast

    def getForecast(self, facility=None):
        return self._forecastdata

    def lastDateDownloaded(self,facility):
        records  = self._forecastdata.get(facility, None)
        if not records:
            return None
        return records[-1][0]

class PlantmonitorDB:

    # lingua franca: {facility: [('time', value)]}

    def __init__(self, config):
        self._config = config
        self._client = None
        self._withinContextManager = False

    @staticmethod
    def demoDBsetupFromDump(configdb, dumpfile):
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
                    "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
                )
                cursor.execute(open(dumpfile, "r").read())


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
                cursor.execute(
                    """
                    CREATE TABLE if not exists forecastHead\
                        (id SERIAL NOT NULL, errorCode VARCHAR(50), facilityId VARCHAR(50), \
                        variableId VARCHAR(50), predictorId VARCHAR(20), forecastDate TIMESTAMPTZ, \
                        granularity INTEGER, PRIMARY KEY(id));
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE forecastData(idForecastHead SERIAL REFERENCES forecastHead(id),\
                        time TIMESTAMPTZ, percentil10 INTEGER, percentil50 INTEGER, \
                        percentil90 INTEGER, PRIMARY KEY(idForecastHead,time));
                    """
                )
                cursor.execute(
                    """
                    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
                    SELECT create_hypertable('forecastData', 'time');
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
            return False
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
        cur.execute("insert into facility_meter(facilityid,meter)\
            values('{}','{}');".format(facility,meter))

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
                raise PlantmonitorDBError('Facility {} has no associated meter'.format(f))
            for t, e in v:
                cur.execute("insert into sistema_contador \
                    (time, name, export_energy)\
                    VALUES('{}', '{}', {});".format(t,m,e))

    @withinContextManager
    def addFullMeterData(self, facilityMeterData):
        # if not self._client:
        #     raise PlantmonitorDBError("Db client is None, have you logged in?")

        cur = self._client.cursor()

        for f, v in facilityMeterData.items():
            m = self.facilityToMeter(f)
            if not m:
                raise PlantmonitorDBError('Facility {} has no associated meter'.format(f))
            for t, e, i, r1, r2, r3, r4 in v:
                cur.execute("insert into sistema_contador \
                    (time, name, import_energy, export_energy, r1, r2, r3, r4)\
                    VALUES('{}', '{}', {}, {}, {}, {}, {}, {});".format(t, m, i, e, r1, r2, r3, r4))


    @withinContextManager
    def getMeterData(self, facility=None, fromDate=None, toDate=None):
        # if not self._client:
        #     raise PlantmonitorDBError("Db client is None, have you logged in?")

        if not toDate:
            toDate = datetime.now()

        condition = ''
        if facility:
            condition = "where facilityid = '{}'".format(facility)
        if fromDate:
            condition = "where time between '{}' and '{}'".format(fromDate, toDate)
        if facility and fromDate:
            condition = "where facilityid = '{}' and time between '{}' and '{}'".format(facility,fromDate,toDate)

        cur = self._client.cursor()
        cur.execute(
            "select facilityid, time,\
                export_energy from sistema_contador \
            inner join facility_meter on meter = name {};".format(condition)
        )
        dbData = cur.fetchall()

        return self.dbToDictionary(dbData)

    def lastDateDownloaded(self, facility):
        cur = self._client.cursor()
        cur.execute(
            "select time at time zone 'Europe/Madrid' from forecastData \
                inner join forecastHead on forecastData.idForecastHead = forecastHead.id \
                where facilityId = '{}' \
                order by time DESC limit 1;".format(facility)
        )
        dbData = cur.fetchone()
        if not dbData:
            return None
        return dbData[0]

    # records = [(datetime,percentil10,percentil50,percentil90)]
    # records = [(datetime,percentil50)]
    def addForecastFull(self, facility, forecastDate, records, headData):
        cur = self._client.cursor()

        facilityId = facility

        errorCode    = headData['errorCode']
        variableId   = headData['variableId']
        predictorId  = headData['predictorId']
        granularity  = headData['granularity']

        fromDate = records[0][0]
        toDate = records[-1][0]

        cur.execute("INSERT INTO forecastHead(errorCode, facilityId, variableId, \
        predictorId, forecastDate, granularity) VALUES ('{}', '{}', '{}', '{}', '{}', '{}') \
        RETURNING id;".format(errorCode, facilityId, variableId, predictorId, forecastDate, granularity))
        currentIdForecastHead = cur.fetchone()[0]

        if errorCode == 'OK':
            cur.execute("DELETE FROM forecastdata USING forecasthead WHERE forecastdata.idforecasthead = forecasthead.id AND forecasthead.facilityId = '{}' AND time BETWEEN '{}' AND '{}'".format(facilityId,fromDate,toDate))

            simple = False
            if len(records[0]) == 2:
                simple = True

            #https://hakibenita.com/fast-load-data-python-postgresql
            if simple:
                psycopg2.extras.execute_values(cur, "INSERT INTO forecastData VALUES %s;", ((
                    currentIdForecastHead,
                    record[0],
                    None,
                    record[1],
                    None,
                ) for record in records), page_size=1000)
            else:
                psycopg2.extras.execute_values(cur, "INSERT INTO forecastData VALUES %s;", ((
                    currentIdForecastHead,
                    record[0],
                    record[1],
                    record[2],
                    record[3],
                ) for record in records), page_size=1000)

    # {facility: [(datetime, value)]}
    @withinContextManager
    def addForecast(self, forecastDataDict, forecastDate):
        for facility, records in forecastDataDict.items():
            headData = {'errorCode': 'OK', 'facilityId': facility, 'variableId': 'prod', 'predictorId': 'aggr',
            'granularity': 60}
            self.addForecastFull(facility, forecastDate, records, headData)

    # TODO filter by facility, filter by date
    # def getForecast(self, facility=None, fromDate=None, toDate=None):
    def getForecast(self):
        cur = self._client.cursor()
        cur.execute(
            "select facilityid, time at time zone 'Europe/Madrid',\
                percentil50 from forecastData \
            inner join forecastHead on forecastData.idForecastHead = forecastHead.id;"
        )
        dbData = cur.fetchall()

        return self.dbToDictionary(dbData)

    def dbToDictionary(self, dbData):
        data = {}
        for f, t, e in dbData:
            data.setdefault(f, []).append((t, e))

        return data
