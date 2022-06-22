import yaml
import datetime
import pandas as pd

from abc import ABCMeta, abstractmethod
from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

class NoSolarEventError(Exception): pass

class Alarm(metaclass=ABCMeta):

    def __init__(self, db_con, name, description, severity, createdate, active=True, sql=None):
        self.db_con = db_con
        self.name = name

        #TODO instead of using the weird "get if exists set+get otherwise" syntax
        # just get and, if the alarm doesn't exist, then create
        # also optionally update fields other than name
        self.id = self.set_new_alarm(name, description, severity, createdate, active=True, sql=None)

    # TODO store the sql in db?
    def set_new_alarm(self, name, description, severity, createdate, active=True, sql=None):
        createdate = createdate.strftime("%Y-%m-%d")
        self.name = name

        # TODO this has a corner case race condition, see alternative:
        # https://stackoverflow.com/questions/40323799/return-rows-from-insert-with-on-conflict-without-needing-to-update
        # TODO: How to prevent autoincrement of id serial on conflict
        query = f'''
            WITH ins AS (
                INSERT INTO
                    alarm (
                        name,
                        description,
                        severity,
                        active,
                        createdate
                    )
                    VALUES(%s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO NOTHING
                    RETURNING
                        id
            )
            SELECT id FROM ins
            UNION  ALL
            SELECT id FROM alarm
            WHERE  name = %s  -- only executed if no INSERT
            LIMIT  1;
        '''
        row = self.db_con.execute(query, (name, description, severity, active, createdate, name)).fetchone()

        self.db_con.execute("SELECT setval('alarm_id_seq', MAX(id), true) FROM alarm;")

        return row and row[0]

    def set_alarm_status_update_time(self, device_table, device_id, alarm, check_time):

        check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")

        query = f'''
            UPDATE alarm_status
            SET update_time = '{check_time}'
            WHERE
                alarm = {alarm} AND
                device_table = '{device_table}' AND
                device_id = '{device_id}'
            RETURNING *
        '''
        return self.db_con.execute(query).fetchone()

    def set_alarm_status(self, device_table, device_id, device_name, alarm, update_time, status):
        update_time = update_time.strftime("%Y-%m-%d %H:%M:%S%z")
        status = 'NULL' if status is None else status

        query = f'''
            INSERT INTO
            alarm_status (
                device_table,
                device_id,
                device_name,
                alarm,
                start_time,
                update_time,
                status
            )
            VALUES('{device_table}','{device_id}','{device_name}','{alarm}','{update_time}', '{update_time}', {status})
            ON CONFLICT (device_table, device_id, alarm)
            DO UPDATE
            SET
                device_name = EXCLUDED.device_name,
                start_time = CASE
                    WHEN EXCLUDED.status is distinct from alarm_status.status THEN '{update_time}'
                    ELSE alarm_status.start_time
                    END,
                update_time = EXCLUDED.update_time,
                status = EXCLUDED.status
            RETURNING
                id, device_table, device_id, device_name, alarm, start_time, update_time, status,
                (SELECT old.status FROM alarm_status old WHERE old.id = alarm_status.id) AS old_status,
                (SELECT old.start_time FROM alarm_status old WHERE old.id = alarm_status.id) AS old_start_time;
        '''
        result = self.db_con.execute(query).fetchone()
        self.db_con.execute("SELECT setval('alarm_status_id_seq', MAX(id), true) FROM alarm_status;")
        return result

    def set_alarm_historic(self, device_table, device_id, device_name, alarm, start_time, end_time):
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S%z")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S%z")

        query = f'''
            INSERT INTO
            alarm_historic (
                device_table,
                device_id,
                device_name,
                alarm,
                start_time,
                end_time
            )
            VALUES('{device_table}','{device_id}','{device_name}','{alarm}','{start_time}', '{end_time}')
            RETURNING
                id, device_table, device_id, device_name, alarm, start_time, end_time;
        '''
        return self.db_con.execute(query).fetchone()

    @classmethod
    def source_table_exists(cls, db_con, table_name):
        query = f"select exists ( select from information_schema.tables where table_name = '{table_name}')"
        return db_con.execute(query).fetchone()[0]

    @classmethod
    def is_daylight(cls, db_con, device_table, device_id, check_time):
        query = f'''
            select
                case when
                    '{check_time}' between sunrise and sunset then TRUE
                    else FALSE
                END as is_daylight
                from solarevent
                left join {device_table} on {device_table}.id = {device_id}
                left join plant on plant.id = {device_table}.plant
            where solarevent.plant = plant.id
            and '{check_time}'::date = sunrise::date
        '''
        result = db_con.execute(query).fetchone()
        is_day = result and result[0]

        if is_day is None:
            raise NoSolarEventError(f"Error: Current datetime of {device_table} {device_id} does not match any solarevent at {check_time}")

        return is_day

    @abstractmethod
    def get_alarm_current(self, check_time):
        pass

    @abstractmethod
    def set_devices_alarms(self, check_time):
        pass

    # TODO all update_alarms are equal, it can be placed here
    @abstractmethod
    def update_alarm(self, check_time = None):
        pass


class AlarmInverterNoPower(Alarm):

    def __init__(self, db_con, name, description, severity, createdate, active=True, sql=None):
        super().__init__(db_con, name, description, severity, createdate, active, sql)
        #TODO absolute and unique device ids must replace device_table specification requirement
        self.device_table = 'inverter'
        self.source_table = 'bucket_5min_inverterregistry'

    def get_alarm_current(self, check_time):
        check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")
        query = f'''
            SELECT reg.inverter AS inverter, inv.name as inverter_name, max(reg.power_w) = 0 as nopower
            FROM {self.source_table} as reg
            LEFT JOIN inverter AS inv ON inv.id = reg.inverter
            WHERE '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
            group by reg.inverter, inv.name
        '''
        return self.db_con.execute(query).fetchall()

    # TODO the is_day condition could be abstracted and passed as a parameter if other alarms have different conditions
    # e.g. skip_condition(db_con, inverter, check_time)
    def set_devices_alarms(self, alarm_id, device_table, alarms_current, check_time):
        for device_id, device_name, status in alarms_current:
            if status is not None:
                # TODO we could pass the plant id instead of device_table+device_id
                # TODO this generates as many queries as inverters, we should fetch all of them beforehand or merge it in the alarm sql
                is_day = self.is_daylight(self.db_con, device_table, device_id, check_time)
                if not is_day:
                    self.set_alarm_status_update_time(device_table, device_id, alarm_id, check_time)
                    continue

            current_alarm = self.set_alarm_status(device_table, device_id, device_name, alarm_id, check_time, status)

            if current_alarm['old_status'] == True and status != True:
                self.set_alarm_historic(device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

    def update_alarm(self, check_time = None):

        if not self.source_table_exists(self.db_con, self.source_table):
            logger.error(f'{self.source_table} table does not exist therefore alarm {self.name} cannot be computed')
            return

        check_time = check_time or datetime.datetime.now()

        # TODO check alarma noreading que invalida l'alarma nopower

        alarm_current = self.get_alarm_current(check_time)
        self.set_devices_alarms(self.id, self.device_table, alarm_current, check_time)

        logger.debug(f"Updated {len(alarm_current)} records with values {alarm_current}")


class AlarmInverterTemperatureAnomaly(Alarm):

    def __init__(self, db_con, name, description, severity, createdate, active=True, sql=None):
        super().__init__(db_con, name, description, severity, createdate, active, sql)
        #TODO absolute and unique device ids must replace device_table specification requirement
        self.device_table = 'inverter'
        self.source_table = 'bucket_5min_inverterregistry'

    def get_inverter_temperatures(self, check_time):
        check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")
        # where assumes UTC otherwise it would be wrong on DST change days
        query = f'''
            SELECT reg.time as time, inv.plant as plant, reg.inverter AS inverter, inv.name as inverter_name, reg.temperature_dc as temperature_dc
            FROM {self.source_table} as reg
            LEFT JOIN inverter AS inv ON inv.id = reg.inverter
            WHERE '{check_time}'::timestamptz - interval '120 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
            order by reg.time, inv.plant, reg.inverter
        '''
        queryresult = self.db_con.execute(query)
        return queryresult.fetchall(), queryresult.keys()

    def get_alarm_current(self, check_time):
        # si la temperatura de l'inversor X és > 40C and la diferencia entre l'inversor X i la resta és > a 10C durant 2 hores

        inverter_temperatures, keys = self.get_inverter_temperatures(check_time)
        df = pd.DataFrame(inverter_temperatures, columns=keys)
        df['minimum'] = df.groupby(['plant','time'])['temperature_dc'].transform('min')
        df['overhot'] = ((df['temperature_dc'] - df['minimum']) > 400) & (df['minimum'] > 100)
        df['null_temperature'] = df['temperature_dc'].isnull()
        alarm_status_df = df.groupby(['plant','inverter'], as_index=False).agg({'inverter_name': 'max', 'overhot': 'all', 'null_temperature': 'all'})
        # TODO return alarm to None if we don't have at least two non-null inverters
        # at the moment we only set to None if the inverter doesn't have values
        alarm_status_df.loc[alarm_status_df.null_temperature, 'overhot'] = pd.NA
        alarm_status_df.overhot = alarm_status_df.overhot.replace({pd.NA: None})
        # alternatives: pd.NA, numpy.nan
        alarm_status_df.drop(columns=['plant','null_temperature'], inplace=True)
        temperature_anomaly_alarm_status = alarm_status_df.to_records(index=False).tolist()
        return temperature_anomaly_alarm_status

    def set_devices_alarms(self, alarm_id, device_table, alarms_current, check_time):
        for device_id, device_name, status in alarms_current:

            current_alarm = self.set_alarm_status(device_table, device_id, device_name, alarm_id, check_time, status)

            if current_alarm['old_status'] == True and status != True:
                self.set_alarm_historic(device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

    def update_alarm(self, check_time = None):

        if not self.source_table_exists(self.db_con, self.source_table):
            logger.error(f'{self.source_table} table does not exist therefore alarm {self.name} cannot be computed')
            return

        check_time = check_time or datetime.datetime.now()

        # TODO check alarma noreading que invalida l'alarma temperatureanomaly

        alarm_current = self.get_alarm_current(check_time)
        self.set_devices_alarms(self.id, self.device_table, alarm_current, check_time)

        logger.debug(f"Updated {len(alarm_current)} records with values {alarm_current}")


class AlarmStringNoIntensity(Alarm):

    def __init__(self, db_con, name, description, severity, createdate, active=True, sql=None):
        super().__init__(db_con, name, description, severity, createdate, active, sql)
        #TODO absolute and unique device ids must replace device_table specification requirement
        self.device_table = 'string'
        self.source_table = 'bucket_5min_stringregistry'

    def get_alarm_current(self, check_time):
        check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")

        #TODO this alarm is preceded by the noreadings of inverter and strings per inverter and per string

        # TODO this simplifies debugging, are we relying too much on sql?
        # query = f'''
        #     SELECT
        #         reg.time as time,
        #         ireg.inverter as inverter,
        #         reg.string as string_id,
        #         s.name as string_name,
        #         reg.intensity_ma,
        #         ireg.power_w,
        #         CASE WHEN (reg.intensity_ma IS NULL or ireg.power_w IS NULL or ireg.power_w = 0) THEN 1 ELSE NULL END as nullcount,
        #         case when ireg.power_w = 0 or ireg.power_w is NULL THEN NULL ELSE reg.intensity_ma = 0 and ireg.power_w != 0 END as nointensity
        #     FROM bucket_5min_stringregistry as reg
        #     LEFT JOIN string AS s ON s.id = reg.string
        #     LEFT JOIN inverter AS inv ON inv.id = s.inverter
        #     -- is it left joining on the whole table?
        #     LEFT JOIN bucket_5min_inverterregistry as ireg on reg.time = ireg.time and ireg.inverter = s.inverter
        #     WHERE
        #         '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
        #         AND '{check_time}'::timestamptz - interval '60 minutes' <= ireg.time and ireg.time <= '{check_time}'::timestamptz
        # '''
        # print(self.db_con.execute(query).fetchall())

        # The logic is:
        # if >8 null readings (either power or intensity) -> None (sense dades)
        # if power_w = 0 row doesn't vote for alarm
        # if power_w != 0 and intensity = 0 vote for True (Alarm)
        # if power_w != 0 and intensity != 0 vote for False (OK)
        # if there's one False vote, alarm is False
        # if all power_w readings are NULL, then None as per >8 condition

        #TODO assumes we have the 12 readings
        minimum_non_null_readings = 8

        query = f'''
            SELECT
                reg.string as string_id,
                s.name as string_name,
                CASE WHEN COUNT(CASE WHEN (reg.intensity_ma IS NULL or ireg.power_w IS NULL) THEN 1 END) > {minimum_non_null_readings}
                     THEN NULL
                     ELSE BOOL_AND(case when ireg.power_w = 0 or ireg.power_w is NULL THEN NULL ELSE reg.intensity_ma <= 500 and ireg.power_w != 0 END)
                END as nointensity
            FROM {self.source_table} as reg
            LEFT JOIN string AS s ON s.id = reg.string
            LEFT JOIN inverter AS inv ON inv.id = s.inverter
            -- is it left joining on the whole table?
            LEFT JOIN bucket_5min_inverterregistry as ireg on reg.time = ireg.time and ireg.inverter = s.inverter
            WHERE
                '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
                AND '{check_time}'::timestamptz - interval '60 minutes' <= ireg.time and ireg.time <= '{check_time}'::timestamptz
            group by string_id, string_name
        '''
        return self.db_con.execute(query).fetchall()

    def set_devices_alarms(self, alarm_id, device_table, alarms_current, check_time):
        for device_id, device_name, status in alarms_current:
            # TODO should we handle no inverter power in some other way? right now it'll just transition to NULL hence 'Sense Dades'
            # if status is not None:
            #     # inverter has no power, update check but don't change status, we can't know if the string can give intensity
            #     self.set_alarm_status_update_time(device_table, device_id, alarm_id, check_time)
            #     continue

            current_alarm = self.set_alarm_status(device_table, device_id, device_name, alarm_id, check_time, status)

            if current_alarm['old_status'] == True and status != True:
                self.set_alarm_historic(device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

    def update_alarm(self, check_time = None):

        if not self.source_table_exists(self.db_con, self.source_table):
            logger.error(f'{self.source_table} table does not exist therefore alarm {self.name} cannot be computed')
            return

        check_time = check_time or datetime.datetime.now()

        # TODO check alarma noreading que invalida l'alarma nointensity
        alarm_current = self.get_alarm_current(check_time)

        self.set_devices_alarms(self.id, self.device_table, alarm_current, check_time)

        logger.debug(f"Updated {len(alarm_current)} records")


class AlarmPVMeterNoEnergy(Alarm):

    def __init__(self, db_con, name, description, severity, createdate, active=True, sql=None):
        super().__init__(db_con, name, description, severity, createdate, active, sql)
        #TODO absolute and unique device ids must replace device_table specification requirement
        self.device_table = 'meter'
        self.source_table = 'meterregistry'

    def get_alarm_current(self, check_time):
        check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")
        query = f'''
            SELECT reg.meter AS meter, inv.name as meter_name, max(reg.power_w) = 0 as nopower
            FROM {self.source_table} as reg
            LEFT JOIN meter AS m ON m.id = reg.meter
            WHERE '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
            group by reg.meter, m.name
        '''
        return self.db_con.execute(query).fetchall()

    # TODO the is_day condition could be abstracted and passed as a parameter if other alarms have different conditions
    # e.g. skip_condition(db_con, inverter, check_time)
    def set_devices_alarms(self, alarm_id, device_table, alarms_current, check_time):
        for device_id, device_name, status in alarms_current:
            if status is not None:
                # TODO we could pass the plant id instead of device_table+device_id
                # TODO this generates as many queries as meters, we should fetch all of them beforehand or merge it in the alarm sql
                is_day = self.is_daylight(self.db_con, device_table, device_id, check_time)
                if not is_day:
                    self.set_alarm_status_update_time(device_table, device_id, alarm_id, check_time)
                    continue

            current_alarm = self.set_alarm_status(device_table, device_id, device_name, alarm_id, check_time, status)

            if current_alarm['old_status'] == True and status != True:
                self.set_alarm_historic(device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

    def update_alarm(self, check_time = None):

        if not self.source_table_exists(self.db_con, self.source_table):
            logger.error(f'{self.source_table} table does not exist therefore alarm {self.name} cannot be computed')
            return

        check_time = check_time or datetime.datetime.now()

        # TODO check alarma noreading que invalida l'alarma nopower

        alarm_current = self.get_alarm_current(check_time)
        self.set_devices_alarms(self.id, self.device_table, alarm_current, check_time)

        logger.debug(f"Updated {len(alarm_current)} records with values {alarm_current}")


class AlarmHydroGasMeterNoEnergy(Alarm):

    def __init__(self, db_con, name, description, severity, createdate, active=True, sql=None):
        super().__init__(db_con, name, description, severity, createdate, active, sql)
        #TODO absolute and unique device ids must replace device_table specification requirement
        self.device_table = 'meter'
        self.source_table = 'meterregistry'

    def get_alarm_current(self, check_time):
        check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")
        query = f'''
            SELECT reg.meter AS meter, inv.name as meter_name, max(reg.power_w) = 0 as nopower
            FROM {self.source_table} as reg
            LEFT JOIN meter AS m ON m.id = reg.meter
            WHERE '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
            group by reg.meter, m.name
        '''
        return self.db_con.execute(query).fetchall()

    # TODO the is_day condition could be abstracted and passed as a parameter if other alarms have different conditions
    # e.g. skip_condition(db_con, inverter, check_time)
    def set_devices_alarms(self, alarm_id, device_table, alarms_current, check_time):
        for device_id, device_name, status in alarms_current:
            if status is not None:
                self.set_alarm_status_update_time(device_table, device_id, alarm_id, check_time)

            current_alarm = self.set_alarm_status(device_table, device_id, device_name, alarm_id, check_time, status)

            if current_alarm['old_status'] == True and status != True:
                self.set_alarm_historic(device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

    def update_alarm(self, check_time = None):

        if not self.source_table_exists(self.db_con, self.source_table):
            logger.error(f'{self.source_table} table does not exist therefore alarm {self.name} cannot be computed')
            return

        check_time = check_time or datetime.datetime.now()

        # TODO check alarma noreading que invalida l'alarma nopower

        alarm_current = self.get_alarm_current(check_time)
        self.set_devices_alarms(self.id, self.device_table, alarm_current, check_time)

        logger.debug(f"Updated {len(alarm_current)} records with values {alarm_current}")


class Alarm5minNoReading(Alarm):

    def __init__(self, db_con, name, description, severity, createdate, active=True, sql=None):
        super().__init__(db_con, name, description, severity, createdate, active, sql)
        #TODO absolute and unique device ids must replace device_table specification requirement
        self.device_table = 'meter'
        self.source_table = 'meterregistry'

    def get_alarm_current(self, check_time):
        check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")
        query = f'''
            SELECT reg.meter AS meter, inv.name as meter_name, max(reg.power_w) = 0 as nopower
            FROM {self.source_table} as reg
            LEFT JOIN meter AS m ON m.id = reg.meter
            WHERE '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
            group by reg.meter, m.name
        '''
        return self.db_con.execute(query).fetchall()

    # TODO the is_day condition could be abstracted and passed as a parameter if other alarms have different conditions
    # e.g. skip_condition(db_con, inverter, check_time)
    def set_devices_alarms(self, alarm_id, device_table, alarms_current, check_time):
        for device_id, device_name, status in alarms_current:
            if status is not None:
                # TODO we could pass the plant id instead of device_table+device_id
                # TODO this generates as many queries as meters, we should fetch all of them beforehand or merge it in the alarm sql
                is_day = self.is_daylight(self.db_con, device_table, device_id, check_time)
                if not is_day:
                    self.set_alarm_status_update_time(device_table, device_id, alarm_id, check_time)
                    continue

            current_alarm = self.set_alarm_status(device_table, device_id, device_name, alarm_id, check_time, status)

            if current_alarm['old_status'] == True and status != True:
                self.set_alarm_historic(device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

    def update_alarm(self, check_time = None):

        if not self.source_table_exists(self.db_con, self.source_table):
            logger.error(f'{self.source_table} table does not exist therefore alarm {self.name} cannot be computed')
            return

        check_time = check_time or datetime.datetime.now()

        # TODO check alarma noreading que invalida l'alarma nopower

        alarm_current = self.get_alarm_current(check_time)
        self.set_devices_alarms(self.id, self.device_table, alarm_current, check_time)

        logger.debug(f"Updated {len(alarm_current)} records with values {alarm_current}")


class AlarmMeterNoReading(Alarm):

    def __init__(self, db_con, name, description, severity, createdate, active=True, sql=None):
        super().__init__(db_con, name, description, severity, createdate, active, sql)
        #TODO absolute and unique device ids must replace device_table specification requirement
        self.device_table = 'meter'
        self.source_table = 'meterregistry'

    def get_alarm_current(self, check_time):
        # get all active meters
        # get last time in the last 24h
        # add the ones not present (so we'll be assuming >24h without readings)

        query = '''
        select meter.* from meter
        left join plant on plant.id = meter.plant
        where description != 'SomRenovables'
        '''

        som_meters = self.db_con.execute(query).fetchall()

        check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")
        query = f'''
            SELECT distinct on (reg.meter)
                reg.meter AS meter,
                m.name as meter_name,
                m.connection_protocol,
                time as latest_reading
            FROM {self.source_table} as reg
            LEFT JOIN meter AS m ON m.id = reg.meter
            left join plant on plant.id = m.plant
            WHERE '{check_time}'::timestamptz - interval '1 day' <= reg.time
            and reg.time <= '{check_time}'::timestamptz
            and description != 'SomRenovables'
            order by meter, time desc
        '''
        latest_readings = self.db_con.execute(query).fetchall()

        import ipdb; ipdb.set_trace()

        # if moxa and 1 day < latest_reading - check_time: alarm
        # if ip and 2h < latest_reading - check_time: alarm
        # if meter in som_meters but not in latest_readings: alarm (it means > 30 day without reading)

        noreadingalarm = [(meter, meter_name, noreading) for lr in latest_readings]

        return noreadingalarm

    # TODO the is_day condition could be abstracted and passed as a parameter if other alarms have different conditions
    # e.g. skip_condition(db_con, inverter, check_time)
    def set_devices_alarms(self, alarm_id, device_table, alarms_current, check_time):
        for device_id, device_name, status in alarms_current:
            if status is not None:
                self.set_alarm_status_update_time(device_table, device_id, alarm_id, check_time)

            current_alarm = self.set_alarm_status(device_table, device_id, device_name, alarm_id, check_time, status)

            if current_alarm['old_status'] == True and status != True:
                self.set_alarm_historic(device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

    def update_alarm(self, check_time = None):

        if not self.source_table_exists(self.db_con, self.source_table):
            logger.error(f'{self.source_table} table does not exist therefore alarm {self.name} cannot be computed')
            return

        check_time = check_time or datetime.datetime.now()

        alarm_current = self.get_alarm_current(check_time)
        self.set_devices_alarms(self.id, self.device_table, alarm_current, check_time)

        logger.debug(f"Updated {len(alarm_current)} records with values {alarm_current}")

