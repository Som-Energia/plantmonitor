import yaml
import datetime

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

class NoSolarEventError(Exception): pass

class Alarm:

    def __init__(self, db_con, name, description, severity, createdate, active=True, sql=None):
        self.db_con = db_con
        self.name = name
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

    def get_alarm_status_nopower_alarmed(self, alarm, device_table, device_id):

        query = f'''
            SELECT status
            FROM alarm_status
            WHERE
                device_table = '{device_table}' AND
                device_id = {device_id} AND
                alarm = {alarm}
        '''
        return self.db_con.execute(query).fetchone()[0]

    def get_alarm_current_nopower_inverter(self, check_time):
        check_time = check_time.strftime("%Y-%m-%d %H:%M:%S%z")
        query = f'''
            SELECT reg.inverter AS inverter, inv.name as inverter_name, max(reg.power_w) = 0 as nopower
            FROM bucket_5min_inverterregistry as reg
            LEFT JOIN inverter AS inv ON inv.id = reg.inverter
            WHERE '{check_time}'::timestamptz - interval '60 minutes' <= reg.time and reg.time <= '{check_time}'::timestamptz
            group by reg.inverter, inv.name
        '''
        return self.db_con.execute(query).fetchall()

    def get_alarm_current_nointensity_string(self, check_time):
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
                     ELSE BOOL_AND(case when ireg.power_w = 0 or ireg.power_w is NULL THEN NULL ELSE reg.intensity_ma = 0 and ireg.power_w != 0 END)
                END as nointensity
            FROM bucket_5min_stringregistry as reg
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

    def set_devices_alarms_if_inverter_power(self, alarm_id, device_table, alarms_current, check_time):
        for device_id, device_name, status in alarms_current:
            # TODO should we handle no inverter power in some other way? right now it'll just transition to NULL hence 'Sense Dades'
            # if status is not None:
            #     # inverter has no power, update check but don't change status, we can't know if the string can give intensity
            #     self.set_alarm_status_update_time(device_table, device_id, alarm_id, check_time)
            #     continue

            current_alarm = self.set_alarm_status(device_table, device_id, device_name, alarm_id, check_time, status)

            if current_alarm['old_status'] == True and status != True:
                self.set_alarm_historic(device_table, device_id, device_name, alarm_id, current_alarm['old_start_time'], check_time)

    # TODO the is_day condition could be abstracted and passed as a parameter if other alarms have different conditions
    # e.g. skip_condition(db_con, inverter, check_time)
    def set_devices_alarms_if_daylight(self, alarm_id, device_table, alarms_current, check_time):
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


    def update_alarm_nopower_inverter(self, check_time = None):
        check_time = check_time or datetime.datetime.now()

        device_table = 'inverter'

        nopower_alarm = {
            'name': 'nopowerinverter',
            'description': 'Inversor sense potència entre alba i posta',
            'severity': 'critical',
            'createdate': datetime.date.today()
        }

        alarm_id = self.set_new_alarm(**nopower_alarm)
        # TODO check alarma noreading que invalida l'alarma nopower

        alarm_current = self.get_alarm_current_nopower_inverter(check_time)
        self.set_devices_alarms_if_daylight(alarm_id, device_table, alarm_current, check_time)

    def update_alarm_nointensity_string(self, check_time = None):
        check_time = check_time or datetime.datetime.now()

        device_table = 'string'

        nointensity_alarm = {
            'name': 'nointensitystring',
            'description': 'String sense intensitat entre alba i posta',
            'severity': 'critical',
            'createdate': datetime.date.today()
        }

        alarm_id = self.set_new_alarm(**nointensity_alarm)
        # TODO check alarma noreading que invalida l'alarma nointensity
        alarm_current = self.get_alarm_current_nointensity_string(check_time)

        self.set_devices_alarms_if_inverter_power(alarm_id, device_table, alarm_current, check_time)
