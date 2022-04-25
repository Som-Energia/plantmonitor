import yaml
import datetime
from typing import List
from conf.log import logger

from .alarm import Alarm, AlarmInverterNoPower, AlarmInverterTemperatureAnomaly, AlarmStringNoIntensity

class UndefinedAlarmError(Exception):
    pass

class AlarmManager:

    def __init__(self, db_con):
        self.db_con = db_con
        #TODO type hinting supported since which python, 3.5?
        self.alarms: List[Alarm] = []

    def add_alarm(self, name=None, **kwargs):
        # TODO use factory
        if name == 'noinverterpower':
            alarm = AlarmInverterNoPower(db_con=self.db_con, name=name, **kwargs)
        elif name == 'nostringintensity':
            alarm = AlarmStringNoIntensity(db_con=self.db_con, name=name, **kwargs)
        elif name == 'invertertemperatureanomaly':
            alarm = AlarmInverterTemperatureAnomaly(db_con=self.db_con, name=name, **kwargs)
        else:
            raise UndefinedAlarmError

        self.alarms.append(alarm)

    def read_alarms_config(self, yamlfile):
        with open(yamlfile, 'r') as stream:
            alarms = yaml.safe_load(stream)
            return alarms

    def get_alarm_by_name(self, name) -> Alarm:
        return next((alarm for alarm in self.alarms if alarm.name == name), None)

    def insert_alarms_from_config(self, alarms_yaml_content):
        #TODO silently continue if alarms key does not exist?
        for alarm in alarms_yaml_content.get('alarms', []):
            alarm['createdate'] = alarm.get('createdate', datetime.datetime.today())
            self.add_alarm(**alarm)

    # TODO use the db_factory
    def create_alarm_table(self):
        table_name = 'alarm'
        alarm_registry = '''
            CREATE TABLE IF NOT EXISTS
                {}
                (id serial primary key,
                name varchar,
                description varchar,
                severity varchar,
                active boolean,
                createdate date,
                CONSTRAINT "unique_alarm__name" UNIQUE ("name")
            );

        '''.format(table_name)
        self.db_con.execute(alarm_registry)
        return table_name

    def create_alarm_status_table(self):
        table_name = 'alarm_status'
        alarm_registry = f'''
            CREATE TABLE IF NOT EXISTS
                {table_name}
                (id serial primary key,
                device_table varchar,
                device_id integer,
                device_name varchar,
                alarm INTEGER NOT NULL,
                start_time timestamptz,
                update_time timestamptz,
                status boolean,
                CONSTRAINT "fk_alarm_status__alarm" FOREIGN KEY ("alarm") REFERENCES "alarm" ("id") ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_idx_device_table_device_id_alarm ON {table_name}(device_table, device_id, alarm);
        '''
        self.db_con.execute(alarm_registry)
        return table_name

    def create_alarm_historic_table(self):
        table_name = 'alarm_historic'
        alarm_registry = f'''
            CREATE TABLE IF NOT EXISTS
                {table_name}
                (
                    id serial,
                    device_table varchar,
                    device_id integer,
                    device_name varchar,
                    alarm INTEGER NOT NULL,
                    start_time timestamptz,
                    end_time timestamptz,
                    CONSTRAINT "fk_alarm_historic__alarm" FOREIGN KEY ("alarm") REFERENCES "alarm" ("id") ON DELETE CASCADE
                );
        '''
        self.db_con.execute(alarm_registry)

        check_timescale = '''
            SELECT extversion
            FROM pg_extension
            where extname = 'timescaledb';
        '''
        has_timescale = self.db_con.execute(check_timescale).fetchone()

        if has_timescale:
            do_hypertable = '''
            SELECT create_hypertable('alarm_historic','start_time', if_not_exists => TRUE);
            '''
            self.db_con.execute(do_hypertable).fetchone()
        else:
            logger.warning(f"Database {self.db_con.info} does not have timescale")

        return table_name

    def create_alarm_tables(self):
        self.create_alarm_table()
        self.create_alarm_status_table()
        self.create_alarm_historic_table()

    def create_alarms(self):
        from conf import envinfo
        alarms_yaml_file = getattr(envinfo,'ALARMS_YAML','conf/alarms.yaml')
        alarms_yaml_content = self.read_alarms_config(alarms_yaml_file)

        self.insert_alarms_from_config(alarms_yaml_content)

    def update_alarms(self):
        logger.debug("Updating alarms maintenance")
        self.create_alarm_tables()
        self.create_alarms()
        logger.debug("alarm tables creation checked")
        for alarm in self.alarms:
            alarm.update_alarm()
        logger.info("Updated alarms maintenance")