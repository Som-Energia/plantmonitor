import datetime

from pony import orm
from pony.orm import (
    Required,
    Optional,
    Set,
    PrimaryKey,
    unicode,
    )

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

# TODO definir una Base Class i heredarla per fer un manager per usecase e.g. PlantmonitorManager, SolarManager...

class PonyManager:

    def __init__(self, database_info):
        self.db = orm.Database()
        self.database_info = database_info
        self.timescaled_tables = []

    ''' creates tables '''
    def binddb(self, createTables=True):
        self.db.bind(**self.database_info)
        self.db.generate_mapping(create_tables=createTables, check_tables=True)

    def get_db(self):
        return self.db

    def timescale_tables(self):
        for t in self.timescaled_tables:
            print(f"Timescaling {t}")
            self.db.execute("SELECT create_hypertable('{}', 'time');".format(t.lower()))

    def create_views(self):
        self.db.execute("CREATE VIEW view_device AS select *, TRUE as filtered from device where device = 1;")

    def define_views(self):
        # TODO map previous view as a Pony object
        class ViewDevice(self.db.Entity):
            table = 'view_device'
            name = Required(str)
            filtered = Required(bool)

    def define_solar_models(self):

        class Municipality(self.db.Entity):

            ineCode = Required(str)
            name = Required(unicode)

            countryCode = Optional(str, nullable=True)
            country = Optional(unicode, nullable=True)
            regionCode = Optional(str, nullable=True)
            region = Optional(unicode, nullable=True)
            provinceCode = Optional(str, nullable=True)
            province = Optional(unicode, nullable=True)

            plants = Set('Plant')

        class Plant(self.db.Entity):

            name = Required(unicode)
            codename = Required(unicode)
            #TODO make municipality required
            municipality = Optional(Municipality)
            location = Optional("PlantLocation")
            description = Optional(str)
            plantParameters = Optional('PlantParameters')
            moduleParameters = Optional('PlantModuleParameters')
            plantMonthlyLegacy = Optional('PlantMonthlyLegacy')

            solarEvents = Set('SolarEvent')

        class PlantLocation(self.db.Entity):
            plant = Required(Plant)
            latitude = Required(float)
            longitude = Required(float)

            def getLatLong(self):
                return (self.latitude, self.longitude)

        class PlantParameters(self.db.Entity):
            plant = Required(Plant)
            peak_power_w = Required(int)
            nominal_power_w = Required(int)
            connection_date = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
            n_strings_plant = Optional(int)
            n_strings_inverter = Optional(int) # TODO poden tenir diferent nombre d'strings els inversors d'una planta?
            n_modules_string = Optional(int)
            inverter_loss_mpercent = Optional(int) # TODO és fixe a la planta o canvia amb l'inversor?
            meter_loss_mpercent = Optional(int) # TODO és fixe a la planta o canvia amb el comptador?

            target_monthly_energy_wh = Required(int)
            historic_monthly_energy_wh = Optional(int)
            month_theoric_pr_cpercent = Optional(int)
            year_theoric_pr_cpercent = Optional(int)

        class PlantModuleParameters(self.db.Entity):
            plant = Required(Plant)
            brand = Optional(str, nullable=True)
            model = Optional(str, nullable=True)
            nominal_power_wp = Required(int, sql_default='250000')
            efficency_cpercent = Required(int, sql_default='1550')
            n_modules = Required(int)
            degradation_cpercent = Required(int)
            max_power_current_ma = Required(int)
            max_power_voltage_mv = Required(int)
            current_temperature_coefficient_mpercent_c = Required(int)
            voltage_temperature_coefficient_mpercent_c = Required(int)
            max_power_temperature_coefficient_mpercent_c = Required(int, sql_default='-442')
            standard_conditions_irradiation_w_m2 = Required(int)
            standard_conditions_temperature_dc = Required(int)
            opencircuit_voltage_mv = Required(int)
            shortcircuit_current_ma = Required(int)
            expected_power_correction_factor_cpercent = Required(int, sql_default='10000')

        class PlantMonthlyLegacy(self.db.Entity):
            """
            Data imported from historical drive spreadshet
            """
            plant = Required(Plant)
            time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
            export_energy_wh = Required(int, size=64)


        class SolarEvent(self.db.Entity):

            plant = Required(Plant)

            sunrise = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE')
            sunset =  Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE')

            @classmethod
            def insertSolarEvents(cls, solarevents):
                for se in solarevents:
                    #self.db.SolarEvent(se)
                    plant_name, sunrise, sunset = se
                    p = Plant.get(name=plant_name)
                    if not p:
                        logger.warning("Plant {} is unknown to database".format(plant_name))
                    else:
                        self.db.SolarEvent(plant=p, sunrise=sunrise, sunset=sunset)
