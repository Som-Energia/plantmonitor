import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

from unittest import TestCase
from ORM.models import (
    database,
    Plant,
    SensorIrradiation,
    SensorIrradiationRegistry,
    PlantModuleParameters,
)
from pony import orm
from pathlib import Path
from .testing_utils import (
    readTimedDataTsv,
    parseDate,
)
from .operations import integrateExpectedPower
from yamlns import namespace as ns
from yamlns.dateutils import Date
import datetime
# import pytz
from ORM.db_utils import setupDatabase
setupDatabase(create_tables=True, timescale_tables=False, drop_tables=True)
from decimal import Decimal

class ExpectedPower_Test(TestCase):
    from b2btest.b2btest import assertB2BEqual

    @classmethod
    def setUpClass(cls):
        ''

    def setUp(self):
        self.maxDiff=None
        self.b2bdatapath='b2bdata'
        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        database.drop_all_tables(with_all_data=True)

        #orm.set_sql_debug(True)
        database.create_tables()


        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()

    def samplePlantNS(self): # Copied from test_models.py:samplePlantNS
        alcoleaPlantNS = ns.loads("""\
            name: myplant
            codename: PLANTCODE
            description: Plant Description
            meters:
            - meter:
                name: '1234578'
            irradiationSensors:
            - irradiationSensor:
                name: irradiationSensor1
        """)
        return alcoleaPlantNS

    def setupPlant(self):
        plantDefinition = self.samplePlantNS()
        plant = Plant(
            name=plantDefinition.name,
            codename=plantDefinition.codename,
        )
        plant.importPlant(plantDefinition)
        plant.flush()
        self.plant = plant.id
        self.sensor = SensorIrradiation.select().first().id

    def importData(self, sensor, filename, outputColumn):
        tsvContent = readTimedDataTsv(filename, [
            'Temperatura modul',
            'Irradiación (W/m2)',
            outputColumn or 'Potencia parque calculada con temperatura kW con degradación placas',
        ])
        for time, temperature_c, irradiation_w_m2, outputValue in tsvContent:
            SensorIrradiationRegistry(
                sensor=self.sensor,
                time=datetime.datetime.fromisoformat(time),
                irradiation_w_m2=int(round(irradiation_w_m2)),
                temperature_dc=int(round(temperature_c*10)),
            )
        database.flush()

        return [
            (time, self.plant, self.sensor, outputValue)
            for time, temperature_c, irradiation_w_m2, outputValue in tsvContent
        ]

    parametersAlcolea = dict(
        nominalPowerMWp = 2.16,
        efficiency = 15.5,
        nModules = 8640, # plant parameter
        Imp = 8.27, # A, module model param
        Vmp = 30.2, # V, module model param
        temperatureCoefficientI = 0.088, # %/ºC, module model param
        temperatureCoefficientV = -0.352, # %/ºC, module model param
        temperatureCoefficientPmax = -0.442, # %/ºC, module model param
        irradiationSTC = 1000.0, # W/m2, module model param
        temperatureSTC = 25, # ºC, module model param
        Isc = 8.75, # A, module model param
        Voc = 37.8, # V, module model param
        #degradation=97.0, # %, module model param
        degradation=95*.9, # %, module model param # According the excel formula
        correctionFactorPercent = 90., # %, plant parameter
    )

    parametersFlorida = dict(
        nominalPowerMWp = 2.16, # TODO use Florida values
        efficiency = 15.5, # TODO use Florida values
        nModules = 4878, # plant parameter
        Imp = 9.07, # A, module model param
        Vmp = 37.5, # V, module model param
        temperatureCoefficientI = 0.05, # %/ºC, module model param
        temperatureCoefficientV = -0.31, # %/ºC, module model param
        temperatureCoefficientPmax = -0.442, # %/ºC, module model param
        irradiationSTC = 1000.0, # W/m2, module model param
        temperatureSTC = 25, # ºC, module model param
        degradation=97.5, # %, module model param
        Isc = 9.5, # A, module model param
        Voc = 46.1, # V, module model param
        correctionFactorPercent = 90., # %, plant parameter
    )

    def setPlantParameters(self, **data):
        data = ns(data)
        plant = PlantModuleParameters(
            plant=self.plant,
            nominal_power_wp=int(data.nominalPowerMWp*1000000),
            efficency_cpercent=int(data.efficiency*100),
            n_modules = data.nModules,
            max_power_current_ma = int(data.Imp*1000),
            max_power_voltage_mv = int(data.Vmp*1000),
            current_temperature_coefficient_mpercent_c = int(data.temperatureCoefficientI*1000),
            voltage_temperature_coefficient_mpercent_c = int(data.temperatureCoefficientV*1000),
            max_power_temperature_coefficient_mpercent_c = int(data.temperatureCoefficientPmax*1000),
            standard_conditions_irradiation_w_m2 = int(data.irradiationSTC),
            standard_conditions_temperature_dc = int(data.temperatureSTC*10),
            degradation_cpercent = int(data.degradation*100),
            opencircuit_voltage_mv = int(data.Voc*1000),
            shortcircuit_current_ma = int(data.Isc*1000),
            expected_power_correction_factor_cpercent = int(data.get('correctionFactorPercent', 100)*100),
        )
        plant.flush()

    def assertOutputB2B(self, result):
        def x2utcisoformat(date):
            if not hasattr(date,'isoformat'):
                date=datetime.datetime.fromisoformat(date)
            date=date.astimezone(datetime.timezone.utc)
            return date.isoformat()

        result = "\n".join((
            "{}\n{:.7f}".format(x2utcisoformat(time),value)
            for time, plant, sensor, value, temperature, irradiance in result
        ))
        self.assertB2BEqual(result)

    # TODO fancy_replace calls make B2B too slow when changing timezone
    # see https://bugs.python.org/issue6931 I guess
    # def assertOutputB2BNoPlant(self, result):
    #     tz = pytz.timezone("Europe/Madrid")
    #     result = "\n".join((
    #         "{},{},{:.9f}".format(t[0].astimezone(tz).isoformat(),sensor.name,t[1])
    #         for sensor, l in result.items() for t in l
    #     ))
    #     self.assertB2BEqual(result)

    def assertOutputB2BNoPlant(self, result):
        result = "\n".join((
            "{},{},{:.9f}".format(t[0].isoformat(),sensor.name,t[1])
            for sensor, l in result.items() for t in l
        ))
        self.assertB2BEqual(result)

    def test_expectedPower_Florida_2020_09(self):
        self.setupPlant()
        self.setPlantParameters(**dict(self.parametersFlorida,
            correctionFactorPercent=100,
        ))
        expected = self.importData(self.sensor,
            'b2bdata/expectedPower-2020-09-Florida.csv',
            'Potencia parque calculada con temperatura kW con degradación placas',
        )
        query = Path('queries/view_expected_power.sql').read_text(encoding='utf8')
        result = database.select(query)

        self.assertOutputB2B(result) # first run with expected instead result

    def test_expectedPower_Florida_2020_09_withCorrection(self):
        self.setupPlant()
        self.setPlantParameters(**dict(self.parametersFlorida,
            correctionFactorPercent=90,
        ))
        expected = self.importData(self.sensor,
            'b2bdata/expectedPower-2020-09-Florida.csv',
            'Potencia parque calculada con temperatura kW con degradación placas',
        )
        query = Path('queries/view_expected_power.sql').read_text(encoding='utf8')
        result = database.select(query)

        self.assertOutputB2B(result) # first run with expected instead result

    def test_expectedPower_Alcolea_2021_04_noCorrections(self):
        self.setupPlant()
        self.setPlantParameters(**dict(self.parametersAlcolea,
            correctionFactorPercent=100,
        ))
        expected = self.importData(self.sensor,
            'b2bdata/expectedPower-2021-04-Alcolea.csv',
            'Potencia parque calculada con temperatura kW con degradación placas',
        )
        query = Path('queries/view_expected_power.sql').read_text(encoding='utf8')
        result = database.select(query)

        self.assertOutputB2B(result) # first run with expected instead result

    def test_integrateExpectedPower_Florida_2020_09(self):
        self.setupPlant()
        self.setPlantParameters(**dict(self.parametersFlorida,
            correctionFactorPercent=100,
        ))
        expected = None
        # expected = self.importData(self.sensor,
        #     'b2bdata/expectedEnergy-2020-09-Florida.csv',
        #     'Potencia parque calculada con temperatura kW con degradación placas',
        # )
        self.importData(self.sensor,
            'b2bdata/expectedPower-2020-09-Florida.csv',
            'Potencia parque calculada con temperatura kW con degradación placas',
        )

        time = datetime.datetime(2020, 9, 1, 2, 5, 10, 588861, tzinfo=datetime.timezone.utc)
        fromDate = time.replace(hour=14, minute=0, second=0, microsecond=0)
        toDate = fromDate + datetime.timedelta(days=30)

        query = Path('queries/view_expected_power.sql').read_text(encoding='utf8')
        expectedPowerQuery = database.select(query)

        # end of setup

        result = integrateExpectedPower(fromDate, toDate)

        self.assertOutputB2BNoPlant(result)

    # TODO add expectedEnergy as a view instead of a redash query
    def _test_expectedEnergy_Alcolea_2021_04(self):
        self.setupPlant()
        self.setPlantParameters(**dict(self.parametersFlorida,
            correctionFactorPercent=90,
        ))
        self.importData(self.sensor,
            'b2bdata/expectedPower-2021-04-Alcolea.csv',
            'Potencia parque calculada con temperatura kW con degradación placas',
        )
        # TODO how to add a view on a test?
        query = Path('queries/view_expected_power.sql').read_text(encoding='utf8')
        result = database.select(query)

        query = Path('queries/view_expected_energy.sql').read_text(encoding='utf8')
        result = database.select(query)

        self.assertOutputB2B(result) # first run with expected instead result
