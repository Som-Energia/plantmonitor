#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

from pony import orm

import datetime

from .models import database
from .models import (
    Plant,
    Meter,
    MeterRegistry,
    Sensor,
    SensorIntegratedIrradiation,
    SensorIrradiation,
    SensorTemperature,
    SensorIrradiationRegistry,
    SensorTemperatureRegistry,
    IntegratedIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)

from .orm_util import setupDatabase
from yamlns import namespace as ns

setupDatabase()


class ORMSetup_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

    def setUp(self):

        from conf import config
        self.assertEqual(config.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        database.drop_all_tables(with_all_data=True)
        self.maxDiff=None

        database.create_tables()
        
        # database.generate_mapping(create_tables=True)
        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        database.drop_all_tables(with_all_data=True)
        database.disconnect()

    def test_Environment(self):
        #TODO will it be too late if the config is misconfigured?
        from conf import config
        self.assertEqual(config.SETTINGS_MODULE, 'conf.settings.testing')

    def test_connection(self):
        with orm.db_session:
            self.assertEqual(database.get_connection().status, 1)

    def test_meters_whenNone(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')

            self.assertMeterListEqual("""\
                data: []
                """)

    def test_InsertOnePlantOneMeter(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            meter = Meter(name='Mary', plant=alcolea)

            self.assertMeterListEqual("""\
                data:
                - name: Mary
                  plant_name: SomEnergia_Alcolea
                  plant_code: SOMSC01
                  plant_description: descripción de planta
                """)

    def assertMeterListEqual(self, expected):

        self.assertNsEqual(ns(data=[
            ns(
                name = name,
                plant_name = plantname,
                plant_code = plantcode,
                plant_description = description,
            ) for name, plantname, plantcode, description in orm.select((
                meter.name,
                meter.plant.name,
                meter.plant.codename,
                meter.plant.description,
            )
            for meter in Meter)]), expected)

    def assertMeterRegistryEqual(self, plantCode, meterName, expected):
        plant = Plant.get(codename=plantCode)
        meter = Meter.get(plant=plant, name=meterName)
        registry = [
            ns(
                # yamlns reads datetimes just as date, compare the string
                time = str(line.time),
                export_energy = line.export_energy,
                import_energy = line.import_energy,
                r1 = line.r1,
                r2 = line.r2,
                r3 = line.r3,
                r4 = line.r4,
            )
            for line in orm.select(
                l for l in MeterRegistry
                if l.meter == meter
            )
        ]
        self.assertNsEqual(ns(registry=registry), expected)

    def test_registry_whenEmpty(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea',  codename='SOMSC01', description='descripción de planta')
            meter = Meter(name='Mary', plant=alcolea)
            self.assertMeterRegistryEqual('SOMSC01', 'Mary', """\
                registry: []
                """)

    def test_registry_singleEntry(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea',  codename='SOMSC01', description='descripción de planta')
            meter = Meter(name='Mary', plant=alcolea)
            meter.insertRegistry(
                time = datetime.datetime(2020,10,20,0,0,0, tzinfo=datetime.timezone.utc),
                export_energy = 10,
                import_energy = 77,
                r1 = 0,
                r2 = 0,
                r3 = 0,
                r4 = 0,
            )
            self.assertMeterRegistryEqual('SOMSC01', 'Mary', """\
                registry:
                - time: '2020-10-20 00:00:00+00:00'
                  export_energy: 10
                  import_energy: 77
                  r1: 0
                  r2: 0
                  r3: 0
                  r4: 0
                """)


    def test_InsertOnePlantOneMeterOneRegistry(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea',  codename='SOMSC01', description='descripción de planta')
            meter = Meter(name='Mary', plant=alcolea)
            meterRegistry = MeterRegistry(
                meter = meter,
                time = datetime.datetime.now(datetime.timezone.utc),
                export_energy = 10,
                import_energy = 77,
                r1 = 0,
                r2 = 0,
                r3 = 0,
                r4 = 0,
            )

    def test_ReadOnePlantOneMeterOneRegistry(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='A fotovoltaic plant')
            meter = Meter(name='Mary', plant=alcolea)
            meterRegistry = MeterRegistry(
                meter = meter,
                time = datetime.datetime.now(),
                export_energy = 10,
                import_energy = 77,
                r1 = 0,
                r2 = 0,
                r3 = 0,
                r4 = 0,
            )

            alcolea_read = Plant[1]
            self.assertEqual(alcolea_read, alcolea)
            self.assertEqual(alcolea_read.name, alcolea.name)
    
    def test_InsertOnePlantOneSensor(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            sensor = SensorTemperature(name='TempAlcolea', plant=alcolea)

            sensor_read = Sensor[1]
            self.assertEqual(sensor_read,sensor)
    
    def test_InsertOnePlantOneSensorOneRegistry(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            sensor = SensorIrradiation(name='IrradAlcolea', plant=alcolea)
            sensorRegistry = SensorIrradiationRegistry(
                sensor = sensor,
                time = datetime.datetime.now(datetime.timezone.utc),
                irradiation_w_m2 = 68,
            )

            sensor_registry_read = SensorIrradiationRegistry[1]
            self.assertEqual(sensor_registry_read,sensorRegistry)
    
    def test_InsertOneForecast(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            forecastVariable = ForecastVariable(name='prod')
            forecastPredictor = ForecastPredictor(name='aggregated')
            forecastMetadata = ForecastMetadata(
                errorcode = '',
                plant = alcolea,
                variable = forecastVariable,
                predictor = forecastPredictor,
                forecastdate = datetime.datetime.now(datetime.timezone.utc),
                granularity = 60,
            )
            forecast = Forecast(
                forecastMetadata = forecastMetadata,
                time = datetime.datetime.now(datetime.timezone.utc),
                percentil10 = 10,
                percentil50 = 50,
                percentil90 = 90,
            )
            
            forecast_read = Forecast[1]
            self.assertEqual(forecast_read,forecast)

    def test_GetRegistriesFromOneSensorInDateRange(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            sensor = SensorIrradiation(name='IrradAlcolea', plant=alcolea)
            timetz = datetime.datetime(2020, 7, 28, 9, 21, 7, 881064, tzinfo=datetime.timezone.utc)
            dt = datetime.timedelta(minutes=5)
            value = 60
            dv = 10
            for i in range(5):
                SensorIrradiationRegistry(
                    sensor = sensor,
                    time = timetz + i*dt,
                    irradiation_w_m2 = value + i*dv,
                )

            oneday = datetime.datetime.strptime('2020-07-28','%Y-%m-%d')
            query = orm.select(r.time for r in SensorIrradiationRegistry
                           if oneday.date() == r.time.date())

            expected = [timetz + i * dt for i in range(5)]

            self.assertListEqual(list(query), expected)

    def test_GetRegistriesFromManySensorsInDateRange(self):
        with orm.db_session:
            alcolea = Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            sensorIrr = SensorIrradiation(name='IrradAlcolea', plant=alcolea)
            sensorTemp = SensorTemperature(name='TempAlcolea', plant=alcolea)
            sensorIntegratedIrr = SensorIntegratedIrradiation(name='IntegIrrAlcolea', plant=alcolea)

            timetz = datetime.datetime(2020, 7, 28, 9, 21, 7, 881064, tzinfo=datetime.timezone.utc)
            dt = datetime.timedelta(minutes=5)
            value = 60
            dv = 10

            for i in range(3):
                SensorIrradiationRegistry(
                    sensor = sensorIrr,
                    time = timetz + i*dt,
                    irradiation_w_m2 = value + i*dv,
                )
                SensorTemperatureRegistry(
                    sensor = sensorTemp,
                    time = timetz + i*dt,
                    temperature_c = 300 + value + i*(dv+10),
                )
                IntegratedIrradiationRegistry(
                    sensor = sensorIntegratedIrr,
                    time = timetz + i*dt,
                    integratedIrradiation_wh_m2 = 5000 + value + i*(dv+50),
                )

            expectedPlantRegistries = [((timetz+i*dt), value + i*dv, 300+value+i*(dv+10), 5000+value+i*(dv+50)) for i in range(3)]
            
            # TODO: First select returns empty results unless we commit
            orm.commit()

            q1 = orm.select(r for r in SensorIrradiationRegistry if r.sensor.plant == alcolea)
            q2 = orm.select(r for r in SensorTemperatureRegistry if r.sensor.plant == alcolea)
            q3 = orm.select(r for r in IntegratedIrradiationRegistry if r.sensor.plant == alcolea)

            qresult = orm.select(
                (r1.time, r1.irradiation_w_m2, r2.temperature_c, r3.integratedIrradiation_wh_m2, r1.sensor, r2.sensor, r3.sensor) 
                for r1 in q1 for r2 in q2 for r3 in q3 
                if r1.time == r2.time and r2.time == r3.time
            )

            plantRegistries = [(t.astimezone(datetime.timezone.utc), irr, temp, integ) for t, irr, temp, integ, s1, s2, s3 in qresult]

            self.assertListEqual(plantRegistries, expectedPlantRegistries)
