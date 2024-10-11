# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.testing')

import unittest

from pony import orm

import datetime

from plantmonitor.ORM.pony_manager import PonyManager
from plantmonitor.ORM.models import importPlants, exportPlants

from yamlns import namespace as ns

class ORMSetup_Test(unittest.TestCase):

    from yamlns.testutils import assertNsEqual

    def setUp(self):

        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

        orm.rollback()
        self.pony = PonyManager(envinfo.DB_CONF)

        self.pony.define_all_models()
        self.pony.binddb(create_tables=True)

        self.pony.db.drop_all_tables(with_all_data=True)

        self.pony.db.create_tables()
        self.maxDiff=None

        orm.db_session.__enter__()

    def tearDown(self):
        orm.rollback()
        orm.db_session.__exit__()
        self.pony.db.drop_all_tables(with_all_data=True)
        self.pony.db.disconnect()

    def fillPlantRegistries(self, plant):

        timeStart = datetime.datetime(2020, 7, 28, 9, 21, 7, 881064, tzinfo=datetime.timezone.utc)

        sensorsIrr = list(self.pony.db.SensorIrradiation.select(lambda p: p.plant==plant))
        #TODO assert not empty
        sensorIrr = sensorsIrr[0]

        sensorTemp = list(self.pony.db.SensorTemperatureAmbient.select(lambda p: p.plant==plant))[0]

        dt = datetime.timedelta(minutes=5)
        value = 60
        dv = 10

        n = 3

        for i in range(n):
            sensorIrr.insertRegistry(
                time = timeStart + i*dt,
                irradiation_w_m2 = value + i*dv,
                temperature_dc = 300 + value + i*(dv+10),
            )
            sensorTemp.insertRegistry(
                time = timeStart + i*dt,
                temperature_dc = 300 + value + i*(dv+10),
            )
            sensorIrr.insertHourlySensorIrradiationMetric(
                time = timeStart + i*dt,
                integratedIrradiation_wh_m2 = 5000 + value + i*(dv+50),
            )

        plantRegistries = {
            'irradiation': [(timeStart+i*dt, value + i*dv)  for i in range(n)],
            'temperature': [(timeStart+i*dt, 300+value+i*(dv+10))  for i in range(n)],
            'integratedIrr': [(timeStart+i*dt, 5000+value+i*(dv+50)) for i in range(n)],
        }

        return plantRegistries

    def test_Environment(self):
        #TODO will it be too late if the config is misconfigured?
        from conf import envinfo
        self.assertEqual(envinfo.SETTINGS_MODULE, 'conf.settings.testing')

    def test_connection(self):
        with orm.db_session:
            self.assertEqual(self.pony.db.get_connection().status, 1)

    def __test_timescaleTables(self):
        with orm.db_session:
            # orm.set_sql_debug(True)

            tablesToTimescale = self.pony.getTablesToTimescale()

            #no raises
            self.pony.timescaleTables(tablesToTimescale)

            numColumnsTimescaleMetadata = 11
            timescaleStringColumn = 3
            hypertableName = 2

            cur = self.pony.db.execute("select * from _timescaledb_catalog.hypertable")
            hypertables = cur.fetchall()

            hypertablesNames = [t[hypertableName] for t in hypertables if len(t) == numColumnsTimescaleMetadata and t[timescaleStringColumn] == '_timescaledb_internal']

            tablesToTimescaleLowerCase = [name.lower() for name in tablesToTimescale]

            self.assertListEqual(hypertablesNames, tablesToTimescaleLowerCase)


    def test_meters_whenNone(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')

            self.assertMeterListEqual("""\
                data: []
                """)

    def test_InsertOnePlantOneMeter(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            meter = self.pony.db.Meter(name='Mary', plant=alcolea)

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
            for meter in self.pony.db.Meter)]), expected)

    def assertMeterRegistryEqual(self, plantCode, meterName, expected):
        plant = self.pony.db.Plant.get(codename=plantCode)
        meter = self.pony.db.Meter.get(plant=plant, name=meterName)
        registry = [
            ns(
                # yamlns reads datetimes just as date, compare the string
                time = str(line.time),
                export_energy_wh = line.export_energy_wh,
                import_energy_wh = line.import_energy_wh,
                r1_VArh = line.r1_VArh,
                r2_VArh = line.r2_VArh,
                r3_VArh = line.r3_VArh,
                r4_VArh = line.r4_VArh,
            )
            for line in orm.select(
                l for l in self.pony.db.MeterRegistry
                if l.meter == meter
            )
        ]
        self.assertNsEqual(ns(registry=registry), expected)

    def test_registry_whenEmpty(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea',  codename='SOMSC01', description='descripción de planta')
            meter = self.pony.db.Meter(name='Mary', plant=alcolea)
            self.assertMeterRegistryEqual('SOMSC01', 'Mary', """\
                registry: []
                """)

    def test_registry_singleEntry(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea',  codename='SOMSC01', description='descripción de planta')
            meter = self.pony.db.Meter(name='Mary', plant=alcolea)
            meter.insertRegistry(
                time = datetime.datetime(2020,10,20,0,0,0, tzinfo=datetime.timezone.utc),
                export_energy_wh = 10,
                import_energy_wh = 77,
                r1_VArh = 0,
                r2_VArh = 0,
                r3_VArh = 0,
                r4_VArh = 0,
            )
            self.assertMeterRegistryEqual('SOMSC01', 'Mary', """\
                registry:
                - time: '2020-10-20 00:00:00+00:00'
                  export_energy_wh: 10
                  import_energy_wh: 77
                  r1_VArh: 0
                  r2_VArh: 0
                  r3_VArh: 0
                  r4_VArh: 0
                """)

    def test_InverterRegistry_singleEntry(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea',  codename='SOMSC01', description='descripción de planta')
            inverter = self.pony.db.Inverter(name='Mary', plant=alcolea)
            inverterDataDict = {
                'time': datetime.datetime(2020,10,20,0,0,0, tzinfo=datetime.timezone.utc),
                'power_w': 10,
                'energy_wh': 10,
                'intensity_cc_mA': 10,
                'intensity_ca_mA': 10,
                'voltage_cc_mV': 10,
                'voltage_ca_mV': 10,
                'uptime_h': 10,
                'temperature_dc': 10,
            }

            inverter.insertRegistry(**inverterDataDict)
            expectedRegistry = inverterDataDict
            expectedRegistry['inverter'] = 1

            oneRegistry = orm.select(r for r in self.pony.db.InverterRegistry).first()
            oneRegistryList = oneRegistry.to_dict()
            self.assertDictEqual(expectedRegistry, oneRegistryList)

    def test_InvertersRegistries_multipleRegistries(self):
        pass

    def test_InsertOnePlantOneMeterOneRegistry(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea',  codename='SOMSC01', description='descripción de planta')
            meter = self.pony.db.Meter(name='Mary', plant=alcolea)
            meterRegistry = self.pony.db.MeterRegistry(
                meter = meter,
                time = datetime.datetime.now(datetime.timezone.utc),
                export_energy_wh = 10,
                import_energy_wh = 77,
                r1_VArh = 0,
                r2_VArh = 0,
                r3_VArh = 0,
                r4_VArh = 0,
            )

    def test_ReadOnePlantOneMeterOneRegistry(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='A fotovoltaic plant')
            meter = self.pony.db.Meter(name='Mary', plant=alcolea)
            meterRegistry = self.pony.db.MeterRegistry(
                meter = meter,
                time = datetime.datetime.now(datetime.timezone.utc),
                export_energy_wh = 10,
                import_energy_wh = 77,
                r1_VArh = 0,
                r2_VArh = 0,
                r3_VArh = 0,
                r4_VArh = 0,
            )

            alcolea_read = self.pony.db.Plant[1]
            self.assertEqual(alcolea_read, alcolea)
            self.assertEqual(alcolea_read.name, alcolea.name)

    def test_InsertTwoPlantTwoMeterOneRegistry(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea',  codename='SOMSC01', description='descripción de planta')
            alcometer = self.pony.db.Meter(name='meter1', plant=alcolea)
            fonti = self.pony.db.Plant(name='SomEnergia_Fontisolar',  codename='SOMSC02', description='descripción de planta')
            fontimeter = self.pony.db.Meter(name='meter1', plant=fonti)

    def test_InsertOnePlantOneSensor(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            sensor = self.pony.db.SensorTemperatureAmbient(name='TempAlcolea', plant=alcolea)

            sensor_read = self.pony.db.Sensor[1]
            self.assertEqual(sensor_read,sensor)

    def test_InsertOnePlantOneSensorOneRegistry(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            sensor = self.pony.db.SensorIrradiation(name='IrradAlcolea', plant=alcolea)
            sensorRegistry = sensor.insertRegistry(
                time = datetime.datetime.now(datetime.timezone.utc),
                irradiation_w_m2 = 68,
                temperature_dc = 250
            )

            sensor_registry_read = list(self.pony.db.SensorIrradiationRegistry.select())[0]
            self.assertEqual(sensor_registry_read,sensorRegistry)

    def test_InsertOneForecast(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            forecastVariable = self.pony.db.ForecastVariable(name='prod')
            forecastPredictor = self.pony.db.ForecastPredictor(name='aggregated')
            forecastMetadata = self.pony.db.ForecastMetadata(
                errorcode = '',
                plant = alcolea,
                variable = forecastVariable,
                predictor = forecastPredictor,
                forecastdate = datetime.datetime.now(datetime.timezone.utc),
                granularity = 60,
            )
            forecast = forecastMetadata.insertForecast(
                time = datetime.datetime.now(datetime.timezone.utc),
                percentil10 = 10,
                percentil50 = 50,
                percentil90 = 90,
            )

            forecast_read = list(self.pony.db.Forecast.select())[0]
            self.assertEqual(forecast_read,forecast)

    def test_GetRegistriesFromOneSensorInDateRange(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            sensor = self.pony.db.SensorIrradiation(name='IrradAlcolea', plant=alcolea)
            timetz = datetime.datetime(2020, 7, 28, 9, 21, 7, 881064, tzinfo=datetime.timezone.utc)
            dt = datetime.timedelta(minutes=5)
            value = 60
            dv = 10
            for i in range(5):
                self.pony.db.SensorIrradiationRegistry(
                    sensor = sensor,
                    time = timetz + i*dt,
                    irradiation_w_m2 = value + i*dv,
                )

            oneday = datetime.datetime.strptime('2020-07-28','%Y-%m-%d')
            query = orm.select(r.time for r in self.pony.db.SensorIrradiationRegistry
                           if oneday.date() == r.time.date())

            expected = [timetz + i * dt for i in range(5)]

            self.assertListEqual(list(query), expected)

    def test_fixtureCreation(self):
        with orm.db_session:

            plantsNS = ns.loads("""\
                plants:
                - plant:
                    name: alcolea
                    plant_uuid:
                    codename: SCSOM04
                    description: la bonica planta
                    meters:
                    - meter:
                        name: '1234578'
                    inverters:
                    - inverter:
                        name: '5555'
                    - inverter:
                        name: '6666'
                    irradiationSensors:
                    - irradiationSensor:
                        name: alberto
                    temperatureModuleSensors:
                    - temperatureModuleSensor:
                        name: pol
                    temperatureAmbientSensors:
                    - temperatureAmbientSensor:
                        name: joana
            """)

            importPlants(self.pony.db, plantsNS)

        #TODO test the whole fixture, not just the plant data
        expectedPlantns = exportPlants(self.pony.db)
        self.assertNsEqual(expectedPlantns, plantsNS)

    def test_fillPlantRegistries(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            alcolea.createPlantFixture()
            plantRegistries = self.fillPlantRegistries(alcolea)
            n = len(plantRegistries['irradiation'])

            self.assertEqual(n,3)
            self.assertListEqual(list(plantRegistries.keys()), ['irradiation', 'temperature', 'integratedIrr'])

    def test_GetRegistriesFromManySensorsInDateRange(self):
        with orm.db_session:
            alcolea = self.pony.db.Plant(name='SomEnergia_Alcolea', codename='SOMSC01', description='descripción de planta')
            alcolea.createPlantFixture()
            plantRegistries = self.fillPlantRegistries(alcolea)
            n = len(plantRegistries['irradiation'])

            # TODO: First select returns empty results unless we commit
            orm.commit()

            expectedPlantRegistries = [
                (
                    plantRegistries['irradiation'][i][0],
                    plantRegistries['irradiation'][i][1],
                    plantRegistries['temperature'][i][1],
                    plantRegistries['integratedIrr'][i][1],
                ) for i in range(n)
            ]

            q1 = orm.select(r for r in self.pony.db.SensorIrradiationRegistry if r.sensor.plant == alcolea)
            q2 = orm.select(r for r in self.pony.db.SensorTemperatureAmbientRegistry if r.sensor.plant == alcolea)
            q3 = orm.select(r for r in self.pony.db.HourlySensorIrradiationRegistry if r.sensor.plant == alcolea)

            qresult = orm.select(
                (r1_w.time, r1_w.irradiation_w_m2, r2_w.temperature_dc, r3_w.integratedIrradiation_wh_m2, r1_w.sensor, r2_w.sensor, r3_w.sensor)
                for r1_w in q1 for r2_w in q2 for r3_w in q3
                if r1_w.time == r2_w.time and r2_w.time == r3_w.time
            )

            plantRegistries = [(t.astimezone(datetime.timezone.utc), irr, temp, integ) for t, irr, temp, integ, s1, s2, s3 in qresult]

            self.assertListEqual(plantRegistries, expectedPlantRegistries)
