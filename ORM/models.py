#!/usr/bin/env python3

import datetime

from yamlns import namespace as ns

from pony import orm
from pony.orm import (
    Required,
    Optional,
    Set,
    PrimaryKey,
    unicode,
    )

database = orm.Database()

class Plant(database.Entity):

    name = Required(unicode)
    codename = Required(unicode)
    description = Optional(str)
    meters = Set('Meter')
    inverters = Set('Inverter')
    sensors = Set('Sensor')
    forecastMetadatas = Set('ForecastMetadata')

    def importPlant(self, nsplant):
        for plant_foo in nsplant.plants:
            plant = plant_foo.plant
            self.name=plant.name
            self.description=plant.description
            [Meter(plant=self, name=meter['meter'].name) for meter in plant.meters]
            [Inverter(plant=self, name=inverter['inverter'].name) for inverter in plant.inverters]
            [SensorIrradiation(plant=self, name=sensor['irradiationSensor'].name) for sensor in plant.irradiationSensors]
            [SensorTemperature(plant=self, name=sensor['temperatureSensor'].name) for sensor in plant.temperatureSensors]
            [SensorIntegratedIrradiation(plant=self, name=sensor['integratedSensor'].name) for sensor in plant.integratedSensors]
        return self

    def exportPlant(self):
        plantns = ns(plants=[ns(plant=ns(
                name = plant.name,
                codename = plant.codename,
                description = plant.description,
                meters    = [ns(meter=ns(name=meter.name)) for meter in Meter.select(lambda m: m.plant == plant)],
                inverters = [ns(inverter=ns(name=inverter.name)) for inverter in Inverter.select(lambda inv: inv.plant == plant)],
                irradiationSensors = [ns(irradiationSensor=ns(name=sensor.name)) for sensor in SensorIrradiation.select(lambda inv: inv.plant == plant)],
                temperatureSensors = [ns(temperatureSensor=ns(name=sensor.name)) for sensor in SensorTemperature.select(lambda inv: inv.plant == plant)],
                integratedSensors  = [ns(integratedSensor=ns(name=sensor.name)) for sensor in SensorIntegratedIrradiation.select(lambda inv: inv.plant == plant)],
            )) for plant in Plant.select()]
        )
        return plantns

    def createPlantFixture(self):

        Meter(plant=self, name='Meter'+self.name)
        Inverter(plant=self, name='Plant'+self.name)
        SensorIrradiation(plant=self, name='Irrad'+self.name)
        SensorTemperature(plant=self, name='Temp'+self.name)
        SensorIntegratedIrradiation(plant=self, name='IntegIrr'+self.name)


class Meter(database.Entity):

    plant = Required(Plant)
    name = Required(unicode, unique=True)
    meterRegistries = Set('MeterRegistry')

    def insertRegistry(self, export_energy, import_energy, r1, r2, r3, r4, time=None):
        MeterRegistry(
            meter = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            export_energy = export_energy,
            import_energy = import_energy,
            r1 = r1,
            r2 = r2,
            r3 = r3,
            r4 = r4,
            )


class MeterRegistry(database.Entity):

    #id = Required(int, auto=True, unique=True)
    meter = Required(Meter)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(meter, time)
    export_energy = Required(int, size=64)
    import_energy = Required(int, size=64)
    r1 = Required(int, size=64)
    r2 = Required(int, size=64)
    r3 = Required(int, size=64)
    r4 = Required(int, size=64)


class Inverter(database.Entity):

    name = Required(unicode, unique=True)
    plant = Required(Plant)
    inverterRegistries = Set('InverterRegistry')

    def insertRegistry(self,
        HR1,
        HR1_0,
        HR2_2,
        HR3_3,
        HR4_4,
        daily_energy_h,
        daily_energy_l,
        e_total_h,
        e_total_l,
        h_total_h,
        h_total_l,
        pac_r,
        pac_s,
        pac_t,
        powerreactive_t,
        powerreactive_r,
        powerreactive_s,
        probe1value,
        probe2value,
        probe3value,
        probe4value,
        temp_inv,
        time = None,
        ):
        InverterRegistry(
            inverter = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            HR1 = HR1,
            HR1_0 = HR1_0,
            HR2_2 = HR2_2,
            HR3_3 = HR3_3,
            HR4_4 = HR4_4,
            daily_energy_h = daily_energy_h,
            daily_energy_l = daily_energy_l,
            e_total_h = e_total_h,
            e_total_l = e_total_l,
            h_total_h = h_total_h,
            h_total_l = h_total_l,
            pac_r = pac_r,
            pac_s = pac_s,
            pac_t = pac_t,
            powerreactive_t = powerreactive_t,
            powerreactive_r = powerreactive_r,
            powerreactive_s = powerreactive_s,
            probe1value = probe1value,
            probe2value = probe2value,
            probe3value = probe3value,
            probe4value = probe4value,
            temp_inv = temp_inv
        )


class InverterRegistry(database.Entity):

    inverter = Required(Inverter)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    HR1 = Optional(int, size=64)
    HR1_0 = Optional(int, size=64)
    HR2_2 = Optional(int, size=64)
    HR3_3 = Optional(int, size=64)
    HR4_4 = Optional(int, size=64)
    daily_energy_h = Optional(int, size=64)
    daily_energy_l = Optional(int, size=64)
    e_total_h = Optional(int, size=64)
    e_total_l = Optional(int, size=64)
    h_total_h = Optional(int, size=64)
    h_total_l = Optional(int, size=64)
    pac_r = Optional(int, size=64)
    pac_s = Optional(int, size=64)
    pac_t = Optional(int, size=64)
    powereactive_t = Optional(int, size=64)
    powerreactive_r = Optional(int, size=64)
    powerreactive_s = Optional(int, size=64)
    probe1value = Optional(int, size=64)
    probe2value = Optional(int, size=64)
    probe3value = Optional(int, size=64)
    probe4value = Optional(int, size=64)
    temp_inv = Optional(int, size=64)

class Sensor(database.Entity):

    name = Required(unicode, unique=True)
    plant = Required(Plant)
    description = Optional(str)

class SensorIrradiation(Sensor):

    sensorRegistries = Set('SensorIrradiationRegistry')

    def insertRegistry(self, irradiation_w_m2, time=None):
        SensorIrradiationRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            irradiation_w_m2 = irradiation_w_m2
            )


class SensorTemperature(Sensor):

    sensorRegistries = Set('SensorTemperatureRegistry')

    def insertRegistry(self, temperature_c, time=None):
        SensorTemperatureRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            temperature_c = temperature_c
            )

class SensorIntegratedIrradiation(Sensor):

    sensorRegistries = Set('IntegratedIrradiationRegistry')

    def insertRegistry(self, integratedIrradiation_wh_m2, time=None):
        IntegratedIrradiationRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            integratedIrradiation_wh_m2 = integratedIrradiation_wh_m2
            )

class SensorIrradiationRegistry(database.Entity):

    sensor = Required(SensorIrradiation)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    irradiation_w_m2 = Optional(int, size=64)

class SensorTemperatureRegistry(database.Entity):

    sensor = Required(SensorTemperature)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    temperature_c = Optional(int, size=64)

class IntegratedIrradiationRegistry(database.Entity):

    sensor = Required(SensorIntegratedIrradiation)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    integratedIrradiation_wh_m2 = Optional(int, size=64)

class ForecastVariable(database.Entity):

    name = Required(unicode, unique=True)
    forecastMetadatas = Set('ForecastMetadata')

class ForecastPredictor(database.Entity):

    name = Required(unicode, unique=True)
    forecastMetadatas = Set('ForecastMetadata')

class ForecastMetadata(database.Entity):

    errorcode = Optional(str)
    plant     = Required(Plant)
    variable  = Optional(ForecastVariable)
    predictor = Optional(ForecastPredictor)
    forecastdate = Optional(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    granularity = Optional(int)
    forecasts = Set('Forecast')

    def insertForecast(self, percentil10, percentil50, percentil90, time=None):
        IntegratedIrradiationRegistry(
            forecastMetadata = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            percentil10 = percentil10,
            percentil50 = percentil50,
            percentil90 = percentil90
            )


class Forecast(database.Entity):

    forecastMetadata = Required(ForecastMetadata)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    percentil10 = Optional(int)
    percentil50 = Optional(int)
    percentil90 = Optional(int)
