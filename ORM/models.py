#!/usr/bin/env python3

import datetime

from yamlns import namespace as ns

from meteologica.utils import todt

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
    meters = Set('Meter', lazy=True)
    inverters = Set('Inverter', lazy=True)
    sensors = Set('Sensor', lazy=True)
    forecastMetadatas = Set('ForecastMetadata', lazy=True)

    def importPlant(self, nsplant):
        for plant_foo in nsplant.plants:
            plant = plant_foo.plant
            self.name=plant.name
            self.description=plant.description
            if 'meters' in plant:
                [Meter(plant=self, name=meter['meter'].name) for meter in plant.meters]
            if 'inverters' in plant:
                [Inverter(plant=self, name=inverter['inverter'].name) for inverter in plant.inverters]
            if 'irradiationSensors' in plant:
                [SensorIrradiation(plant=self, name=sensor['irradiationSensor'].name) for sensor in plant.irradiationSensors]
            if 'temperatureSensors' in plant:
                [SensorTemperature(plant=self, name=sensor['temperatureSensor'].name) for sensor in plant.temperatureSensors]
            if 'integratedSensors' in plant:
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
    name = Required(unicode)
    meterRegistries = Set('MeterRegistry', lazy=True)

    def insertRegistry(self, export_energy_wh, import_energy_wh, r1_w, r2_w, r3_w, r4_w, time=None):
        return MeterRegistry(
            meter = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            export_energy_wh = export_energy_wh,
            import_energy_wh = import_energy_wh,
            r1_w = r1_w,
            r2_w = r2_w,
            r3_w = r3_w,
            r4_w = r4_w,
            )


class MeterRegistry(database.Entity):

    meter = Required(Meter)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(meter, time)
    export_energy_wh = Required(int, size=64)
    import_energy_wh = Required(int, size=64)
    r1_w = Required(int, size=64)
    r2_w = Required(int, size=64)
    r3_w = Required(int, size=64)
    r4_w = Required(int, size=64)


class Inverter(database.Entity):

    name = Required(unicode)
    plant = Required(Plant)
    inverterRegistries = Set('InverterRegistry', lazy=True)

    def insertRegistry(self,
        daily_energy_h_wh,
        daily_energy_l_wh,
        e_total_h_wh,
        e_total_l_wh,
        h_total_h_h,
        h_total_l_h,
        pac_r_w,
        pac_s_w,
        pac_t_w,
        powerreactive_t_v,
        powerreactive_r_v,
        powerreactive_s_v,
        temp_inv_c,
        time = None,
        ):
        return InverterRegistry(
            inverter = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            daily_energy_h_wh = daily_energy_h_wh,
            daily_energy_l_wh = daily_energy_l_wh,
            e_total_h_wh = e_total_h_wh,
            e_total_l_wh = e_total_l_wh,
            h_total_h_h = h_total_h_h,
            h_total_l_h = h_total_l_h,
            pac_r_w = pac_r_w,
            pac_s_w = pac_s_w,
            pac_t_w = pac_t_w,
            powerreactive_t_v = powerreactive_t_v,
            powerreactive_r_v = powerreactive_r_v,
            powerreactive_s_v = powerreactive_s_v,
            temp_inv_c = temp_inv_c
        )


class InverterRegistry(database.Entity):

    inverter = Required(Inverter)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(inverter, time)
    daily_energy_h_wh = Optional(int, size=64)
    daily_energy_l_wh = Optional(int, size=64)
    e_total_h_wh = Optional(int, size=64)
    e_total_l_wh = Optional(int, size=64)
    h_total_h_h = Optional(int, size=64)
    h_total_l_h = Optional(int, size=64)
    pac_r_w = Optional(int, size=64)
    pac_s_w = Optional(int, size=64)
    pac_t_w = Optional(int, size=64)
    powerreactive_t_v = Optional(int, size=64)
    powerreactive_r_v = Optional(int, size=64)
    powerreactive_s_v = Optional(int, size=64)
    temp_inv_c = Optional(int, size=64)


class Sensor(database.Entity):

    name = Required(unicode)
    plant = Required(Plant)
    description = Optional(str)


class SensorIrradiation(Sensor):

    sensorRegistries = Set('SensorIrradiationRegistry', lazy=True)

    def insertRegistry(self, irradiation_w_m2, time=None):
        return SensorIrradiationRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            irradiation_w_m2 = irradiation_w_m2
            )


class SensorTemperature(Sensor):

    sensorRegistries = Set('SensorTemperatureRegistry', lazy=True)

    def insertRegistry(self, temperature_c, time=None):
        return SensorTemperatureRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            temperature_c = temperature_c
            )


class SensorIntegratedIrradiation(Sensor):

    sensorRegistries = Set('IntegratedIrradiationRegistry', lazy=True)

    def insertRegistry(self, integratedIrradiation_wh_m2, time=None):
        return IntegratedIrradiationRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            integratedIrradiation_wh_m2 = integratedIrradiation_wh_m2
            )


class SensorIrradiationRegistry(database.Entity):

    sensor = Required(SensorIrradiation)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(sensor, time)
    irradiation_w_m2 = Optional(int, size=64)


class SensorTemperatureRegistry(database.Entity):

    sensor = Required(SensorTemperature)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(sensor, time)
    temperature_c = Optional(int, size=64)


class IntegratedIrradiationRegistry(database.Entity):

    sensor = Required(SensorIntegratedIrradiation)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(sensor, time)
    integratedIrradiation_wh_m2 = Optional(int, size=64)


class ForecastVariable(database.Entity):

    name = Required(unicode, unique=True)
    forecastMetadatas = Set('ForecastMetadata', lazy=True)


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
    forecasts = Set('Forecast', lazy=True)

    def insertForecast(self, percentil10, percentil50, percentil90, time=None):
        return Forecast(
            forecastMetadata = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            percentil10 = percentil10,
            percentil50 = percentil50,
            percentil90 = percentil90
            )


class Forecast(database.Entity):

    forecastMetadata = Required(ForecastMetadata)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(forecastMetadata, time)

    percentil10 = Optional(int)
    percentil50 = Optional(int)
    percentil90 = Optional(int)
