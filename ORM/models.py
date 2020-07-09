#!/usr/bin/env python3

import datetime

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


class Meter(database.Entity):

    plant = Required(Plant)
    name = Required(unicode, unique=True)
    meterRegistries = Set('MeterRegistry')

    def insertRegistry(self,
        export_energy,
        import_energy,
        r1,r2,r3,r4,
        time=None
    ):
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

    meter = Required(Meter)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
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

class SensorTemperature(Sensor):

    sensorRegistries = Set('SensorTemperatureRegistry')

class SensorIntegratedIrradiation(Sensor):

    sensorRegistries = Set('IntegratedIrradiationRegistry')

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

class Forecast(database.Entity):

    forecastMetadata = Required(ForecastMetadata)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    percentil10 = Optional(int)
    percentil50 = Optional(int)
    percentil90 = Optional(int)

