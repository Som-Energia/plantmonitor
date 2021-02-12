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

from conf.logging_configuration import LOGGING
import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

database = orm.Database()

def getRegistries(entitySet, exclude, fromdate=None, todate=None):
        if fromdate and todate:
            registries = orm.select(r for r in entitySet if fromdate <= r.time and r.time <= todate)[:]
        else:
            registries = orm.select(r for r in entitySet)[:]
        return [r.to_dict(exclude=exclude) for r in registries]

def importPlants(nsplants):
    with orm.db_session:
        if 'municipalities' in nsplants:
            for kmunicipality in nsplants.municipalities:
                municipality = kmunicipality['municipality']
                municipality = Municipality(
                    name=municipality.name,
                    ineCode=municipality.ineCode,
                    countryCode=municipality.countryCode if "countryCode" in municipality else None,
                    country=municipality.country if "countryCode" in municipality else None,
                    regionCode=municipality.regionCode if "regionCode" in municipality else None,
                    region=municipality.region if "region" in municipality else None,
                    provinceCode=municipality.provinceCode if "provinceCode" in municipality else None,
                    province=municipality.province if "province" in municipality else None,
                )
        if 'plants' in nsplants:
            for kplantns in nsplants.plants:
                plantns = kplantns['plant']
                plant = Plant(name=plantns.name, codename=plantns.codename)
                plant.importPlant(plantns)

def exportPlants(skipEmpty=False):
    plantsns = ns()
    with orm.db_session:
        if Municipality.select().exists():
            plantsns['municipalities'] = [ns([
                    ('municipality', municipality.exportMunicipality())
                    ])
                    for municipality in Municipality.select()]

        plantsns['plants'] = [ns([
                    ('plant', plant.exportPlant(skipEmpty))
                ])
                for plant in Plant.select()]
    return plantsns

# TODO might be useful to insert data from several plants at the same time
# requires lingua franca dictionary re-nesting
# def insertPlantsData(plants_data):
#     results = [plant.insertPlantData(plant_data) for plant_name,plant_data in plants_data]

class Municipality(database.Entity):

    ineCode = Required(str)
    name = Required(unicode)

    countryCode = Optional(str, nullable=True)
    country = Optional(unicode, nullable=True)
    regionCode = Optional(str, nullable=True)
    region = Optional(unicode, nullable=True)
    provinceCode = Optional(str, nullable=True)
    province = Optional(unicode, nullable=True)

    plants = Set('Plant')

    def exportMunicipality(self):

        municipalityns = ns(
            ineCode = self.ineCode,
            name = self.name)
        if self.countryCode:
            municipalityns.countryCode = self.countryCode
        if self.country:
            municipalityns.country = self.country
        if self.regionCode:
            municipalityns.regionCode = self.regionCode
        if self.region:
            municipalityns.region = self.region
        if self.provinceCode:
            municipalityns.provinceCode = self.provinceCode
        if self.province:
            municipalityns.province = self.province
        return municipalityns

class Plant(database.Entity):

    name = Required(unicode)
    codename = Required(unicode)
    #TODO make municipality required
    municipality = Optional(Municipality)
    location = Optional("PlantLocation")
    description = Optional(str)
    meters = Set('Meter')
    inverters = Set('Inverter', lazy=True)
    sensors = Set('Sensor', lazy=True)
    forecastMetadatas = Set('ForecastMetadata', lazy=True)
    inclinometers = Set('Inclinometer', lazy=True)
    anemometer = Set('Anemometer', lazy=True)
    omie = Set('Omie', lazy=True)
    marketRepresentative = Set('MarketRepresentative', lazy=True)
    simel = Set('Simel', lazy=True)
    nagios = Set('Nagios', lazy=True)

    @classmethod
    def insertPlantsData(cls, plantsData):
        for plantData in plantsData:
            logger.debug("importing plant data for plant {}".format(plantData['plant']))
            plant = Plant.get(name=plantData['plant'])
            if not plant:
                logger.error("Plant {} is not known. Known plants: {}".format(plantData['plant'], Plant.select('name')))
            else:
                plant.insertPlantData(plantData)

    def importPlant(self, nsplant):
        plant = nsplant
        self.name = plant.name
        self.description = plant.description
        if 'municipality' in plant:
            m = Municipality.get(ineCode=plant['municipality'])
            if m:
                self.municipality = Municipality.get(ineCode=plant['municipality'])
            else:
                #TODO error or exception?
                logger.error("Error: municipality {} not found".format(plant['municipality']))
                raise
        if 'meters' in plant:
            [Meter(plant=self, name=meter['meter'].name) for meter in plant.meters]
        if 'inverters' in plant:
            [Inverter(plant=self, name=inverter['inverter'].name) for inverter in plant.inverters]
        if 'irradiationSensors' in plant:
            [SensorIrradiation(plant=self, name=sensor['irradiationSensor'].name) for sensor in plant.irradiationSensors]
        if 'temperatureAmbientSensors' in plant:
            [SensorTemperatureAmbient(plant=self, name=sensor['temperatureAmbientSensor'].name) for sensor in plant.temperatureAmbientSensors]
        if 'temperatureModuleSensors' in plant:
            [SensorTemperatureModule(plant=self, name=sensor['temperatureModuleSensor'].name) for sensor in plant.temperatureModuleSensors]
        if 'integratedSensors' in plant:
            [SensorIntegratedIrradiation(plant=self, name=sensor['integratedSensor'].name) for sensor in plant.integratedSensors]
        return self

    def exportPlant(self, skipEmpty=False):
        # print([meter.to_dict() for meter in Meter.select(lambda m: m.plant == self)])

        if not skipEmpty:
            plantns = ns(
                    name = self.name,
                    codename = self.codename,
                    description = self.description,
                    meters    = [ns(meter=ns(name=meter.name)) for meter in Meter.select(lambda m: m.plant == self)],
                    inverters = [ns(inverter=ns(name=inverter.name)) for inverter in Inverter.select(lambda inv: inv.plant == self)],
                    irradiationSensors = [ns(irradiationSensor=ns(name=sensor.name)) for sensor in SensorIrradiation.select(lambda inv: inv.plant == self)],
                    temperatureAmbientSensors = [ns(temperatureAmbientSensor=ns(name=sensor.name)) for sensor in SensorTemperatureAmbient.select(lambda inv: inv.plant == self)],
                    temperatureModuleSensors = [ns(temperatureModuleSensor=ns(name=sensor.name)) for sensor in SensorTemperatureModule.select(lambda inv: inv.plant == self)],
                    integratedSensors  = [ns(integratedSensor=ns(name=sensor.name)) for sensor in SensorIntegratedIrradiation.select(lambda inv: inv.plant == self)],
                )
        else:
            plantns = ns(
                    name = self.name,
                    codename = self.codename,
                    description = self.description
            )
            meters    = [ns(meter=ns(name=meter.name)) for meter in Meter.select(lambda m: m.plant == self)]
            inverters = [ns(inverter=ns(name=inverter.name)) for inverter in Inverter.select(lambda inv: inv.plant == self)]
            irradiationSensors = [ns(irradiationSensor=ns(name=sensor.name)) for sensor in SensorIrradiation.select(lambda inv: inv.plant == self)]
            temperatureAmbientSensors = [ns(temperatureAmbientSensor=ns(name=sensor.name)) for sensor in SensorTemperatureAmbient.select(lambda inv: inv.plant == self)]
            temperatureModuleSensors = [ns(temperatureModuleSensor=ns(name=sensor.name)) for sensor in SensorTemperatureModule.select(lambda inv: inv.plant == self)]
            integratedSensors  = [ns(integratedSensor=ns(name=sensor.name)) for sensor in SensorIntegratedIrradiation.select(lambda inv: inv.plant == self)]
            if meters:
                plantns['meters'] = meters
            if inverters:
                plantns['inverters'] = inverters
            if irradiationSensors:
                plantns['irradiationSensors'] = irradiationSensors
            if temperatureAmbientSensors:
                plantns['temperatureAmbientSensors'] = temperatureAmbientSensors
            if temperatureModuleSensors:
                plantns['temperatureModuleSensors'] = temperatureModuleSensors
            if integratedSensors:
                plantns['integratedSensors'] = integratedSensors

        if self.municipality:
            plantns.municipality = self.municipality.ineCode
        logger.debug(plantns.dump())
        return plantns

    def createPlantFixture(self):

        Meter(plant=self, name='Meter'+self.name)
        Inverter(plant=self, name='Plant'+self.name)
        SensorIrradiation(plant=self, name='Irrad'+self.name)
        SensorTemperatureAmbient(plant=self, name='TempAmb'+self.name)
        SensorTemperatureModule(plant=self, name='TempMod'+self.name)
        SensorIntegratedIrradiation(plant=self, name='IntegIrr'+self.name)

    def plantData(self, fromdate=None, todate=None, skipEmpty=False):
        data = {"plant": self.name}

        #TODO generalize this
        meterList = [{
            "id":"Meter:{}".format(m.name), "readings": m.getRegistries(fromdate, todate)
            } for m in orm.select(mc for mc in self.meters) if not skipEmpty or m.getRegistries(fromdate, todate)]

        inverterList = [{
            "id":"Inverter:{}".format(i.name), "readings": i.getRegistries(fromdate, todate)
            } for i in orm.select(ic for ic in self.inverters) if not skipEmpty or i.getRegistries(fromdate, todate)]
        sensorList = [i.toDict(fromdate, todate) for i in orm.select(ic for ic in self.sensors) if not skipEmpty or i.getRegistries(fromdate, todate)]
        forecastMetadatasList = [{
            "id":"ForecastMetadatas:{}".format(i.name), "readings": i.getRegistries(fromdate, todate)
            } for i in orm.select(ic for ic in self.forecastMetadatas) if not skipEmpty or i.getRegistries(fromdate, todate)]

        data["devices"] = inverterList + meterList + forecastMetadatasList + sensorList
        data["devices"].sort(key=lambda x : x['id'])

        return data

    def str2model(self, classname, devicename):
        #TODO generalize this
        #TODO distinguish between failure due to missing name and unknown class
        if classname == "Meter":
            return Meter.get(name=devicename)
        if classname == "Inverter":
            return Inverter.get(name=devicename)
        if classname == "ForecastMetadata":
            return ForecastMetadata.get(name=devicename)
        if classname == "SensorIrradiation":
            return SensorIrradiation.get(name=devicename)
        if classname == "SensorTemperatureAmbient":
            return SensorTemperatureAmbient.get(name=devicename)
        if classname == "SensorTemperatureModule":
            return SensorTemperatureModule.get(name=devicename)
        if classname == "SensorIntegratedIrradiation":
            return SensorIntegratedIrradiation.get(name=devicename)
        return None

    def insertDeviceData(self, devicedata, packettime=None):
        devicetype, devicename = devicedata["id"].split(":")
        device = self.str2model(classname=devicetype, devicename=devicename)
        if not device:
            logger.error("unknown device {}:{}".format(devicetype, devicename))
            return None
        return [device.insertRegistry(**{**{"time":packettime}, **r}) for r in devicedata["readings"]]

    def insertPlantData(self, plantdata):
        if self.name != plantdata["plant"]:
            return False
        packettime = plantdata.get("time")
        return [self.insertDeviceData(d, packettime) for d in plantdata["devices"]]

class PlantLocation(database.Entity):
    plant = Required(Plant)
    latitude = Required(float)
    longitude = Required(float)

    def getLatLong(self):
        return (self.latitude, self.longitude)

class Meter(database.Entity):

    plant = Required(Plant)
    name = Required(unicode)
    meterRegistries = Set('MeterRegistry', lazy=True)

    def insertRegistry(self, export_energy_wh, import_energy_wh, r1_VArh, r2_VArh, r3_VArh, r4_VArh, time=None):
        return MeterRegistry(
            meter = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            export_energy_wh = export_energy_wh,
            import_energy_wh = import_energy_wh,
            r1_VArh = r1_VArh,
            r2_VArh = r2_VArh,
            r3_VArh = r3_VArh,
            r4_VArh = r4_VArh,
            )

    # TODO: convert to a fixture this function
    def getRegistries(self, fromdate=None, todate=None):
        readings = getRegistries(self.meterRegistries, exclude='meter', fromdate=fromdate, todate=todate)
        return readings

    # TODO: implement
    @classmethod
    def getLastReadingsDate(cls):

        return []

class MeterRegistry(database.Entity):

    meter = Required(Meter)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(meter, time)
    export_energy_wh = Required(int, size=64)
    import_energy_wh = Required(int, size=64)
    r1_VArh = Required(int, size=64)
    r2_VArh = Required(int, size=64)
    r3_VArh = Required(int, size=64)
    r4_VArh = Required(int, size=64)


class Inverter(database.Entity):

    name = Required(unicode)
    plant = Required(Plant)
    inverterRegistries = Set('InverterRegistry', lazy=True)

    def insertRegistry(self,
        power_w,
        energy_wh,
        intensity_cc_mA,
        intensity_ca_mA,
        voltage_cc_mV,
        voltage_ca_mV,
        uptime_h,
        temperature_dc,
        time=None,
        ):
        return InverterRegistry(
            inverter = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            power_w = power_w,
            energy_wh = energy_wh,
            intensity_cc_mA = intensity_cc_mA,
            intensity_ca_mA = intensity_ca_mA,
            voltage_cc_mV = voltage_cc_mV,
            voltage_ca_mV = voltage_ca_mV,
            uptime_h = uptime_h,
            temperature_dc = temperature_dc,
        )

    def getRegistries(self, fromdate=None, todate=None):
        readings = getRegistries(self.inverterRegistries, exclude='inverter', fromdate=fromdate, todate=todate)
        return readings

class InverterRegistry(database.Entity):

    inverter = Required(Inverter)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(inverter, time)
    power_w = Optional(int, size=64)
    energy_wh = Optional(int, size=64)
    intensity_cc_mA = Optional(int, size=64)
    intensity_ca_mA = Optional(int, size=64)
    voltage_cc_mV = Optional(int, size=64)
    voltage_ca_mV = Optional(int, size=64)
    uptime_h = Optional(int, size=64)
    temperature_dc = Optional(int, size=64)


class Sensor(database.Entity):

    name = Required(unicode)
    plant = Required(Plant)
    description = Optional(str)


class SensorIrradiation(Sensor):

    sensorRegistries = Set('SensorIrradiationRegistry', lazy=True)

    def toDict(self, fromdate=None, todate=None):
        return {
            "id": "{}:{}".format(self.classtype, self.name),
            "readings": self.getRegistries(fromdate, todate)
        }

    def insertRegistry(self, irradiation_w_m2, temperature_dc, time=None):
        return SensorIrradiationRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            irradiation_w_m2 = irradiation_w_m2,
            temperature_dc = temperature_dc
            )

    def getRegistries(self, fromdate=None, todate=None):
        readings = getRegistries(self.sensorRegistries, exclude='sensor', fromdate=fromdate, todate=todate)
        return readings

class SensorTemperatureAmbient(Sensor):

    sensorRegistries = Set('SensorTemperatureAmbientRegistry', lazy=True)

    def toDict(self, fromdate=None, todate=None):
        return {
            "id": "{}:{}".format(self.classtype, self.name),
            "readings": self.getRegistries(fromdate, todate)
        }

    def insertRegistry(self, temperature_dc, time=None):
        return SensorTemperatureAmbientRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            temperature_dc = temperature_dc
            )

    def getRegistries(self, fromdate=None, todate=None):
        readings = getRegistries(self.sensorRegistries, exclude='sensor', fromdate=fromdate, todate=todate)
        return readings

class SensorTemperatureModule(Sensor):

    sensorRegistries = Set('SensorTemperatureModuleRegistry', lazy=True)
    ambient = Optional(bool)

    def toDict(self, fromdate=None, todate=None):
        return {
            "id": "{}:{}".format(self.classtype, self.name),
            "readings": self.getRegistries(fromdate, todate)
        }

    def insertRegistry(self, temperature_dc, time=None):
        return SensorTemperatureModuleRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            temperature_dc = temperature_dc
            )

    def getRegistries(self, fromdate=None, todate=None):
        readings = getRegistries(self.sensorRegistries, exclude='sensor', fromdate=fromdate, todate=todate)
        return readings


class SensorIntegratedIrradiation(Sensor):

    sensorRegistries = Set('IntegratedIrradiationRegistry', lazy=True)

    def toDict(self, fromdate=None, todate=None):
        return {
            "id": "{}:{}".format(self.classtype, self.name),
            "readings": self.getRegistries(fromdate, todate)
        }

    def insertRegistry(self, integratedIrradiation_wh_m2, time=None):
        return IntegratedIrradiationRegistry(
            sensor = self,
            time = time or datetime.datetime.now(datetime.timezone.utc),
            integratedIrradiation_wh_m2 = integratedIrradiation_wh_m2
            )

    def getRegistries(self, fromdate=None, todate=None):
        readings = getRegistries(self.sensorRegistries, exclude='sensor', fromdate=fromdate, todate=todate)
        return readings

class SensorIrradiationRegistry(database.Entity):

    sensor = Required(SensorIrradiation)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(sensor, time)
    irradiation_w_m2 = Optional(int, size=64)
    temperature_dc = Optional(int, size=64)

class SensorTemperatureAmbientRegistry(database.Entity):

    sensor = Required(SensorTemperatureAmbient)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(sensor, time)
    temperature_dc = Optional(int, size=64)

class SensorTemperatureModuleRegistry(database.Entity):

    sensor = Required(SensorTemperatureModule)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(sensor, time)
    temperature_dc = Optional(int, size=64)

class Inclinometer(database.Entity):
    name = Required(unicode)
    plant = Required(Plant)
    description = Optional(str)
    registries = Set('InclinometerRegistry', lazy=True)


class InclinometerRegistry(database.Entity):
    inclinometer = Required(Inclinometer)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(inclinometer, time)
    angle_real_co = Optional(int, size=64)
    angle_demand_co = Optional(int, size=64)

class Anemometer(database.Entity):
    name = Required(unicode)
    plant = Required(Plant)
    description = Optional(str)
    registries = Set('AnemometerRegistry', lazy=True)

class AnemometerRegistry(database.Entity):
    anemometer = Required(Anemometer)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(anemometer, time)
    wind_mms = Optional(int, size=64)

# third-party / derived

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

    def getForecast(self, fromdate=None, todate=None):
        readings = getRegistries(self.forecasts, exclude='forecastMetadata', fromdate=fromdate, todate=todate)
        return readings


class Forecast(database.Entity):

    forecastMetadata = Required(ForecastMetadata)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(forecastMetadata, time)

    percentil10 = Optional(int)
    percentil50 = Optional(int)
    percentil90 = Optional(int)

class Omie(database.Entity):
    name = Required(unicode)
    plant = Required(Plant)
    description = Optional(str)
    registries = Set('OmieRegistry', lazy=True)

class OmieRegistry(database.Entity):
    omie = Required(Omie)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(omie, time)
    price_ce_mwh = Optional(int, size=64)

class MarketRepresentative(database.Entity):
    name = Required(unicode)
    plant = Required(Plant)
    description = Optional(str)
    registries = Set('MarketRepresentativeRegistry', lazy=True)

class MarketRepresentativeRegistry(database.Entity):
    marketRepresentative = Required(MarketRepresentative)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(marketRepresentative, time)
    billed_energy_wh = Optional(int, size=64)
    billed_market_price_ce_mwh = Optional(int, size=64)
    cost_deviation_ce = Optional(int, size=64)
    absolute_deviation_ce = Optional(int, size=64)

# Fer servir Meter instead?
class Simel(database.Entity):
    name = Required(unicode)
    plant = Required(Plant)
    description = Optional(str)
    registries = Set('SimelRegistry', lazy=True)

class SimelRegistry(database.Entity):
    simel = Required(Simel)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(simel, time)
    exported_energy_wh = Optional(int, size=64)
    imported_energy_wh = Optional(int, size=64)
    r1_varh = Optional(int, size=64)
    r2_varh = Optional(int, size=64)
    r3_varh = Optional(int, size=64)
    r4_varh = Optional(int, size=64)

# Rethink this one, device?
class Nagios(database.Entity):
    name = Required(unicode)
    plant = Required(Plant)
    description = Optional(str)
    registries = Set('NagiosRegistry', lazy=True)

class NagiosRegistry(database.Entity):
    nagios = Required(Nagios)
    time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
    PrimaryKey(nagios, time)
    status = Optional(str)