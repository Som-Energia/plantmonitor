#!/usr/bin/env python3

import datetime
import pytz

from yamlns import namespace as ns

from .orm_utils import str2model

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


class RegisterMixin:

    def getRegistries(self, fromdate=None, todate=None):
        if fromdate and todate:
            registries = orm.select(r for r in self.registries if fromdate <= r.time and r.time <= todate)[:]
        else:
            registries = orm.select(r for r in self.registries)[:]
        return [r.to_dict(exclude=self.deviceColumnName) for r in registries]

    def insertDeviceData(self, devicedata, packettime=None):

        return [self.insertRegistry(**{**{"time":packettime}, **r}) for r in devicedata["readings"]]


def importPlants(db, nsplants):
    with orm.db_session:
        if 'municipalities' in nsplants:
            for kmunicipality in nsplants.municipalities:
                municipality = kmunicipality['municipality']
                municipality = db.Municipality.get(ineCode=municipality.ineCode) or db.Municipality(
                    name=municipality.name,
                    ineCode=municipality.ineCode,
                    countryCode=municipality.get("countryCode"),
                    country=municipality.get("country"),
                    regionCode=municipality.get("regionCode"),
                    region=municipality.get("region"),
                    provinceCode=municipality.get("provinceCode"),
                    province=municipality.get("province"),
                )
        if 'plants' in nsplants:
            for kplantns in nsplants.plants:
                plantns = kplantns['plant']
                plantns.codename = plantns.get('codename','SomEnergia_{}'.format(plantns.name))
                plant = db.Plant.get(name=plantns.name) or db.Plant(name=plantns.name, codename=plantns.codename)
                plant.importPlant(plantns)

def exportPlants(db, skipEmpty=False):
    plantsns = ns()
    with orm.db_session:
        if db.Municipality.select().exists():
            plantsns['municipalities'] = [ns([
                    ('municipality', municipality.exportMunicipality())
                    ])
                    for municipality in db.Municipality.select()]

        plantsns['plants'] = [ns([
                    ('plant', plant.exportPlant(skipEmpty))
                ])
                for plant in db.Plant.select()]
    return plantsns

# TODO might be useful to insert data from several plants at the same time
# requires lingua franca dictionary re-nesting
# def insertPlantsData(plants_data):
#     results = [plant.insertPlantData(plant_data) for plant_name,plant_data in plants_data]

def define_models(database):

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
        solarEvents = Set('SolarEvent')
        plantParameters = Optional('PlantParameters')
        moduleParameters = Optional('PlantModuleParameters')
        plantMonthlyLegacy = Optional('PlantMonthlyLegacy')


        @classmethod
        def insertPlantsData(cls, plantsData):
            for plantData in plantsData:
                logger.debug("importing plant data for plant {}".format(plantData['plant']))
                plant = Plant.get(name=plantData['plant'])
                if not plant:
                    logger.error("Plant {} is not known. Known plants: {}".format(plantData['plant'], [p.name for p in Plant.select()]))
                else:
                    plant.insertPlantData(plantData)

        @classmethod
        def getFromMeteologica(cls, plant_code):
            return Plant.get(codename=plant_code)

        def lastForecastDownloaded(self):
            # TODO when the forecast was made doesn't represent the forecast dates,
            # we might prefer checking all forecasts and get the latest
            return orm.select(f.forecastdate for f in self.forecastMetadatas).order_by(-1).first()

        def importPlant(self, nsplant):
            plant = nsplant
            # checking before setting prevents unnecessary update to db
            if 'name' in plant and self.name != plant.name:
                self.name = plant.name
            if 'description' in plant and self.description != plant['description']:
                self.description = plant['description']
            logger.info("Importing plant {}".format(self.name))
            if 'municipality' in plant:
                m = Municipality.get(ineCode=plant['municipality'])
                if m:
                    self.municipality = Municipality.get(ineCode=plant['municipality'])
                else:
                    #TODO error or exception?
                    logger.error("Error: municipality {} not found".format(plant['municipality']))
                    raise
            if 'meters' in plant:
                devices = [
                    Meter.get(plant=self, name=meter['meter'].name)
                    or Meter(plant=self, name=meter['meter'].name)
                    for meter in plant.meters
                ]
                logger.info(devices)
            if 'inverters' in plant:
                for inverter in plant.inverters:
                    inv = Inverter.get(plant=self, name=inverter['inverter'].name) or Inverter(plant=self, name=inverter['inverter'].name)
                    for string_name in inverter.inverter.get('strings', []):
                        String.get(inverter=inv, name=string_name) or String(inverter=inv, name=string_name)
            if 'irradiationSensors' in plant:
                devices = [
                    SensorIrradiation.get(plant=self, name=sensor['irradiationSensor'].name)
                    or SensorIrradiation(plant=self, name=sensor['irradiationSensor'].name)
                    for sensor in plant.irradiationSensors
                ]
                logger.info(devices)
            if 'temperatureAmbientSensors' in plant:
                devices = [
                    SensorTemperatureAmbient.get(plant=self, name=sensor['temperatureAmbientSensor'].name)
                    or SensorTemperatureAmbient(plant=self, name=sensor['temperatureAmbientSensor'].name)
                    for sensor in plant.temperatureAmbientSensors
                ]
                logger.info(devices)
            if 'temperatureModuleSensors' in plant:
                devices = [
                    SensorTemperatureModule.get(plant=self, name=sensor['temperatureModuleSensor'].name)
                    or SensorTemperatureModule(plant=self, name=sensor['temperatureModuleSensor'].name)
                    for sensor in plant.temperatureModuleSensors
                ]
                logger.info(devices)
            if 'moduleParameters' in plant:
                self.setModuleParameters(**plant['moduleParameters'])
            if 'plantParameters' in plant:
                self.setPlantParameters(**plant['plantParameters'])
            if 'location' in plant:
                self.setLocation(**plant['location'])
            return self

        def exportPlant(self, skipEmpty=False):
            # print([meter.to_dict() for meter in Meter.select(lambda m: m.plant == self)])

            if not skipEmpty:
                plantns = ns(
                        name = self.name,
                        codename = self.codename,
                        description = self.description,
                        meters    = [ns(meter=ns(name=meter.name)) for meter in Meter.select(lambda m: m.plant == self)],
                        inverters = [ns(inverter=ns(name=inverter.name) if not inverter.strings else
                                    ns(name=inverter.name, strings=sorted([s.name for s in inverter.strings])))
                                    for inverter in Inverter.select(lambda inv: inv.plant == self)],
                        irradiationSensors = [ns(irradiationSensor=ns(name=sensor.name)) for sensor in SensorIrradiation.select(lambda inv: inv.plant == self)],
                        temperatureAmbientSensors = [ns(temperatureAmbientSensor=ns(name=sensor.name)) for sensor in SensorTemperatureAmbient.select(lambda inv: inv.plant == self)],
                        temperatureModuleSensors = [ns(temperatureModuleSensor=ns(name=sensor.name)) for sensor in SensorTemperatureModule.select(lambda inv: inv.plant == self)],
                    )
                if self.moduleParameters:
                    plantns.moduleParameters = self.moduleParameters.export()
            else:
                plantns = ns(
                        name = self.name,
                        codename = self.codename,
                        description = self.description
                )
                meters    = [ns(meter=ns(name=meter.name)) for meter in Meter.select(lambda m: m.plant == self)]
                inverters = [ns(inverter=ns(name=inverter.name) if not inverter.strings else
                            ns(name=inverter.name, strings=sorted([s.name for s in inverter.strings])))
                            for inverter in Inverter.select(lambda inv: inv.plant == self)]
                irradiationSensors = [ns(irradiationSensor=ns(name=sensor.name)) for sensor in SensorIrradiation.select(lambda inv: inv.plant == self)]
                temperatureAmbientSensors = [ns(temperatureAmbientSensor=ns(name=sensor.name)) for sensor in SensorTemperatureAmbient.select(lambda inv: inv.plant == self)]
                temperatureModuleSensors = [ns(temperatureModuleSensor=ns(name=sensor.name)) for sensor in SensorTemperatureModule.select(lambda inv: inv.plant == self)]
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
                if self.moduleParameters:
                    plantns['moduleParameters'] = self.moduleParameters.export()
                if self.plantParameters:
                    plantns['plantParameters'] = self.plantParameters.export()
                if self.location:
                    plantns['location'] = {'lat': self.location.latitude, 'long': self.location.longitude}

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

        def plantData(self, fromdate=None, todate=None, skipEmpty=False):
            data = {"plant": self.name}

            #TODO generalize this
            meterList = [{
                "id":"Meter:{}".format(m.name), "readings": m.getRegistries(fromdate, todate)
                } for m in orm.select(mc for mc in self.meters) if not skipEmpty or m.getRegistries(fromdate, todate)]

            inverterList = [{
                "id":"Inverter:{}".format(i.name), "readings": i.getRegistries(fromdate, todate)
                } for i in orm.select(ic for ic in self.inverters) if not skipEmpty or i.getRegistries(fromdate, todate)]
            # inverterList = [inverter.plantData() for inverter in self.inverters]
            sensorList = [i.toDict(fromdate, todate) for i in orm.select(ic for ic in self.sensors) if not skipEmpty or i.getRegistries(fromdate, todate)]
            forecastMetadatasList = [{
                "id":"ForecastMetadatas:{}".format(i.name), "readings": i.getRegistries(fromdate, todate)
                } for i in orm.select(ic for ic in self.forecastMetadatas) if not skipEmpty or i.getRegistries(fromdate, todate)]

            data["devices"] = inverterList + meterList + forecastMetadatasList + sensorList
            data["devices"].sort(key=lambda x : x['id'])

            return data

        def setModuleParameters(
            self,
            nominalPowerMWp,
            efficiency,
            nModules,
            degradation,
            Imp,
            Vmp,
            temperatureCoefficientI,
            temperatureCoefficientV,
            temperatureCoefficientPmax,
            irradiationSTC,
            temperatureSTC,
            Voc,
            Isc):

            values = {
                'nominal_power_wp': int(nominalPowerMWp*1000000),
                'efficency_cpercent': int(efficiency*100),
                'n_modules': nModules,
                'degradation_cpercent': int(degradation*100),
                'max_power_current_ma': int(Imp*1000),
                'max_power_voltage_mv': int(Vmp*1000),
                'current_temperature_coefficient_mpercent_c': int(temperatureCoefficientI*1000),
                'voltage_temperature_coefficient_mpercent_c': int(temperatureCoefficientV*1000),
                'max_power_temperature_coefficient_mpercent_c': int(temperatureCoefficientPmax*1000),
                'standard_conditions_irradiation_w_m2': int(irradiationSTC),
                'standard_conditions_temperature_dc': int(temperatureSTC*10),
                'opencircuit_voltage_mv': int(Voc*1000),
                'shortcircuit_current_ma': int(Isc*1000),
            }

            if self.moduleParameters:
                self.moduleParameters.set(**values)
            else:
                self.moduleParameters = PlantModuleParameters(
                    plant=self,
                    **values
                )

        def setEstimateddMonthlyEnergy(
            self,
            time,
            monthlyTargetEnergyKWh,
            monthlyHistoricEnergyKWh):
            values = {
                'time': time,
                'monthly_target_energy_kwh': monthlyTargetEnergyKWh,
                'monthly_historic_energy_kwh': monthlyHistoricEnergyKWh,
            }
            if self.plantEstimatedMonthlyEnergy:
                self.plantEstimatedMonthlyEnergy.set(**values)
            else:
                self.plantEstimatedMonthlyEnergy = PlantEstimatedMonthlyEnergy(
                    plantparameters=self,
                    **values
                )

        def setPlantParameters(
            self,
            peakPowerMWp,
            nominalPowerMW,
            connectionDate,
            targetMonthlyEnergyGWh,
            nStringsPlant=None,
            nStringsInverter=None,
            nModulesString=None,
            inverterLossPercent=None,
            meterLossPercent=None,
            historicMonthlyEnergyMWh=None,
            monthTheoricPRPercent=None,
            yearTheoricPRPercent=None):

            # yamlns gives us a Date instead of string
            connection_date = datetime.datetime.combine(connectionDate, datetime.time())
            cet = pytz.timezone('Europe/Madrid')
            connection_date = cet.localize(connection_date)

            values = {
                'peak_power_w': int(peakPowerMWp*1000000),
                'nominal_power_w': int(nominalPowerMW*1000000),
                'connection_date': connection_date,
                'target_monthly_energy_wh': int(targetMonthlyEnergyGWh*1000000000),
                'n_strings_plant': nStringsPlant and int(nStringsPlant),
                'n_strings_inverter': nStringsInverter and int(nStringsInverter),
                'n_modules_string': nModulesString and int(nModulesString*1000),
                'inverter_loss_mpercent': inverterLossPercent and int(inverterLossPercent*1000),
                'meter_loss_mpercent': meterLossPercent and int(meterLossPercent*1000),
                'historic_monthly_energy_wh': historicMonthlyEnergyMWh and int(historicMonthlyEnergyMWh/1000000),
                'month_theoric_pr_cpercent': monthTheoricPRPercent and int(monthTheoricPRPercent*100),
                'year_theoric_pr_cpercent': yearTheoricPRPercent and int(yearTheoricPRPercent*100),
            }

            if self.plantParameters:
                self.plantParameters.set(**values)
            else:
                self.plantParameters = PlantParameters(
                    plant=self,
                    **values
                )

        def setLocation(self, lat, long):
            if self.location:
                self.location.set(latitude=lat, longitude=long)
            else:
                self.location = PlantLocation(plant=self,latitude=lat,longitude=long)

        @staticmethod
        def str2device(plant, classname, devicename):
            #TODO generalize this
            #TODO distinguish between failure due to missing name and unknown class
            model = str2model(database, classname)
            if not model:
                logger.error("Model {} not found".format(classname))
                return None
            device = model.get(plant=plant, name=devicename)
            if not device:
                logger.error("Device {} {} not found".format(classname, devicename))
                return None
            return device

        def createDevice(self, classname, devicename):
            # TODO handle this better, String is in the list of supported_models,
            # but we don't do creation as a device but as part of an inverter registry

            if classname == 'String':
                logger.error("String creation is not supported")
                return None

            #TODO generalize this
            #TODO distinguish between failure due to missing name and unknown class
            Model = str2model(database, classname)

            if not Model:
                logger.error("Model {} not found".format(classname))
                return None

            logger.debug("Create device from model {}".format(classname))

            return Model(plant=self, name=devicename)

        def insertDeviceData(self, devicedata, packettime=None):
            devicetype, devicename = devicedata["id"].split(":")
            device = self.str2device(plant=self, classname=devicetype, devicename=devicename)

            if not device:
                logger.warning("New device {}:{}".format(devicetype, devicename))
                logger.warning("Creating {} named {} for {}".format(devicetype, devicename, self.name))
                device = self.createDevice(classname=devicetype, devicename=devicename)

            if not device:
                logger.warning("Unknown device type {}".format(devicetype))
                return None

            return device.insertDeviceData(devicedata, packettime)

        def insertPlantData(self, plantdata):
            if self.name != plantdata["plant"]:
                return False
            packettime = plantdata.get("time")
            return [self.insertDeviceData(d, packettime) for d in plantdata["devices"]]

        def getMeter(self):
            # TODO a plant has more than one meter, how do we know which one is active?
            return orm.select(m for m in self.meters).order_by(lambda: orm.desc(m.id)).first()

    class PlantLocation(database.Entity):
        plant = Required(Plant)
        latitude = Required(float)
        longitude = Required(float)

        def getLatLong(self):
            return (self.latitude, self.longitude)

    class Meter(RegisterMixin, database.Entity):

        plant = Required(Plant)
        name = Required(unicode)
        connection_protocol = Required(str, sql_default='\'ip\'')
        registries = Set('MeterRegistry', lazy=True)
        deviceColumnName = 'meter'

        def insertRegistry(self, export_energy_wh, import_energy_wh, r1_VArh, r2_VArh, r3_VArh, r4_VArh, time=None):
            logger.debug("inserting {} into {} ".format(time, self.name))
            r = MeterRegistry(
                meter = self,
                time = time or datetime.datetime.now(datetime.timezone.utc),
                export_energy_wh = export_energy_wh,
                import_energy_wh = import_energy_wh,
                r1_VArh = r1_VArh,
                r2_VArh = r2_VArh,
                r3_VArh = r3_VArh,
                r4_VArh = r4_VArh,
                )

        def getLastReadingDate(self):
            newestRegistry = self.registries.select().order_by(orm.desc(MeterRegistry.time)).first()
            return newestRegistry.time if newestRegistry else None

        @staticmethod
        def updateMeterProtocol(protocolsByCounterName):
            for meter in Meter.select():
                if meter.name not in protocolsByCounterName:
                    continue
                meter.connection_protocol = protocolsByCounterName[meter.name]

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

    class Inverter(RegisterMixin, database.Entity):

        name = Required(unicode)
        plant = Required(Plant)
        brand = Optional(str, nullable=True)
        model = Optional(str, nullable=True)
        nominal_power_w = Optional(int)
        registries = Set('InverterRegistry', lazy=True)
        strings = Set('String')
        deviceColumnName = 'inverter'


        # inverter Strings are created via modmap the first time
        # and via parsing in registry insert

        # def __init__(self, plant, name):
        #     super().__init__(plant=plant, name=name)

            # if classname == "String":
            #     invertername, stringname = devicename.split('-')
            #     inverter = Inverter.get(name=invertername)
            #     if not inverter:
            #         logger.warning("Unknown inverter {} for string {}".format(invertername, stringname))
            #         return None
            #     return String(inverter=inverter, name=stringname)


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

        # [{
        #     'energy_wh': 100,
        #     'intensity_ca_mA': 1,
        #     'intensity_cc_mA': 1,
        #     'power_w': 100,
        #     'temperature_dc': 1,
        #     'time': time,
        #     'uptime_h': 1,
        #     'voltage_ca_mV': 1,
        #     'voltage_cc_mV': 1,
        #     'String_intensity_mA:string1': 100,
        #     'String_intensity_mA:string2': 200
        # }]

        def insertDeviceData(self, devicedata, packettime=None):

            readings = devicedata['readings']

            regs = []
            for rg in readings:
                rg.setdefault('time', packettime)
                # TODO this modifies the entry dictionary, do we want to?
                # string_readings = {k:rg.pop(k) for k in list(rg) if k.startswith('String')}
                string_readings = {k:d for k,d in rg.items() if k.startswith('String')}
                inverter_readings = {k:d for k,d in rg.items() if not k.startswith('String')}
                inv_reg = self.insertRegistry(**inverter_readings)

                reg = [inv_reg]

                for sr, value in string_readings.items():
                    #TODO polymorfize this with sth like String.insertDeviceData(sr)
                    devicetype, stringname, magnitude = sr.split(":")
                    string = String.get(inverter=self, name=stringname)

                    if not string:
                        logger.warning("New device {}:{}".format(devicetype, stringname))
                        logger.warning("Creating {} named {} for {}".format(devicetype, stringname, self.name))
                        string = String(inverter=self, name=stringname)

                    str_reg = string.insertRegistry(time=rg['time'], intensity_mA=value)

                    reg.append(str_reg)

                regs.append(reg)

            return regs


        def plantData(self, fromdate=None, todate=None, skipEmpty=False):

            # TODO add strings' registries in inverter registries

            # select * from stringregistry where time = inverterregistry.time and stringregistry.string in self.inverter.strings

            # [sr for sr in StringRegistry.select()]

            # StringRegistry.select())

            # [{**ir,**sr} for sr in StringRegistry.select() if sr.time = ir.time and sr.string in self.strings for ir in InverterRegistry.select()]

            # for r in self.registries:
            #     for s in self.strings:
            #         r.time == s.registries.time
            #         {**r, "String_{}:{}".format(s)}
            # self.strings

            inverterPlantData = {
                "id":"Inverter:{}".format(self.name),
                "readings": self.getRegistries(fromdate, todate),
            }

            return inverterPlantData

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

    class String(RegisterMixin, database.Entity):

        inverter = Required(Inverter)
        name = Required(str)
        registries = Set('StringRegistry', lazy=True)
        deviceColumnName = 'string'
        # used as displayname, might be CCP box name + plug slot or string name
        stringbox_name = Optional(str, nullable=True)

        def insertRegistry(self,
            intensity_mA,
            time=None,
            ):
            return StringRegistry(
                string=self,
                time=time or datetime.datetime.now(datetime.timezone.utc),
                intensity_mA=intensity_mA,
            )

    class StringRegistry(database.Entity):

        string = Required(String)
        time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
        PrimaryKey(string, time)
        intensity_mA = Optional(int, size=64)

    class Sensor(RegisterMixin, database.Entity):

        name = Required(unicode)
        plant = Required(Plant)
        description = Optional(str)
        deviceColumnName = 'sensor'


    class SensorIrradiation(Sensor):

        registries = Set('SensorIrradiationRegistry', lazy=True)
        hourlySensorIrradiationRegistries = Set('HourlySensorIrradiationRegistry', lazy=True)

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

        # TODO should we allow None as time and use now() to maintain insertRegistry argument order?
        def insertHourlySensorIrradiationMetric(self, time, integratedIrradiation_wh_m2=None, expected_energy_wh=None):
            return HourlySensorIrradiationRegistry(
                sensor = self,
                time = time,
                integratedIrradiation_wh_m2=integratedIrradiation_wh_m2,
                expected_energy_wh=expected_energy_wh
            )

    class SensorTemperatureAmbient(Sensor):

        registries = Set('SensorTemperatureAmbientRegistry', lazy=True)

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

    class SensorTemperatureModule(Sensor):

        registries = Set('SensorTemperatureModuleRegistry', lazy=True)
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

    class HourlySensorIrradiationRegistry(database.Entity):

        sensor = Required(SensorIrradiation)
        time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
        PrimaryKey(sensor, time)
        integratedIrradiation_wh_m2 = Optional(int, size=64)
        expected_energy_wh = Optional(int, size=64)


    class ForecastVariable(database.Entity):

        name = Required(unicode, unique=True)
        forecastMetadatas = Set('ForecastMetadata', lazy=True)


    class ForecastPredictor(database.Entity):

        name = Required(unicode, unique=True)
        forecastMetadatas = Set('ForecastMetadata')


    class ForecastMetadata(RegisterMixin, database.Entity):

        errorcode = Optional(str)
        plant     = Required(Plant)
        variable  = Optional(ForecastVariable)
        predictor = Optional(ForecastPredictor)
        forecastdate = Optional(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
        granularity = Optional(int)
        registries = Set('Forecast', lazy=True)
        deviceColumnName = 'forecastmetadata'

        @classmethod
        def create(cls, plant, forecastdate, errorcode, variable='prod', predictor='aggregated', granularity='60'):
            if errorcode != 'OK':
                return ForecastMetadata(
                    plant=plant,
                    errorcode=errorcode,
                    forecastdate=forecastdate,
                )

            forecastPredictor = ForecastPredictor.get(name=predictor) or ForecastPredictor(name=predictor)
            forecastVariable = ForecastVariable.get(name=variable) or ForecastVariable(name=variable)

            return ForecastMetadata(
                plant=plant,
                predictor=forecastPredictor,
                variable=forecastVariable,
                forecastdate=forecastdate,
                granularity=granularity,
                errorcode=errorcode
            )

        def addForecasts(self, meterDataForecast):
            for forecast in meterDataForecast:
                time, p50 = forecast
                self.insertForecast(percentil10=None, percentil50=p50, percentil90=None, time=time)

        def insertForecast(self, percentil10, percentil50, percentil90, time=None):
            return Forecast(
                forecastMetadata = self,
                time = time or datetime.datetime.now(datetime.timezone.utc),
                percentil10 = percentil10,
                percentil50 = percentil50,
                percentil90 = percentil90
                )

        # TODO remove and call directly getRegistries
        def getForecast(self, fromdate=None, todate=None):
            return self.getRegistries(fromdate=fromdate, todate=todate)


    class Forecast(database.Entity):

        forecastMetadata = Required(ForecastMetadata)
        time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
        PrimaryKey(forecastMetadata, time)

        # TODO remove percentil10 and 90 since we won't use them
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

    class PlantMonthlyLegacy(database.Entity):
        """
        Data imported from historical drive spreadshet
        """
        plant = Required(Plant)
        time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
        export_energy_wh = Required(int, size=64)

    class PlantParameters(database.Entity):
        plant = Required(Plant)
        plant_estimated_monthly_energy = Set('PlantEstimatedMonthlyEnergy')
        peak_power_w = Required(int, size=64)
        nominal_power_w = Required(int, size=64)
        connection_date = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
        n_strings_plant = Optional(int)
        n_strings_inverter = Optional(int) # TODO poden tenir diferent nombre d'strings els inversors d'una planta?
        n_modules_string = Optional(int)
        inverter_loss_mpercent = Optional(int) # TODO és fixe a la planta o canvia amb l'inversor?
        meter_loss_mpercent = Optional(int) # TODO és fixe a la planta o canvia amb el comptador?
        target_monthly_energy_wh = Required(int, size=64)
        historic_monthly_energy_wh = Optional(int, size=64)
        month_theoric_pr_cpercent = Optional(int, size=64)
        year_theoric_pr_cpercent = Optional(int, size=64)

        def export(self):
            pp = {
                'peakPowerMWp': round(self.peak_power_w/1000000.0, 3),
                'nominalPowerMW': round(self.nominal_power_w/1000000.0, 3),
                'connectionDate': self.connection_date.date(),
                'targetMonthlyEnergyGWh': round(self.target_monthly_energy_wh/1000000000.0, 3),
                'nStringsPlant': self.n_strings_plant and int(self.n_strings_plant),
                'nStringsInverter': self.n_strings_inverter and int(self.n_strings_inverter),
                'nModulesString': self.n_modules_string and int(self.n_modules_string),
                'inverterLossPercent': self.inverter_loss_mpercent and self.inverter_loss_mpercent/1000.0,
                'meterLossPercent': self.meter_loss_mpercent and self.meter_loss_mpercent/1000.0,
                'historicMonthlyEnergyMWh': self.historic_monthly_energy_wh and round(self.historic_monthly_energy_wh*1000000.0, 3),
                'monthTheoricPRPercent': self.month_theoric_pr_cpercent and self.month_theoric_pr_cpercent*100,
                'yearTheoricPRPercent': self.year_theoric_pr_cpercent and self.year_theoric_pr_cpercent*100,
            }

            pp = {k:v for k,v in pp.items() if v is not None}
            return pp

    class PlantEstimatedMonthlyEnergy(database.Entity):
        plantparameters = Required(PlantParameters)
        time = Required(datetime.datetime, sql_type='TIMESTAMP WITH TIME ZONE', default=datetime.datetime.now(datetime.timezone.utc))
        monthly_target_energy_kwh = Required(int, size=64)
        monthly_historic_energy_kwh = Required(int, size=64)

        def export(self):
            pp = {
                'time': self.time.date(),
                'monthly_target_energy_kwh': self.monthly_target_energy_kwh,
                'monthly_historic_energy_kwh': self.monthly_historic_energy_kwh,
            }
            pp = {k:v for k,v in pp.items() if v is not None}
            return pp

    class PlantModuleParameters(database.Entity):
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
        expected_power_correction_factor_cpercent = Required(int, sql_default='10000') # % losses up to inverter

        def export(self):
            mp = {
                'nModules': self.n_modules,
                'degradation': self.degradation_cpercent,
                'Imp': self.max_power_current_ma,
                'Vmp': self.max_power_voltage_mv,
                'temperatureCoefficientI': self.current_temperature_coefficient_mpercent_c,
                'temperatureCoefficientV': self.voltage_temperature_coefficient_mpercent_c,
                'irradiationSTC': self.standard_conditions_irradiation_w_m2,
                'temperatureSTC': self.standard_conditions_temperature_dc,
                'Voc': self.opencircuit_voltage_mv,
                'Isc': self.shortcircuit_current_ma,
            }
            return mp


    class SolarEvent(database.Entity):

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
                    SolarEvent(plant=p, sunrise=sunrise, sunset=sunset)

        @classmethod
        def insertPlantSolarEvents(cls, plant, solarevents):
            for se in solarevents:
                sunrise, sunset = se
                SolarEvent(plant=plant, sunrise=sunrise, sunset=sunset)


# vim: et sw=4 ts=4
