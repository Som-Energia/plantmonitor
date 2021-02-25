
from yamlns import namespace as ns

import datetime as dt
import sys

from conf.logging_configuration import LOGGING

import logging
import logging.config
logging.config.dictConfig(LOGGING)
logger = logging.getLogger("plantmonitor")

from meteologica.plantmonitor_db import (
    PlantmonitorDB,
    PlantmonitorDBError,
)

from pony import orm

from .models import database
from .models import (
    Plant,
    Meter,
    MeterRegistry,
    Inverter,
    InverterRegistry,
    Sensor,
    SensorIntegratedIrradiation,
    SensorIrradiation,
    SensorTemperatureAmbient,
    SensorTemperatureModule,
    SensorIrradiationRegistry,
    SensorTemperatureAmbientRegistry,
    SensorTemperatureModuleRegistry,
    IntegratedIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)

def migrateLegacySensorIrradiationTableToPony(configdbns, plantName, deviceName, excerpt=False):

    if excerpt:
        sampleSize = 10
    else:
        sampleSize = 1000

    with orm.db_session:
        plant = Plant.get(name=plantName)
        if not plant:
            plant = Plant(name=plantName, codename=plantName)

        sensor = SensorIrradiation(name=deviceName, plant=plant)

        with PlantmonitorDB(configdbns) as db:
            curr = db._client.cursor()
            curr.execute("select distinct on (time) time, irradiation_w_m2, temperature_celsius from sensors limit {};".format(sampleSize))
            while True:
                records = curr.fetchmany(sampleSize)
                if not records:
                    break

                for r in records:
                    print(r)
                    time, irradiation_w_m2, temperature_celsius = r
                    time = time.replace(tzinfo=dt.timezone.utc)
                    # TODO should we change to mw_m2 on the values to support decimals?
                    irradiation_w_m2 = round(irradiation_w_m2)
                    temperature_dc = round(temperature_celsius*10)
                    sensor.insertRegistry(irradiation_w_m2=irradiation_w_m2, temperature_dc=temperature_dc, time=time)

                if excerpt:
                    break

def migrateLegacySensorTableToPony(configdbns, plantName, tableName, deviceType, dataColumnName, deviceName, excerpt=False):

    if excerpt:
        sampleSize = 10
    else:
        sampleSize = 1000

    with orm.db_session:
        plant = Plant.get(name=plantName)
        if not plant:
            plant = Plant(name=plantName, codename=plantName)

        if deviceType == 'SensorIrradiation':
            device = SensorIrradiation(name=deviceName, plant=plant)
        elif deviceType == 'SensorTemperature':
            device = SensorTemperatureAmbient(name=deviceName, plant=plant)
        elif deviceType == 'SensorIntegratedIrradiation':
            device = SensorIntegratedIrradiation(name=deviceName, plant=plant)
        else:
            raise PlantmonitorDBError("Unknown device type {}".format(deviceType))

        with PlantmonitorDB(configdbns) as db:
            curr = db._client.cursor()
            curr.execute("select distinct on (time) time, {} from {} limit {};".format(dataColumnName, tableName, sampleSize))
            while True:
                records = curr.fetchmany(sampleSize)
                if not records:
                    break

                for r in records:
                    time, value = r
                    time = time.replace(tzinfo=dt.timezone.utc)
                    # TODO should we change to mw_m2 on the values to support decimals?
                    valueInt = round(value)
                    device.insertRegistry(valueInt, time=time)

                if excerpt:
                    break


@orm.db_session
def createPlants():
    # TODO què fem amb els records antics?
    Plant(name='Alcolea', codename='SCSOM01') # Vallehermoso
    Plant(name='Riudarenes_SM', codename='SomEnergia_Riudarenes_SM')
    Plant(name='Fontivsolar', codename='SCSOM04') # SomEnergia_Fontivsolar
    Plant(name='Riudarenes_BR', codename='SomEnergia_Riudarenes_BR')
    Plant(name='Riudarenes_ZE', codename='SomEnergia_Riudarenes_ZE')
    Plant(name='Florida', codename='SCSOM06') # SomEnergia_La_Florida
    Plant(name='Matallana', codename='SCSOM02') # Exiom


def migrateLegacyMeterToPony(db, deviceName, plantName, sampleSize=10000, excerpt=False):
    with orm.db_session:

        plant = Plant.get(name=plantName)

        curr = db._client.cursor()

        device = Meter(name=deviceName, plant=plant)

        if logging.INFO >= logger.level:
            curr.execute("select count(*) from sistema_contador where name = '{}' group by name;".format(deviceName))
            totalrecords = curr.fetchone()[0]
            logger.info("Meter {} has {} records".format(deviceName, totalrecords))

        i = 0
        curr.execute("select distinct on (time, name) * from sistema_contador where name = '{}';".format(deviceName))
        while True:
            records = curr.fetchmany(sampleSize)
            if not records:
                break

            if logging.DEBUG >= logger.level:
                i += 1
                logger.debug("Inserting {}/{} records for meter {}".format(i*sampleSize, totalrecords, deviceName))

            for r in records:
                time, name, *values = r
                time = time.replace(tzinfo=dt.timezone.utc)
                device.insertRegistry(*values, time=time)

            if excerpt:
                break


def migrateLegacyMeterTableToPony(configdbns, excerpt=False):

    # createPlants has to have been run
    with orm.db_session:
        if orm.count(p for p in Plant) == 0:
            print("Database Doesn't have any plant to migrate to. Run createPlants().")
            return

    if excerpt:
        sampleSize = 10
    else:
        sampleSize = 1000

    with PlantmonitorDB(configdbns) as db:
        curr = db._client.cursor()

        curr.execute("select name from sistema_contador group by name order by name asc;")
        deviceNames = [r[0] for r in curr.fetchall()]

    for deviceName in deviceNames:
        with PlantmonitorDB(configdbns) as db:
            # TODO normalize facility names
            facilities = [f for f, m in db.getFacilityMeter() if m == deviceName]

            with orm.db_session:
                plants = [Plant.get(codename=f) for f in facilities if Plant.get(codename=f)]

                if not plants:
                    print("Plant codename {} does not exist, assuming old record".format(facilities))
                    continue
                    #plant = Plant(name=plantName, codename=facilityName)
                elif len(plants) > 1:
                    print("[WARNING] Meter {} has several plants: {}".format(deviceName,[plant.name for plant in plants]))

                plantName = plants[0].name

            migrateLegacyMeterToPony(db, deviceName, plantName, sampleSize, excerpt)


def migrateLegacyInverterToPony(db, inverterName, plantName, sampleSize=10000, excerpt=False):

    with orm.db_session:
        plant = Plant.get(name=plantName)
        inverter = Inverter(name=inverterName, plant=plant)
        sensorTemperatureAmbient = SensorTemperatureAmbient(name=inverterName, plant=plant)
        plantName = plant.name
        if logging.INFO >= logger.level:
            curr = db._client.cursor("logging")
            curr.execute("select distinct on (location, inverter_name) count(*) from sistema_inversor where location = '{}' and inverter_name = '{}' group by location, inverter_name;".format(plantName, inverterName))
            totalrecords = curr.fetchone()[0]
            logger.info("Inverter {} has {} records".format(inverterName, totalrecords))
            curr.close()

        curr = db._client.cursor("inverter")
        i = 0
        curr.execute("select distinct on (time, location, inverter_name) * from sistema_inversor where location = '{}' and inverter_name = '{}';".format(plantName, inverterName))

        while True:
            records = curr.fetchmany(sampleSize)
            if not records:
                logger.debug("No more records for inverter".format(inverterName))
                break

            if logging.DEBUG >= logger.level:
                i += 1
                logger.debug("Inserting {}/{} records for inverter {}".format(i*sampleSize, totalrecords, inverterName))

            for r in records:
                time, inverter_name, location, HR_1, HR0_1, HR2_2, HR3_3, HR4_4, *values, probe1value, probe2value, probe3value, probe4value, temp_inv  = r
                daily_energy_h, daily_energy_l, e_total_h, e_total_l, h_total_h, h_total_l, pac_r, pac_s, pac_t, powereactive_t, powerreactive_r, powerreactive_s = values
                time = time.replace(tzinfo=dt.timezone.utc)
                power_w = int(round(pac_r + pac_s + pac_t))
                energy_wh = int(round((daily_energy_h << 16) + daily_energy_l))
                uptime_h = int(round((h_total_h << 16) + h_total_l))
                temperature_dc = int(round(temp_inv*100))
                inverter.insertRegistry(
                    power_w=power_w,
                    energy_wh=energy_wh,
                    intensity_cc_mA=None,
                    intensity_ca_mA=None,
                    voltage_cc_mV=None,
                    voltage_ca_mV=None,
                    uptime_h=uptime_h,
                    temperature_dc=temperature_dc,
                    time=time)
                sensorTemperatureAmbient.insertRegistry(temperature_dc=probe1value, time=time)

            if excerpt:
                break

        curr.close()


def migrateLegacyInverterTableToPony(configdbns, excerpt):
    if excerpt:
        sampleSize = 10
    else:
        sampleSize = 10000

    with PlantmonitorDB(configdbns) as db:
        curr = db._client.cursor()

        # fetch column names
        curr.execute('select * from sistema_inversor limit 1;')
        colNames = [c[0] for c in curr.description]

        insertSignature = [
            'time', 'inverter_name', 'location', '1HR', '1HR0', '2HR2', '3HR3',
            '4HR4', 'daily_energy_h', 'daily_energy_l', 'e_total_h', 'e_total_l',
            'h_total_h', 'h_total_l', 'pac_r', 'pac_s', 'pac_t', 'powereactive_t',
            'powerreactive_r', 'powerreactive_s', 'probe1value', 'probe2value',
            'probe3value', 'probe4value', 'temp_inv']
        assert(insertSignature == colNames)

        curr = db._client.cursor("header")
        curr.execute('select location from sistema_inversor group by location order by location asc;')
        plantNames = [r[0] for r in curr.fetchall()]
        curr.close()

    for plantName in plantNames:
        logger.info("Migrating inverters from plant {}".format(plantName))
        with orm.db_session:
            plant = Plant.get(name=plantName)
            if not plant:
                plant = Plant(name=plantName, codename=plantName)
                logger.warning("Plant {} did not exist. We creted it.".format(plantName))

        with PlantmonitorDB(configdbns) as db:
            curr = db._client.cursor("header")
            curr.execute("select inverter_name from sistema_inversor where location = '{}' group by inverter_name order by inverter_name asc;".format(plantName))
            inverterNames = [r[0] for r in curr.fetchall()]
            curr.close()

        for inverterName in inverterNames:
            with PlantmonitorDB(configdbns) as db:
                migrateLegacyInverterToPony(db, inverterName, plantName, sampleSize, excerpt)

    logger.info("Inverters migrated")


def migrateLegacyToPony(configdbns, excerpt=False, skipList=[]):
    plantName = 'Alcolea'

    if 'plants' not in skipList:
        logger.info("Create Plants")
        createPlants()

    if 'inverters' not in skipList:
        logger.info("Migrate Inverters")
        migrateLegacyInverterTableToPony(configdbns, excerpt)

    if 'meters' not in skipList:
        logger.info("Migrate Meters")
        migrateLegacyMeterTableToPony(configdbns, excerpt)

    if 'sensors' not in skipList:
        logger.info("Migrate SensorIrradiation")
        migrateLegacySensorIrradiationTableToPony(
            configdbns,
            plantName=plantName,
            deviceName='irradiation_alcolea',
            excerpt=excerpt
        )
        logger.info("Migrate SensorIntegratedIrradiation")
        migrateLegacySensorTableToPony(
            configdbns,
            plantName=plantName,
            tableName='integrated_sensors',
            deviceType='SensorIntegratedIrradiation',
            dataColumnName='integral_irradiation_wh_m2',
            deviceName='integratedIrradiation_alcolea',
            excerpt=excerpt
        )
    logger.info("Migration complete")


if __name__ == "__main__":

    configdbns = ns.load('conf/configlegacydb.yaml')
    migrateLegacyToPony(configdbns)
