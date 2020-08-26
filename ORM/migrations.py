
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
    SensorTemperature,
    SensorIrradiationRegistry,
    SensorTemperatureRegistry,
    IntegratedIrradiationRegistry,
    ForecastMetadata,
    ForecastVariable,
    ForecastPredictor,
    Forecast,
)


def migrateLegacySensorTableToPony(db, plantName, tableName, deviceType, dataColumnName, deviceName, excerpt=False):

    if excerpt:
        sampleSize = 10
    else:
        sampleSize = 1000

    curr = db._client.cursor()

    plant = Plant.get(name=plantName)
    if not plant:
        plant = Plant(name=plantName, codename=plantName)

    if deviceType == 'SensorIrradiation':
        device = SensorIrradiation(name=deviceName, plant=plant)
    elif deviceType == 'SensorTemperature':
        device = SensorTemperature(name=deviceName, plant=plant)
    elif deviceType == 'SensorIntegratedIrradiation':
        device = SensorIntegratedIrradiation(name=deviceName, plant=plant)
    else:
        raise PlantmonitorDBError("Unknown device type {}".format(deviceType))

    curr.execute("select time, {} from {} limit {};".format(dataColumnName, tableName, sampleSize))
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


def createPlants():
    # TODO què fem amb els records antics?
    Plant(name='Alcolea', codename='SCSOM01') # Vallehermoso
    Plant(name='Riudarenes_SM', codename='SomEnergia_Riudarenes_SM')
    Plant(name='Fontivsolar I', codename='SCSOM04') # SomEnergia_Fontivsolar
    Plant(name='Riudarenes_BR', codename='SomEnergia_Riudarenes_BR')
    Plant(name='Riudarenes_ZE', codename='SomEnergia_Riudarenes_ZE')
    Plant(name='Florida', codename='SCSOM06') # SomEnergia_La_Florida
    Plant(name='Matallana', codename='SCSOM02') # Exiom


def migrateLegacyMeterTableToPony(db, excerpt=False):

    # createPlants has to have been run
    if Plant.select().count() == 0:
        print("Database Doesn't have any plant to migrate to. Run createPlants().")
        return

    if excerpt:
        sampleSize = 10
    else:
        sampleSize = 1000

    curr = db._client.cursor()

    curr.execute("select name from sistema_contador group by name order by name asc;")
    deviceNames = [r[0] for r in curr.fetchall()]
    for deviceName in deviceNames:

        # TODO normalize facility names
        facilities = [f for f, m in db.getFacilityMeter() if m == deviceName]

        plants = [Plant.get(codename=f) for f in facilities if Plant.get(codename=f)]

        if not plants:
            print("Plant codename {} does not exist, assuming old record".format(facilities))
            continue
            #plant = Plant(name=plantName, codename=facilityName)
        elif len(plants) > 1:
            print("[WARNING] Meter {} has several plants: {}".format(deviceName,[plant.name for plant in plants]))

        plant = plants[0]

        device = Meter(name=deviceName, plant=plant)
        curr.execute("select distinct on (time, name) * from sistema_contador where name = '{}';".format(deviceName))
        while True:
            records = curr.fetchmany(sampleSize)
            if not records:
                break

            for r in records:
                time, name, *values = r
                time = time.replace(tzinfo=dt.timezone.utc)
                device.insertRegistry(*values, time=time)

            if excerpt:
                break


def migrateLegacyInverterTableToPony(db, excerpt):

    if excerpt:
        sampleSize = 10
    else:
        sampleSize = 1000

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

    curr.execute('select location from sistema_inversor group by location order by location asc;')

    plantNames = [r[0] for r in curr.fetchall()]

    for plantName in plantNames:
        plant = Plant.get(name=plantName)
        if not plant:
            plant = Plant(name=plantName, codename=plantName)
        curr.execute("select inverter_name from sistema_inversor where location = '{}' group by inverter_name order by inverter_name asc;".format(plantName))
        inverterNames = [r[0] for r in curr.fetchall()]
        for inverterName in inverterNames:
            inverter = Inverter(name=inverterName, plant=plant)
            sensorIrradiation = SensorIrradiation(name=inverterName, plant=plant)
            curr.execute("select distinct on (time, location, inverter_name) * from sistema_inversor where location = '{}' and inverter_name = '{}';".format(plantName, inverterName))
            while True:
                records = curr.fetchmany(sampleSize)
                if not records:
                    break

                for r in records:
                    time, inverter_name, location, HR_1, HR0_1, HR2_2, HR3_3, HR4_4, *values, probe1value, probe2value, probe3value, probe4value, temp_inv  = r
                    time = time.replace(tzinfo=dt.timezone.utc)
                    inverter.insertRegistry(*values, temp_inv_c=temp_inv, time=time)
                    sensorIrradiation.insertRegistry(irradiation_w_m2=probe1value, time=time)

                if excerpt:
                    break


def migrateLegacyToPony(db, excerpt=False):
    plantName = 'Alcolea'
    createPlants()
    migrateLegacyInverterTableToPony(db, excerpt)
    migrateLegacyMeterTableToPony(db, excerpt)
    migrateLegacySensorTableToPony(
        db,
        plantName=plantName,
        tableName='sensors',
        deviceType='SensorIrradiation',
        dataColumnName='irradiation_w_m2',
        deviceName='irradiation_alcolea',
        excerpt=excerpt
    )
    migrateLegacySensorTableToPony(
        db,
        plantName=plantName,
        tableName='sensors',
        deviceType='SensorTemperature',
        dataColumnName='temperature_celsius',
        deviceName='thermometer_alcolea',
        excerpt=excerpt
    )
    migrateLegacySensorTableToPony(
        db,
        plantName=plantName,
        tableName='integrated_sensors',
        deviceType='SensorIntegratedIrradiation',
        dataColumnName='integral_irradiation_wh_m2',
        deviceName='integratedIrradiation_alcolea',
        excerpt=excerpt
    )


if __name__ == "__main__":

    from conf import config
    with PlantmonitorDB(config) as db:
        migrateLegacyToPony(db)
