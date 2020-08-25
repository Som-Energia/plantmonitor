
from yamlns import namespace as ns

import datetime
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


def migrateLegacyInverterTableToPony(db, numrecords=0):

    remainingRecords = numrecords

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

    curr.execute('select location from sistema_inversor group by location order by location asc limit 100;')

    plantNames = [r[0] for r in curr.fetchall()]

    for plantName in plantNames:
        plant = Plant(name=plantName, codename=plantName)
        curr.execute("select inverter_name from sistema_inversor where location = '{}' group by inverter_name order by inverter_name asc limit 100;".format(plantName))
        inverterNames = [r[0] for r in curr.fetchall()]
        for inverterName in inverterNames:
            inverter = Inverter(name=inverterName, plant=plant)
            curr.execute("select * from sistema_inversor where location = '{}' and inverter_name = '{}';".format(plantName, inverterName))
            while remainingRecords > 0:
                records = curr.fetchmany(min(remainingRecords, 1000))
                if not records:
                    break
                remainingRecords -= len(records)

                for r in records:
                    time, inverter_name, location, HR_1, HR0_1, HR2_2, HR3_3, HR4_4, *values, probe1value, probe2value, probe3value, probe4value, temp_inv  = r
                    inverter.insertRegistry(*values, temp_inv_c=temp_inv, time=time)
                    # TODO create Sensor and insert values from probeNvalue


def migrateLegacyToPony(configdb):
    with PlantmonitorDB(configdb) as db:
        migrateLegacyInverterTableToPony(db)


def main():

    from conf import config

    migrateLegacyToPony(config)
