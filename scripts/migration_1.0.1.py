#!/usr/bin/env python3
import os
os.environ.setdefault('PLANTMONITOR_MODULE_SETTINGS', 'conf.settings.devel')
import sys
from yamlns import namespace as ns
from consolemsg import step, error
from pony import orm
from ORM.models import (
    Plant,
    PlantModuleParameters,
)


if __name__ == '__main__':

    force = '--force' in sys.argv

    from ORM.orm_util import setupDatabase

    setupDatabase(create_tables=True, timescale_tables=False, drop_tables=False)

    parametersAlcolea = dict(
        Imp = 8.27, # A, module model param
        Vmp = 30.2, # V, module model param
        temperatureCoefficientI = 0.088, # %/ºC, module model param
        temperatureCoefficientV = -0.352, # %/ºC, module model param
        irradiationSTC = 1000.0, # W/m2, module model param
        temperatureSTC = 25, # ºC, module model param
        nModules = 8640, # plant parameter
        Isc = 8.75, # A, module model param
        Voc = 37.8, # V, module model param
        degradation=97.0, # %, module model param
        correctionFactorPercent = 90., # %, plant parameter
    )

    parametersFlorida = dict(
        Imp = 9.07, # A, module model param
        Vmp = 37.5, # V, module model param
        temperatureCoefficientI = 0.05, # %/ºC, module model param
        temperatureCoefficientV = -0.31, # %/ºC, module model param
        irradiationSTC = 1000.0, # W/m2, module model param
        temperatureSTC = 25, # ºC, module model param
        nModules = 4878, # plant parameter
        degradation=97.5, # %, module model param
        Isc = 9.5, # A, module model param
        Voc = 46.1, # V, module model param
        correctionFactorPercent = 90., # %, plant parameter
    )

    def setPlantParameters(plantname, **data):
        step("Setting parameters for plant '{plantname}'")
        with orm.db_session:
            plant = Plant.get(name=plantname)
            if not plant:
                error(f"Plant '{plantname}' not found")
                return
            if PlantModuleParameters.exists(plant=plant.id):
                if not force:
                    error(f"Plant '{plant.name}' already has parameters, use --force to overwrite")
                return
                warn("Forcing removal of previous plant parameters")
                oldparams = PlantModule[plant.id]
                out(ns(oldparams.as_dict()).dump())
                oldparams.delete()
            data = ns(data)
            params = PlantModuleParameters(
                plant=plant,
                n_modules = data.nModules,
                max_power_current_ma = int(data.Imp*1000),
                max_power_voltage_mv = int(data.Vmp*1000),
                current_temperature_coefficient_mpercent_c = int(data.temperatureCoefficientI*1000),
                voltage_temperature_coefficient_mpercent_c = int(data.temperatureCoefficientV*1000),
                standard_conditions_irradiation_w_m2 = int(data.irradiationSTC),
                standard_conditions_temperature_dc = int(data.temperatureSTC*10),
                degradation_cpercent = int(data.degradation*100),
                opencircuit_voltage_mv = int(data.Voc*1000),
                shortcircuit_current_ma = int(data.Isc*1000),
                expected_power_correction_factor_cpercent = int(data.get('correctionFactorPercent', 100)*100),
            )

    setPlantParameters("Alcolea", **parametersAlcolea)
    setPlantParameters("Florida", **parametersFlorida)






