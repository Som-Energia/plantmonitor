#!/bin/bash

cat "$1" |
sed 's%sistema_contador.name%meter.name%' | 
sed 's%sistema_contador."time"%meterregistry."time"%' | 
sed 's%sistema_contador.export_energy%meterregistry.export_energy_wh%' |
sed 's%integrated_sensors.integral_irradiation_wh_m2%sensorirradiationregistry.irradiation_w_m2%' |
sed 's%sistema_inversor."time"%inverterregistry."time"%' | 
sed 's%sistema_inversor.inverter_name%inverter.name%' | 
sed 's%sistema_inversor.daily_energy_l%inverterregistry.energy_wh%' |
sed 's%sistema_inversor.location%plant.location%' |
cat

