select
 reg.time AS time,
 plant.id as plant_id,
 meter.id as meter_id,
 reg.import_energy_wh AS import_energy_wh,
 reg.export_energy_wh AS export_energy_wh,
 reg.r1_varh-reg.r4_varh AS qimportada,
 reg.r2_varh-reg.r3_varh AS qexportada
from meterregistry as reg
left join meter
on meter.id = reg.meter
left join plant
on plant.id = meter.plant;