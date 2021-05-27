select
 date_trunc('month', reg.time at time zone 'Europe/Madrid') at time zone 'Europe/Madrid' AS time,
 plant.id as plant_id,
 meter.id as meter_id,
 sum(reg.import_energy_wh) AS import_energy_wh,
 sum(reg.export_energy_wh) AS export_energy_wh,
 sum(reg.r1_varh-reg.r4_varh) AS qimportada,
 sum(reg.r2_varh-reg.r3_varh) AS qexportada
from meterregistry as reg
left join meter
on meter.id = reg.meter
left join plant
on plant.id = meter.plant
GROUP BY date_trunc('month', reg.time at time zone 'Europe/Madrid'),
    plant.id,
    reg.meter,
    meter.id;