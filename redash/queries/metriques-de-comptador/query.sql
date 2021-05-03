select
 date_trunc('{{ granularity }}', reg.time) AS time,
 plant.name as plant_name,
 meter.name as "meter_name::filter",
 sum(reg.import_energy_wh) AS import_energy_wh,
 sum(reg.export_energy_wh) AS export_energy_wh,
 sum(reg.r1_varh-reg.r4_varh) AS qimportada,
 sum(reg.r2_varh-reg.r3_varh) AS qexportada
from meterregistry as reg
left join meter
on meter.id = reg.meter
left join plant
on plant.id = meter.plant
where plant.id = {{ plant }}
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', reg.time),                               
    plant.name,
    reg.meter,
    rollup(meter.name)                                             
