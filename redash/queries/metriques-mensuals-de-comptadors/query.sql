select
 plant.name as plant_name,
 meter.name as "meter_name::filter",
 import_energy_wh,
 export_energy_wh,
 qimportada,
 qexportada,
 sqrt(qimportada*qimportada + reg.export_energy_wh*reg.export_energy_wh)) as simportada, --pendent de validar amb el grafana, mensual = suma(horaria), segur?
 sqrt(qexportada + reg.import_energy_wh*reg.import_energy_wh)) as sexportada,
 (reg.r1_varh-reg.r4_varh) / NULLIF(sqrt((reg.r1_varh-reg.r4_varh)*(reg.r1_varh-reg.r4_varh) + reg.export_energy_wh*reg.export_energy_wh),0)) as cos_phi_importacio,
 (reg.r2_varh-reg.r3_varh) / NULLIF(sqrt((reg.r2_varh-reg.r3_varh)*(reg.r2_varh-reg.r3_varh) + reg.import_energy_wh*reg.import_energy_wh),0)) as cos_phi_exportacio
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
