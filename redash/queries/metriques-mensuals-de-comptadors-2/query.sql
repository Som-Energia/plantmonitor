select
 plant_name,
 meter_name,
 import_energy_wh,
 export_energy_wh,
 qimportada,
 qexportada,
 sqrt(qimportada*qimportada + reg.export_energy_wh*reg.export_energy_wh) as simportada, --pendent de validar amb el grafana, mensual = suma(horaria), segur?
 sqrt(qexportada*qexportada + reg.import_energy_wh*reg.import_energy_wh) as sexportada,
 qimportada / NULLIF(sqrt(qimportada*qimportada + reg.export_energy_wh*reg.export_energy_wh),0) as cos_phi_importacio,
 qexportada / NULLIF(sqrt(qexportada*qexportada + reg.import_energy_wh*reg.import_energy_wh),0) as cos_phi_exportacio
from query_37
where plant_name = {{ plant }}
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY reg.time,
    plant.name,
    reg.meter,
    rollup(meter.name)                                             
