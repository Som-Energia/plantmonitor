select
"time" at time zone 'Europe/Madrid',
 plant.name as plant,
 meter.name as meter,
 export_energy_wh,
 import_energy_wh,
 qimportada,
 qexportada,
 simportada,
 sexportada,
 cos_phi_importacio,
 cos_phi_exportacio
from view_cos_phi_monthly
join meter on meter_id = meter.id
join plant on plant_id = plant.id
where plant_id = {{ plant }}
  AND "time" at time zone 'Europe/Madrid' >= '{{ interval.start }}'
  AND "time" at time zone 'Europe/Madrid' <= '{{ interval.end }}';
