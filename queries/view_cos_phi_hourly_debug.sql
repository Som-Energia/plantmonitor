select
 "time",
 plant_id,
 meter_id,
 import_energy_wh,
 export_energy_wh,
 qimportada,
 qexportada,
 sqrt(qimportada*qimportada + export_energy_wh*export_energy_wh) as simportada, --pendent de validar amb el grafana, mensual = suma(horaria), segur?
 sqrt(qexportada*qexportada + import_energy_wh*import_energy_wh) as sexportada,
 qimportada / NULLIF(sqrt(qimportada*qimportada + export_energy_wh*export_energy_wh),0) as cos_phi_importacio,
 qexportada / NULLIF(sqrt(qexportada*qexportada + import_energy_wh*import_energy_wh),0) as cos_phi_exportacio
from view_q_hourly_debug;
