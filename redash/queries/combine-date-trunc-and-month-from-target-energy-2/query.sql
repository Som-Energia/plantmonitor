SELECT date_trunc('{{ granularity }}', meterregistry.time) at time zone 'Europe/Madrid' AS "time",
max(view_target_energy.target_energy_kwh) AS target_energy_kwh,
max(extract('month' from view_target_energy.time)) as month_target
from meterregistry
LEFT JOIN view_target_energy ON extract('month' from meterregistry.time at time zone 'Europe/Madrid') = extract('month' from view_target_energy.time)
group by date_trunc('{{ granularity }}', meterregistry.time);
