
SELECT date_trunc('{{ granularity }}', meterregistry.time) at time zone 'Europe/Madrid' AS "time",
       meter.name AS meter,
       plant.name AS plant,
       sum(export_energy_wh)/1000000.0 AS export_energy_mwh,
       sum(integratedirradiation_wh_m2)/1000.0 AS integratedirradiation_kwh_m2,
       100*(sum(export_energy_wh / 2160000.0) / sum(NULLIF(integratedirradiation_wh_m2, 0.0) / 1000.0)) AS pr,
       view_target_energy.target_energy_kwh/1000.0 AS target_energy_mwh
FROM meterregistry
LEFT JOIN meter ON meterregistry.meter = meter.id
LEFT JOIN hourlysensorirradiationregistry ON hourlysensorirradiationregistry."time" = meterregistry."time"
LEFT JOIN sensor ON sensor.plant = meter.plant
AND hourlysensorirradiationregistry.sensor = sensor.id
LEFT JOIN plant ON meter.plant = plant.id
LEFT JOIN view_target_energy ON extract('month' from meterregistry.time at time zone 'Europe/Madrid') = extract('month' from view_target_energy.time)
WHERE plant.name = '{{ plant }}'
  and meterregistry.time >= '{{ interval.start }}'
  AND meterregistry.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', meterregistry.time),
         meter.name,
         plant.name,
         target_energy_kwh;
