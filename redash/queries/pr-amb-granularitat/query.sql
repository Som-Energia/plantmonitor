
SELECT date_trunc('{{ granularity }}', meterregistry.time) AS "time",
       meter.name AS meter,
       plant.name AS plant,
       sum(export_energy_wh) as export_energy_wh,
       sum(integratedirradiation_wh_m2) as integratedirradiation_wh_m2,
       100*avg((export_energy_wh / 2160.0) / (NULLIF(integratedirradiation_wh_m2, 0.0) / 1000.0)) AS pr_avg,
       100*(sum(export_energy_wh / 2160.0) / sum(NULLIF(integratedirradiation_wh_m2, 0.0) / 1000.0)) AS pr_sum
FROM meterregistry
JOIN meter ON meterregistry.meter = meter.id
JOIN hourlysensorirradiationregistry ON hourlysensorirradiationregistry."time" = meterregistry."time"
JOIN sensor ON sensor.plant = meter.plant
AND hourlysensorirradiationregistry.sensor = sensor.id
JOIN plant ON meter.plant = plant.id
WHERE meterregistry.time >= '{{ interval.start }}'
  AND meterregistry.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', meterregistry.time),
meter.name, plant.name;