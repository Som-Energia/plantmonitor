
SELECT date_trunc('{{ granularity }}', meterregistry.time at time zone 'Europe/Madrid') AS "time",
       meter.name AS meter,
       plant.name AS plant,
       sum(export_energy_wh)/1000000.0 AS export_energy_mwh,
       sum(irradiation_w_m2_h)/1000.0 AS irradiation_w_m2_h,
       100*(sum(export_energy_wh / plantparameters.peak_power_w::float) / sum(NULLIF(irradiation_w_m2_h, 0.0) / 1000.0)) AS pr,
       view_target_energy.target_energy_kwh/1000.0 AS target_energy_mwh
FROM meterregistry
LEFT JOIN meter ON meterregistry.meter = meter.id
LEFT JOIN view_irradiation ON view_irradiation."time" = meterregistry."time"
LEFT JOIN sensor ON sensor.plant = meter.plant
AND view_irradiation.sensor = sensor.id
LEFT JOIN plant ON meter.plant = plant.id
JOIN plantparameters ON plant.id = plantparameters.plant
LEFT JOIN view_target_energy ON extract('month' from meterregistry.time at time zone 'Europe/Madrid') = extract('month' from view_target_energy.time)
WHERE plant.name = '{{ plant }}'
  and meterregistry.time >= '{{ interval.start }}'
  AND meterregistry.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', meterregistry.time at time zone 'Europe/Madrid'),
         meter.name,
         plant.name,
         target_energy_kwh;
