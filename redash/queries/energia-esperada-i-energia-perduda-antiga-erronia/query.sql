SELECT date_trunc('{{granularity}}', reg.time at time zone 'Europe/Madrid') AS temps,
       plant.name AS plant_name,
       sum(0.97*reg.expected_energy_wh)/1000.0 AS meter_expected_energy_kwh,
       sum(meterregistry.export_energy_wh)/1000.0 AS meter_exported_energy_kwh,
       sum(0.97*reg.expected_energy_wh - meterregistry.export_energy_wh)/1000.0 as lost_energy_kwh
FROM hourlysensorirradiationregistry AS reg
LEFT JOIN sensor ON reg.sensor = sensor.id
LEFT JOIN plant ON sensor.plant = plant.id
LEFT JOIN meter ON sensor.plant = meter.plant
LEFT JOIN meterregistry ON meterregistry.meter = meter.id
AND reg.time = meterregistry.time
WHERE plant.name = '{{ planta }}'
  AND reg.time at time zone 'Europe/Madrid' >= '{{ interval.start }}'
  AND reg.time at time zone 'Europe/Madrid' <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', reg.time at time zone 'Europe/Madrid'),
         plant.name
order by temps;