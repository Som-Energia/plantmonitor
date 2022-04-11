SELECT date_trunc('{{granularity}}', reg.hour at time zone 'Europe/Madrid') AS temps,
       plant.name AS plant_name,
       num_readings,
       sum(0.977*reg.expected_energy_wh)/1000.0 AS meter_expected_energy_kwh,
       sum(meterregistry.export_energy_wh)/1000.0 AS meter_exported_energy_kwh,
       sum(0.977*reg.expected_energy_wh - meterregistry.export_energy_wh)/1000.0 as lost_energy_kwh
FROM (
    SELECT date_trunc('hour', "time") + interval '1 hour' AS hour,
           plant,
           sensor,
           1000*avg(expectedpower) as expected_energy_wh,
           count(expectedpower) as num_readings
    FROM view_expected_power
    GROUP BY hour, plant, sensor
) AS reg
LEFT JOIN sensor ON reg.sensor = sensor.id
LEFT JOIN plant ON sensor.plant = plant.id
LEFT JOIN meter ON sensor.plant = meter.plant
LEFT JOIN meterregistry ON meterregistry.meter = meter.id
AND reg.hour = meterregistry.time
WHERE plant.name = '{{ planta }}'
  AND reg.hour at time zone 'Europe/Madrid' >= '{{ interval.start }}'
  AND reg.hour at time zone 'Europe/Madrid' <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', reg.hour at time zone 'Europe/Madrid'),
         plant.name, num_readings
order by temps desc;