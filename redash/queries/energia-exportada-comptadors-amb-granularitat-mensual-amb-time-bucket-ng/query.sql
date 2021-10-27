SELECT timescaledb_experimental.time_bucket_ng('1 day', reg.time at time zone 'Europe/Madrid') AS temps,
       plant.name AS plant,
       meter.name AS meter,
       sum(reg.export_energy_wh)/1000.0 as energy_kwh
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id=reg.meter
LEFT JOIN plant ON plant.id=meter.plant
WHERE plant.name='{{ planta }}'
and timescaledb_experimental.time_bucket_ng('1 month', reg.time at time zone 'Europe/Madrid') between '{{ interval.start }}'
  AND '{{ interval.end }}'
GROUP BY temps,
         plant.name,
         meter.name order by temps desc;
         
