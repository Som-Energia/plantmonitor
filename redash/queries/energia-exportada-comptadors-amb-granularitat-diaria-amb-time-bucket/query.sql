SELECT time_bucket('1 day', reg.time at time zone 'Europe/Madrid', '04-01-2021 01:00'::timestamptz) at time zone 'Europe/Madrid' AS temps,
       --reg.time at time zone 'Europe/Madrid' as timehour,
       plant.name AS plant,
       meter.name AS meter,
       sum(reg.export_energy_wh)/1000.0 as energy_kwh
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id=reg.meter
LEFT JOIN plant ON plant.id=meter.plant
WHERE plant.name='{{ planta }}'
and time_bucket('1 day', reg.time at time zone 'Europe/Madrid', '04-01-2021 01:00'::timestamptz) between '{{ interval.start }}'
  AND '{{ interval.end }}'
  group by temps, plant.name, meter.name
order by temps desc;
         
-- time_bucket no suporta timezone, origin és al dilluns 04-01-2021