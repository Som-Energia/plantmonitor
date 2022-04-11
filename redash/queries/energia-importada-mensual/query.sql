SELECT date_trunc('month', (reg.time - interval '1 hour') at time zone 'Europe/Madrid') AS "time",
       sum(reg.import_energy_wh) AS import_energy_wh,
       plant.name AS plant_name,
       meter.name AS "meter_name::filter"
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id = reg.meter
LEFT JOIN plant ON plant.id = meter.plant
WHERE plant.name ='{{ planta }}'
  AND (reg.time - interval '1 hour') at time zone 'Europe/Madrid' between '{{ interval.start }}' and '{{ interval.end }}'
GROUP BY plant.name,
         reg.meter,
         meter.name,
         date_trunc('month', (reg.time - interval '1 hour') at time zone 'Europe/Madrid')
ORDER BY date_trunc('month', (reg.time - interval '1 hour') at time zone 'Europe/Madrid')