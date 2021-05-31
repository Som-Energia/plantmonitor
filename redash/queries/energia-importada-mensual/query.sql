SELECT sum(reg.import_energy_wh) AS import_energy_wh,
       date_trunc('month'::text, reg."time") AS "time",
       plant.name AS plant_name,
       meter.name AS "meter_name::filter"
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id = reg.meter
LEFT JOIN plant ON plant.id = meter.plant
WHERE plant.id = {{ plant }}
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY plant.name,
         reg.meter,
         meter.name,
         date_trunc('month'::text, reg.time)
ORDER BY date_trunc('month'::text, reg.time)