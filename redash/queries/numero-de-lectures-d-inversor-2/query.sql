SELECT date_trunc('{{ granularity }}', reg.time) AS temps,
       plant.name AS plant,
       inverter.name AS inverter,
       count(*)
FROM inverterregistry AS reg
LEFT JOIN inverter ON inverter.id=reg.inverter
LEFT JOIN plant ON plant.id=inverter.plant
WHERE plant.name='{{ planta }}'
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', reg.time),
         plant.name,
         inverter.name;