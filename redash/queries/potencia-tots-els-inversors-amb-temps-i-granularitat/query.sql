SELECT reg.time at time zone 'Europe/Madrid' AS temps,
       plant.name AS plant,
       inverter.name AS inverter,
       sum(power_w)/1000 as potencia_kw
FROM inverterregistry AS reg
LEFT JOIN inverter ON inverter.id=reg.inverter
LEFT JOIN plant ON plant.id=inverter.plant
WHERE plant.name='{{ planta }}'
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY reg.time at time zone 'Europe/Madrid',
         plant.name,
         rollup(inverter.name)
order by temps asc;

