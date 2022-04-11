SELECT reg.time at time zone 'Europe/Madrid' AS temps,
       plant.name AS plant,
       inverter.name as inverter,
       string.name AS string,
       sum(intensity_mA) as intensity_mA
FROM stringregistry AS reg
LEFT JOIN string on string.id = reg.string
LEFT JOIN inverter ON inverter.id=string.inverter
LEFT JOIN plant ON plant.id=inverter.plant
WHERE plant.name='{{ plant }}'
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY reg.time at time zone 'Europe/Madrid',
         plant.name,
         rollup(inverter.name, string.name)
order by temps asc;

