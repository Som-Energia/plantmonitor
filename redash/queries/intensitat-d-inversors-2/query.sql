SELECT date_trunc('{{ granularity }}', reg.time at time zone 'Europe/Madrid') AS temps,
       plant.name AS plant,
       inverter.name as inverter,
       sum(intensity_mA)/1000.0 as intensity_A
FROM stringregistry AS reg
LEFT JOIN string on string.id = reg.string
LEFT JOIN inverter ON inverter.id=string.inverter
LEFT JOIN plant ON plant.id=inverter.plant
WHERE plant.name='{{ plant }}'
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', reg.time at time zone 'Europe/Madrid'),
         plant.name,
         inverter.name
order by temps, plant, inverter desc;

