SELECT date_trunc('{{ granularity }}', reg.time at time zone 'Europe/Madrid') AS temps,
       plant.name AS plant,
       sum(CASE WHEN inverter.name = 'inversor1' THEN intensity_mA END) AS "inversor1",
       sum(CASE WHEN inverter.name = 'inversor2' THEN intensity_mA END) AS "inversor2",
       sum(CASE WHEN inverter.name = 'inversor3' THEN intensity_mA END) AS "inversor3",
       sum(CASE WHEN inverter.name = 'inversor4' THEN intensity_mA END) AS "inversor4"
FROM stringregistry AS reg
LEFT JOIN string on string.id = reg.string
LEFT JOIN inverter ON inverter.id = string.inverter
LEFT JOIN plant ON plant.id = inverter.plant
WHERE plant.name = '{{ plant }}'
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', reg.time at time zone 'Europe/Madrid'),
         plant.name
order by temps, plant desc;

