SELECT date_trunc('{{ granularitat }}', ireg.time AT TIME ZONE 'Europe/Madrid') AS "time",
       ireg.inverter_name,
       sum(ireg.energy_wh)*100 AS energy_percent
FROM view_inverter_energy_daily as ireg
LEFT JOIN inverter ON inverter.id = ireg.inverter_id
LEFT JOIN plant ON plant.id = inverter.plant
WHERE plant.name = '{{ plant }}'
  AND ireg."time" >= '{{ interval.start }}'
  AND ireg."time" <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularitat }}', ireg.time AT TIME ZONE 'Europe/Madrid'),
         plant.id,
         ireg.inverter_name;