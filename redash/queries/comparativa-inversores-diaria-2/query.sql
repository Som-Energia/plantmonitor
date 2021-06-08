SELECT date_trunc('{{ granularitat }}', ireg.time AT TIME ZONE 'Europe/Madrid') AS "time",
       ireg.inverter_name,
       ((ireg.energy_wh) / NULLIF((result.inverter_total), 0)) AS energy_pc
FROM view_inverter_energy_daily AS ireg
LEFT JOIN inverter ON inverter.id = ireg.inverter_id
LEFT JOIN plant ON plant.id = inverter.plant ON time =
  (SELECT date_trunc('{{ granularitat }}', ireg.time AT TIME ZONE 'Europe/Madrid') AS "time",
          sum(view_inverter_energy_daily_1.energy_wh) AS inverter_total
   FROM view_inverter_energy_daily view_inverter_energy_daily_1
   GROUP BY view_inverter_energy_daily_1.time) as result
WHERE plant.name = '{{ plant }}'
  AND ireg."time" >= '{{ interval.start }}'
  AND ireg."time" <= '{{ interval.end }}'
  AND result.time = ireg.time
GROUP BY date_trunc('{{ granularitat }}', ireg.time AT TIME ZONE 'Europe/Madrid'),
         plant.id,
         ireg.inverter_name ;
