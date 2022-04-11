
SELECT date_trunc('{{granularity}}', sub.time AT TIME ZONE 'Europe/Madrid') AS temps,
       sub.plant AS plant,
       sub.inverter AS inverter,
       sum(sub.energy_wh)/1000 AS energy_kwh
FROM
  ( SELECT date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid' AS "time",
           reg.plant_name AS plant,
           reg.inverter_name AS inverter,
           max(reg.energy_wh) as energy_wh
   FROM view_inverterregistry_energes AS reg
   group by  date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid'), reg.plant_name, reg.inverter_name) sub
WHERE sub.plant = 'Alcolea'
  AND date_trunc('{{granularity}}', sub.time AT TIME ZONE 'Europe/Madrid') BETWEEN '{{ interval.start }}' AND '{{ interval.end }}'
GROUP BY temps,
         sub.plant,
         rollup(sub.inverter)
ORDER BY temps;