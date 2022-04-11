 SELECT date_trunc('{{granularity}}', reg.time AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid' AS "time",
           reg.plant_name AS plant,
           reg.inverter_name AS inverter,
           sum(reg.energy_wh) as energy_wh
   FROM view_inverterregistry_energes AS reg
WHERE reg.plant_name = '{{ plant }}'
  AND date_trunc('{{granularity}}', time AT TIME ZONE 'Europe/Madrid') BETWEEN '{{ interval.start }}' AND '{{ interval.end }}'
GROUP BY date_trunc('{{granularity}}', reg.time AT TIME ZONE 'Europe/Madrid'),
         plant_name,
         rollup(inverter_name)
ORDER BY time desc;