SELECT date_trunc('month', reg.time) AS "time",
       ((sum(export_energy_wh)- 221.730*1000*1000)/(221.730*1000*1000))*100 AS prod_resp_obj,
       sum(export_energy_wh),
       meter.name
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id = reg.meter
LEFT JOIN plant ON plant.id = meter.plant
WHERE plant.id = {{ plant }}
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY date_trunc('month', reg.time),
         plant.name,
         reg.meter,
         meter.name
ORDER BY TIME DESC;