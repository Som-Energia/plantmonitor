SELECT reg.sensor,
       date_trunc('month', reg.time AT TIME ZONE 'Europe/Madrid') AS "time",
       count(*) AS ht,
       plant.name AS plant
FROM plant
INNER JOIN sensor ON sensor.plant = plant.id
INNER JOIN hourlysensorirradiationregistry AS reg ON reg.sensor = sensor.id
WHERE integratedirradiation_wh_m2 > 5
  AND sensor.classtype = 'SensorIrradiation'
GROUP BY date_trunc('month', reg.time AT TIME ZONE 'Europe/Madrid'),
         reg.sensor,
         plant.name
ORDER BY date_trunc('month', reg.time AT TIME ZONE 'Europe/Madrid');

