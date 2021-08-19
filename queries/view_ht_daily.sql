SELECT reg.sensor,
       date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid') AS "time",
       count(*) AS ht,
       plant.name AS plant
FROM plant
INNER JOIN sensor ON sensor.plant = plant.id
INNER JOIN view_irradiation AS reg ON reg.sensor = sensor.id
WHERE irradiation_w_m2_h > 5
  AND sensor.classtype = 'SensorIrradiation'
GROUP BY date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid'),
         reg.sensor,
         plant.name
ORDER BY date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid');
