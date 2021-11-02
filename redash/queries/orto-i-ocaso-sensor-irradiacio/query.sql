SELECT
       date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid') as time,
       reg.sensor,
       MAX(reg.time AT TIME ZONE 'Europe/Madrid') as sunset,
       MIN(reg.time AT TIME ZONE 'Europe/Madrid') as sunsrise
FROM sensorirradiationregistry AS reg
LEFT JOIN sensor ON sensor.id = reg.sensor
LEFT JOIN plant ON sensor.plant = plant.id
WHERE TIME >= '{{ interval.start }}'
  AND TIME <= '{{ interval.end }}'
  AND reg.irradiation_w_m2 > 10 
GROUP by date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid'),
reg.sensor
ORDER BY time DESC