SELECT time at time zone 'Europe/Madrid' as temps,
       plant.name as plant_name,
       classtype,
       plant.name || '-ambient' as plant_sensor,
       temperature_dc/10.0 AS temperature_c
FROM sensortemperatureambientregistry AS ireg
LEFT JOIN sensor ON sensor.id = ireg.sensor
LEFT JOIN plant ON plant.id = sensor.plant
WHERE time BETWEEN '{{ interval.start }}' AND '{{ interval.end }}'
UNION
SELECT time at time zone 'Europe/Madrid' as temps,
       plant.name as plant_name,
       classtype,
       plant.name || '-module' as plant_sensor,
       temperature_dc/10.0 AS temperature_c
FROM sensortemperaturemoduleregistry AS ireg
LEFT JOIN sensor ON sensor.id = ireg.sensor
LEFT JOIN plant ON plant.id = sensor.plant
WHERE time BETWEEN '{{ interval.start }}' AND '{{ interval.end }}'
ORDER BY temps DESC, classtype DESC