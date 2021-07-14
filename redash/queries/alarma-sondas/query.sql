SELECT plant_name,
       sensor_name || ' ' || classtype AS device_name,
       alarm,
       time
FROM
  (SELECT CASE
              WHEN now() - max(reg.time) > interval '24 hours' THEN 'Error: Sense lectures des de ' || now() - max(reg.time)
              ELSE 'OK'
          END AS alarm,
          max(reg.time) at time zone 'Europe/Madrid' AS time,
          sensor.classtype AS classtype,
          sensor.name AS sensor_name,
          plant.name AS plant_name
   FROM sensorirradiationregistry AS reg
   LEFT JOIN sensor ON sensor.id=reg.sensor
   LEFT JOIN plant ON plant.id=sensor.plant
   GROUP BY plant.name,
            sensor.classtype,
            sensor.name
   UNION SELECT CASE
                    WHEN now() - max(reg.time) > interval '24 hours' THEN 'Error: Sense lectures des de ' || now() - max(reg.time)
                    ELSE 'OK'
                END AS alarm,
                max(reg.time) AS TIME,
                sensor.classtype AS classtype,
                sensor.name AS sensor_name,
                plant.name AS plant_name
   FROM sensortemperatureambientregistry AS reg
   LEFT JOIN sensor ON sensor.id=reg.sensor
   LEFT JOIN plant ON plant.id=sensor.plant
   GROUP BY plant.name,
            sensor.classtype,
            sensor.name
   UNION SELECT CASE
                    WHEN now() - max(reg.time) > interval '24 hours' THEN 'Error: Sense lectures des de ' || now() - max(reg.time)
                    ELSE 'OK'
                END AS alarm,
                max(reg.time) AS TIME,
                sensor.classtype AS classtype,
                sensor.name AS sensor_name,
                plant.name AS plant_name
   FROM sensortemperaturemoduleregistry AS reg
   LEFT JOIN sensor ON sensor.id=reg.sensor
   LEFT JOIN plant ON plant.id=sensor.plant
   GROUP BY plant.name,
            sensor.classtype,
            sensor.name) AS sensor
-- Filter for old inverter data
WHERE time >= current_date - interval '100 days'
ORDER BY sensor_name