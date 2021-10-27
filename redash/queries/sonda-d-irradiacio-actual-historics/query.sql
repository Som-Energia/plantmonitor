SELECT coalesce(irradiationsensor.time, ambientreg.time) AT TIME ZONE 'Europe/Madrid' AS temps,
                                                                      irradiationsensor.irradiation_w_m2 AS irradiation_w_m2,
                                                                      irradiationsensor.temperature_dc/10.0 AS irradiation_temp_c,
                                                                      ambientreg.temperature_dc/10.0 AS temperature_modul_c
FROM
  (SELECT plant.name,
          reg.irradiation_w_m2,
          reg.temperature_dc,
          reg.time
   FROM sensorirradiationregistry AS reg
   LEFT JOIN sensor ON sensor.id = reg.sensor
   LEFT JOIN plant ON sensor.plant = plant.id
   WHERE TIME >= '{{ interval.start }}'
     AND TIME <= '{{ interval.end }}'
   ORDER BY reg.time DESC ) AS irradiationsensor
FULL OUTER JOIN
  (SELECT plant.name,
          ambientreg.temperature_dc,
          ambientreg.time
   FROM sensortemperatureambientregistry AS ambientreg
   LEFT JOIN sensor ON sensor.id = ambientreg.sensor
   LEFT JOIN plant ON sensor.plant = plant.id
   WHERE TIME >= '{{ interval.start }}'
     AND TIME <= '{{ interval.end }}'
   ORDER BY ambientreg.time DESC) AS ambientreg ON irradiationsensor.time = ambientreg.time
WHERE coalesce(irradiationsensor.time, ambientreg.time) AT TIME ZONE 'Europe/Madrid' >= '{{ interval.start }}'
  AND coalesce(irradiationsensor.time, ambientreg.time) AT TIME ZONE 'Europe/Madrid' <= '{{ interval.end }}'
ORDER BY temps DESC