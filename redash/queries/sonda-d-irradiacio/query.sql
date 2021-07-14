SELECT date_trunc('{{ granularity }}', coalesce(irradiationsensor.time, ambientreg.time)) AS temps,
       irradiationsensor.plant AS plant,
       avg(irradiationsensor.irradiation_w_m2) AS irradiation_w_m2,
       avg(irradiationsensor.temperature_dc)/10.0 AS irradiation_temp_c,
       avg(ambientreg.temperature_dc)/10.0 AS ambient_temp_c
FROM
  (SELECT plant.name AS plant,
          reg.irradiation_w_m2,
          reg.temperature_dc,
          reg.time
   FROM sensorirradiationregistry AS reg
   LEFT JOIN sensor ON sensor.id = reg.sensor
   LEFT JOIN plant ON sensor.plant = plant.id
   WHERE plant.name = '{{ plant }}'
     AND reg.time >= '{{ interval.start }}'
     AND reg.time <= '{{ interval.end }}'
   ORDER BY reg.time DESC) AS irradiationsensor
FULL OUTER JOIN
  (SELECT plant.name,
          ambientreg.temperature_dc,
          ambientreg.time
   FROM sensortemperatureambientregistry AS ambientreg
   LEFT JOIN sensor ON sensor.id = ambientreg.sensor
   LEFT JOIN plant ON sensor.plant = plant.id
   WHERE plant.name = '{{ plant }}'
     AND ambientreg.time >= '{{ interval.start }}'
     AND ambientreg.time <= '{{ interval.end }}'
   ORDER BY ambientreg.time DESC) AS ambientreg ON irradiationsensor.time = ambientreg.time
WHERE irradiationsensor.plant = '{{ plant }}' 
  and coalesce(irradiationsensor.time, ambientreg.time) >= '{{ interval.start }}'
  AND coalesce(irradiationsensor.time, ambientreg.time) <= '{{ interval.end }}'
GROUP BY irradiationsensor.plant,
         date_trunc('{{ granularity }}', coalesce(irradiationsensor.time, ambientreg.time))
ORDER BY temps asc;

