SELECT coalesce(irradiationsensor.time, ambientreg.time) as temps,
       irradiationsensor.irradiation_w_m2 as irradiation_w_m2,
       irradiationsensor.temperature_dc/10.0 as irradiation_temp_c,
       ambientreg.temperature_dc/10.0 as temperature_modul_c
FROM
  (SELECT plant.name,
       reg.irradiation_w_m2,
       reg.temperature_dc,
       reg.time
FROM sensorirradiationregistry AS reg
LEFT JOIN sensor ON sensor.id = reg.sensor
LEFT JOIN plant ON sensor.plant = plant.id  order by reg.time desc limit 1
) as irradiationsensor
FULL OUTER JOIN
  (
SELECT plant.name,
       ambientreg.temperature_dc,
       ambientreg.time
FROM sensortemperatureambientregistry AS ambientreg
LEFT JOIN sensor ON sensor.id = ambientreg.sensor
LEFT JOIN plant ON sensor.plant = plant.id order by ambientreg.time desc limit 1
) AS ambientreg ON irradiationsensor.time = ambientreg.time order by temps desc limit 1;