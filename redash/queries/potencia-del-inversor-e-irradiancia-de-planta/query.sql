
select 
sensorirradiationregistry.time as time_sensor,
inverterregistry.time as time_inverter,
sensorirradiationregistry.irradiation_w_m2,
inverterregistry.power_w,
inverter.id,
plant.id,
sensor.id
FROM sensorirradiationregistry
LEFT JOIN sensor ON sensor.id = sensorirradiationregistry.sensor
LEFT JOIN plant ON plant.id = sensor.plant
LEFT JOIN inverter ON inverter.plant = plant.id
LEFT JOIN inverterregistry ON inverterregistry.time = sensorirradiationregistry.time
-- Aquest funciona perque hem fet la adquisicio de la data del paquet 
-- LEFT JOIN inverterregistry ON date_trunc('minute',inverterregistry.time) = date_trunc('minute', sensorirradiationregistry.time)
WHERE 
plant.id = 1 and inverter.id = 18 and sensor.id = 7 order by time_sensor desc limit 10;
