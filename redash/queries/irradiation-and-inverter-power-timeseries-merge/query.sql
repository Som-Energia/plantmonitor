SELECT sreg.time as sensor_time,
       sensor,
       sreg.irradiation_w_m2 AS irr,
       inverter,
       inverterregistry.time as inverter_time,
       inverterregistry.power_w AS power
FROM sensorirradiationregistry AS sreg
LEFT JOIN sensor ON sreg.sensor = sensor.id
LEFT JOIN inverter ON sensor.plant = inverter.plant
inner JOIN inverterregistry ON inverterregistry.inverter = inverter.id and inverterregistry.time = sreg.time
ORDER BY sreg.time desc
limit 1000;