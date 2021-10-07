SELECT sreg.time,
        sensor,
       sreg.irradiation_w_m2 AS irr,
       inverter,
       inverterregistry.time,
       inverterregistry.power_w AS power
FROM sensorirradiationregistry AS sreg
LEFT JOIN sensor ON sreg.sensor = sensor.id
LEFT JOIN inverter ON sensor.plant = inverter.plant
inner JOIN inverterregistry ON inverterregistry.time = sreg.time AND inverterregistry.inverter = inverter.id
ORDER BY sreg.time;