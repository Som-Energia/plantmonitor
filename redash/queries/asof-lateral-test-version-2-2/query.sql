SELECT sreg.time AS sensor_time,
       sreg.irradiation_w_m2 AS irr,
       ireg.time AS inverter_time,
       ireg.power_w AS power
FROM sensorirradiationregistry AS sreg
LEFT JOIN sensor ON sreg.sensor = sensor.id
INNER JOIN LATERAL
  ( SELECT subireg.time,
           inverter.plant,
           inverter,
           subireg.power_w
   FROM inverterregistry subireg
   LEFT JOIN inverter ON subireg.inverter = inverter.id
   WHERE subireg.time <= sreg.time + interval '2 minutes'
   ORDER BY subireg.time DESC
   LIMIT 1) ireg ON sensor.plant = ireg.plant
ORDER BY sreg.time
limit 1000;