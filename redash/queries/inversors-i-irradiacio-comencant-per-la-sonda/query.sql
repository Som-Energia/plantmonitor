SELECT sreg.time AS sensor_time,
       ireg.time AS inverter_time,
       sreg.irradiation_w_m2 AS irr,
       ireg.power_w AS power,
       inverter.name AS inverter_name
FROM sensorirradiationregistry AS sreg
LEFT JOIN sensor ON sreg.sensor = sensor.id
LEFT JOIN plant ON plant.id = sensor.plant
INNER JOIN LATERAL
  (SELECT subireg.time,
          inverter.plant,
          inverter,
          subireg.power_w
   FROM inverterregistry subireg
   LEFT JOIN inverter ON subireg.inverter = inverter.id
   WHERE subireg.time <= sreg.time + interval '2 minutes'
     AND sensor.plant = inverter.plant
     AND subireg.time >= '{{ interval.start }}'
     AND subireg.time <= '{{ interval.end }}'
   ORDER BY subireg.time DESC
   LIMIT 1) ireg ON sensor.plant = ireg.plant
LEFT JOIN inverter ON inverter.id = ireg.inverter
WHERE plant.name='{{ plant }}'
  AND sreg.time >= '{{ interval.start }}'
  AND sreg.time <= '{{ interval.end }}'
ORDER BY sreg.time DESC;