SELECT ireg.time AS time,
       sensorreg.time AS sensor_time,
       inverter.plant,
       inverter,
       ireg.power_w,
       sensorreg.irr
FROM inverterregistry ireg
LEFT JOIN inverter ON ireg.inverter = inverter.id
left join plant on plant.id = inverter.plant
INNER JOIN LATERAL
  (SELECT sreg.time AS time,
          plant,
          sreg.sensor AS sensor,
          sreg.irradiation_w_m2 AS irr
   FROM sensorirradiationregistry AS sreg
   LEFT JOIN sensor ON sreg.sensor = sensor.id
   LEFT JOIN plant ON plant.id = sensor.plant
   WHERE sreg.time BETWEEN ireg.time - interval '2 minutes' AND ireg.time + interval '2 minutes'
     AND sensor.plant = inverter.plant
     AND sreg.time >= '{{ interval.start }}'
     AND sreg.time <= '{{ interval.end }}'
   LIMIT 1) sensorreg ON sensorreg.plant = plant.id
WHERE sensorreg.irr IS NOT NULL
  AND plant.name = '{{ plant }}'
  AND ireg.time >= '{{ interval.start }}'
  AND ireg.time <= '{{ interval.end }}'
ORDER BY ireg.time DESC;