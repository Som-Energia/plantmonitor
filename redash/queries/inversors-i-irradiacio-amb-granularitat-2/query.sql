SELECT date_trunc('{{ granularity }}', ireg.time AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid' AS inverter_time,
        date_trunc('{{ granularity }}', sensorreg.time AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid' AS sensor_time,
        plant.name as plant_name,
        inverter.name as inverter_name,
        sum(ireg.power_w) as power_w,
        sum(sensorreg.irr) as irradiation_w_m2
FROM inverterregistry ireg
LEFT JOIN inverter ON ireg.inverter = inverter.id 
LEFT JOIN plant ON plant.id = inverter.plant
INNER JOIN LATERAL
  (SELECT sreg.time AS TIME,
          plant as splant,
          sreg.sensor AS sensor,
          sreg.irradiation_w_m2 AS irr
   FROM sensorirradiationregistry AS sreg
   LEFT JOIN sensor ON sreg.sensor = sensor.id
   LEFT JOIN plant ON plant.id = sensor.plant
   WHERE sreg.time BETWEEN ireg.time - interval '2 minutes' AND ireg.time + interval '2 minutes'
     AND sensor.plant = inverter.plant
     AND sreg.time AT TIME ZONE 'Europe/Madrid' >= '{{ interval.start }}'
     AND sreg.time AT TIME ZONE 'Europe/Madrid' <= '{{ interval.end }}'
   LIMIT 1) sensorreg ON sensorreg.splant = plant.id
WHERE sensorreg.irr IS NOT NULL
  AND plant.name='{{ plant }}'
  AND date_trunc('{{ granularity }}', ireg.time AT TIME ZONE 'Europe/Madrid') >= '{{ interval.start }}'
  AND date_trunc('{{ granularity }}', ireg.time AT TIME ZONE 'Europe/Madrid') <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', ireg.time AT TIME ZONE 'Europe/Madrid'),
         date_trunc('{{ granularity }}', sensorreg.time AT TIME ZONE 'Europe/Madrid'),
         plant_name, inverter_name
ORDER BY date_trunc('{{ granularity }}', ireg.time AT TIME ZONE 'Europe/Madrid') DESC;