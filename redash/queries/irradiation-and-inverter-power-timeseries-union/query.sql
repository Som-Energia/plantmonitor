SELECT time as time,
       NULL as sensor,
       NULL as irr,
       inverter,
       power_w AS power
FROM inverterregistry AS ireg
LEFT JOIN inverter ON inverter.id = inverter
LEFT JOIN plant on plant.id = plant
where plant.name='{{ plant }}'
  AND ireg.time >= '{{ interval.start }}'
  AND ireg.time <= '{{ interval.end }}'
UNION 
select sreg.time as time,
       sensor as sensor,
       sreg.irradiation_w_m2 AS irr,
       NULL as inverter,
       NULL AS power
FROM sensorirradiationregistry AS sreg
LEFT JOIN sensor ON sreg.sensor = sensor.id
LEFT JOIN plant on plant.id = plant
where plant.name='{{ plant }}'
  AND sreg.time >= '{{ interval.start }}'
  AND sreg.time <= '{{ interval.end }}'
ORDER BY time desc;