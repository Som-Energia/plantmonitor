SELECT sreg.time::text, sreg.irradiation_w_m2 as irr, ireg.time::text, ireg.power_w as power
FROM sensorirradiationregistry as sreg
left join sensor on sreg.sensor = sensor.id
left join 
LATERAL (
  SELECT subireg.time, inverter.plant, inverter,subireg.power_w
  FROM inverterregistry subireg
  left join inverter on subireg.inverter = inverter.id
  WHERE subireg.time between sreg.time - interval '2 minutes' and sreg.time + interval '2 minutes'
  ORDER BY subireg.time DESC
  LIMIT 1
) ireg on sensor.plant = ireg.plant
ORDER BY sreg.time;