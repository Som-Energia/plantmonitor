SELECT sreg.time::text, sreg.irradiation_w_m2 as irr, inverterregistry.time::text, inverterregistry.power_w as power
FROM sensorirradiationregistry as sreg
left join sensor on sreg.sensor = sensor.id
left join inverterregistry on inverterregistry.time between sreg.time - interval '5 minutes' and sreg.time + interval '5 minutes'
left join inverter on sensor.plant = inverter.plant and inverterregistry.inverter = inverter.id
ORDER BY sreg.time;