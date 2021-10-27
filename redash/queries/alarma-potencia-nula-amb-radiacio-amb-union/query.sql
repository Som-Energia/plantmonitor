SELECT max(inverter_time) at time zone 'Europe/Madrid' as inverter_time,
max(sensor_time) at time zone 'Europe/Madrid' as sensor_time,
plant.name as plant_name,
--inverter.name as inverter_name,
max(power_w) as power,
sum(irradiation_w_m2) as irradiation,
(sum(irradiation_w_m2) > 100 AND sum(power_w) = 0)::integer AS inverter_fail
FROM
  ( SELECT max(ireg.time) AS inverter_time,
           NULL AS sensor_time,
           inverter.plant AS plant,
		   NULL as sensor,
           inverter,
           max(ireg.power_w) AS power_w,
           NULL AS irradiation_w_m2
   FROM inverterregistry ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE now() - interval '1h' <= ireg.time
   GROUP BY inverter,
            inverter.plant
   UNION SELECT NULL AS inverter_time,
           		max(sreg.time) AS sensor_time,
                sensor.plant AS plant,
                NULL AS inverter,
                sreg.sensor,
                NULL AS power_w,
                avg(sreg.irradiation_w_m2) AS irradiation_w_m2
   FROM sensorirradiationregistry sreg
   LEFT JOIN sensor ON sensor.id = sreg.sensor
   LEFT JOIN plant ON plant.id = sensor.plant
   WHERE now() - interval '1h' <= sreg.time
   GROUP BY sreg.sensor,
            sensor.plant
   ORDER BY inverter_time DESC, sensor,
                                plant
) sub
left join plant on plant.id = sub.plant
left join inverter on inverter.id = sub.inverter
where sub.plant = 1
group by plant.name;