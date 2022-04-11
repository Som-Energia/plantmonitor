
    SELECT max(inverter_time) at time zone 'Europe/Madrid' as inverter_time,
    max(sensor_time) at time zone 'Europe/Madrid' as sensor_time,
    plant.id as plant_id,
    plant.name as plant_name,
    inverter.name as inverter_name,
    max(power_w) as power_w,
    sum(irradiation_w_m2) as irradiation_w_m2,
    (not (sum(irradiation_w_m2) > 100 AND sum(power_w) = 0))::integer AS inverter_ok
    FROM
      ( SELECT max(ireg.time) AS inverter_time,
               plant AS plant,
               inverter,
               max(ireg.power_w) AS power_w
       FROM inverterregistry ireg
       LEFT JOIN inverter ON inverter.id = ireg.inverter
       LEFT JOIN plant ON plant.id = inverter.plant
       WHERE now() - interval '1h' <= ireg.time
       GROUP BY inverter,
                plant
    ) isub
    left join (SELECT max(sreg.time) AS sensor_time,
                    sensor.plant AS plant,
                    sreg.sensor,
                    avg(sreg.irradiation_w_m2) AS irradiation_w_m2
       FROM sensorirradiationregistry sreg
       LEFT JOIN sensor ON sensor.id = sreg.sensor
       LEFT JOIN plant ON plant.id = sensor.plant
       WHERE now() - interval '1h' <= sreg.time
       GROUP BY sreg.sensor,
                sensor.plant
       ORDER BY sensor_time DESC, sensor,
                                    plant
    ) ssub on isub.plant = ssub.plant 
    left join plant on plant.id = isub.plant
    left join inverter on inverter.id = isub.inverter
    group by plant.name, inverter.name, plant.id
    order by inverter_ok, plant.name, inverter_name;