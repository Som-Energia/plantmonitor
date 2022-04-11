
    SELECT max(inverter_time) at time zone 'Europe/Madrid' as inverter_time,
    max(sensor_time) at time zone 'Europe/Madrid' as sensor_time,
    plant.id as plant_id,
    plant.name as plant_name,
    inverter.name as inverter_name,
    string.name as string_name,
    max(intensity_mA) as intensity_mA,
    sum(irradiation_w_m2) as irradiation,
    (not (sum(irradiation_w_m2) > 100 AND sum(intensity_mA) = 0))::integer AS string_ok
    FROM
      ( SELECT max(strreg.time) AS inverter_time,
               inverter.plant AS plant,
               inverter,
               string,
               max(strreg.intensity_mA) AS intensity_mA
       FROM stringregistry strreg
       left join string on string.id = strreg.string
       LEFT JOIN inverter ON inverter.id = string.inverter
       LEFT JOIN plant ON plant.id = inverter.plant
       WHERE now() - interval '1h' <= strreg.time
       GROUP BY string,
                inverter,
                inverter.plant
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
    left join string on string.id = isub.string
    group by plant.name, inverter.name, string.name, plant.id
    order by string_ok, plant.name, inverter_name, string_name;