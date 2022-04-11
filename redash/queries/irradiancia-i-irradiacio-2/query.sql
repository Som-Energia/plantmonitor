SELECT coalesce(reg.time, view_irradiation.time) at time zone 'Europe/Madrid' AS "time",
       plant.name AS plant_name,
       reg.irradiation_w_m2 as irradiance_w_m2,
       view_irradiation.irradiation_w_m2_h as irradiation_wh_m2
FROM sensorirradiationregistry AS reg
FULL OUTER JOIN view_irradiation ON view_irradiation.time = reg.time
LEFT JOIN sensor ON reg.sensor = sensor.id
OR view_irradiation.sensor = sensor.id
LEFT JOIN plant ON plant.id = sensor.plant
WHERE plant.name = '{{ planta }}'
  AND coalesce(reg.time, view_irradiation.time) at time zone 'Europe/Madrid' >= '{{ interval.start }}'
  AND coalesce(reg.time, view_irradiation.time) at time zone 'Europe/Madrid' <= '{{ interval.end }}'
ORDER BY "time" DESC;

