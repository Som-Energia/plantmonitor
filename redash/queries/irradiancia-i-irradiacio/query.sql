SELECT coalesce(reg.time, hourlysensorirradiationregistry.time) AS "time",
       plant.name AS plant_name,
       reg.irradiation_w_m2 as irradiance_w_m2,
       hourlysensorirradiationregistry.integratedirradiation_wh_m2 as irradiation_wh_m2
FROM sensorirradiationregistry AS reg
FULL OUTER JOIN hourlysensorirradiationregistry ON hourlysensorirradiationregistry.time = reg.time
LEFT JOIN sensor ON reg.sensor = sensor.id
OR hourlysensorirradiationregistry.sensor = sensor.id
LEFT JOIN plant ON plant.id = sensor.plant
WHERE plant.name = '{{ planta }}'
  AND coalesce(reg.time, hourlysensorirradiationregistry.time) >= '{{ interval.start }}'
  AND coalesce(reg.time, hourlysensorirradiationregistry.time) <= '{{ interval.end }}'
ORDER BY "time" DESC;

