SELECT  plant.name as plant_name,
        meter.name as device_name,
        CASE
           WHEN now() - max(reg.time) > interval '24 hours' THEN 'Error: Sense lectures des de ' || now() - max(reg.time)
           ELSE 'OK'
       END AS alarm,
       max(reg.time) at time zone 'Europe/Madrid' as time
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id=reg.meter
LEFT JOIN plant ON plant.id=meter.plant
GROUP BY plant.name,
         meter.name;