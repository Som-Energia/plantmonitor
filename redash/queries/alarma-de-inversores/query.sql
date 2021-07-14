SELECT  plant.name as plant_name,
        inverter.name as device_name,
        CASE
           WHEN now() - max(reg.time) > interval '24 hours' THEN 'Error: Sense lectures des de ' || now() - max(reg.time)
           ELSE 'OK'
       END AS alarm,
       max(reg.time) at time zone 'Europe/Madrid' as time
FROM inverterregistry AS reg
LEFT JOIN inverter ON inverter.id=reg.inverter
LEFT JOIN plant ON plant.id=inverter.plant
GROUP BY plant.name,
         inverter.name;