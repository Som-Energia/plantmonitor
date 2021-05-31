SELECT date_trunc('day', TIME) AS TIME,
       plant.id as plant_id,
       plant.name as plant_name,
       inverter.id as inverter_id,
       inverter.name as inverter_name,
       max(energy_wh) as energy_wh
FROM inverterregistry AS reg
LEFT JOIN inverter ON inverter.id = reg.inverter
LEFT JOIN plant ON plant.id = inverter.plant
GROUP BY date_trunc('day', reg.time), plant.id, plant.name, inverter.id, inverter.name
ORDER BY date_trunc('day', reg.time), plant.id, plant.name, inverter.id, inverter.name
;
