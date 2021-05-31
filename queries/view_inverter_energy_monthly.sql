SELECT date_trunc('month', TIME) AS TIME,
       plant.id as plant_id,
       plant.name as plant_name,
       inverter.id as inverter_id,
       inverter.name as inverter_name,
       sum(energy_wh) as monthly_energy_wh
FROM inverterregistry AS reg
LEFT JOIN inverter ON inverter.id = reg.inverter
LEFT JOIN plant ON plant.id = inverter.plant
GROUP BY date_trunc('month', reg.time),
         plant.id,
         plant.name,
         inverter.id,
         rollup(inverter.name)
ORDER BY TIME DESC;
