SELECT date_trunc('month', TIME) AS TIME,
       plant_id,
       plant_name,
       inverter_id,
       inverter_name,
       sum(energy_wh) as energy_wh
FROM view_inverter_energy_daily AS daily
GROUP BY date_trunc('month', time), plant_id, plant_name, inverter_id, inverter_name
ORDER BY date_trunc('month', time), plant_id, plant_name, inverter_id, inverter_name
;
