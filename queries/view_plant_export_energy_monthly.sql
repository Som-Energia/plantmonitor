SELECT
   "time" AS "time",
   plant_id,
   plant_name,
   sum(export_energy_wh) AS export_energy_wh
FROM view_meter_export_energy_monthly as meter_energy
WHERE "time" >= '2021-01-01'::date
GROUP BY "time", plant_id, plant_name
--ORDER BY "time", plant_id, plant_name
UNION
SELECT
   "time",
   plant as plant_id,
   plant.name as plant_name,
   export_energy_wh 
FROM plantmonthlylegacy as legacy
LEFT JOIN plant ON plant.id = legacy.plant
ORDER BY "time", plant_id, plant_name
;
