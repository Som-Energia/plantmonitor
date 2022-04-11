SELECT plant_name,
       count(*),
       count(*) = 0 AS inverter_2h_hot
FROM
  ( SELECT ireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
                                  plant.name AS plant_name,
                                  inverter.name AS inverter_name,
                                  max(temperature_dc) AS max_t,
                                  min(temperature_dc) AS min_t,
                                  max(temperature_dc) - min(temperature_dc) AS d_t
   FROM inverterregistry ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE now() - interval '2h' < ireg.time
   GROUP BY ireg.time,
            plant.name,
            rollup(inverter.name)
   ORDER BY ireg.time,
            plant.name,
            inverter.name) sub
WHERE inverter_name IS NULL --select the rollup row
  AND (d_t < 100 or max_t < 400)
GROUP BY plant_name