SELECT TIME,
    plant_name,
       tmin.temperature_dc - tmax.temperature_dc AS temp_diff,
       tmax.temperature_dc > 40 AS hot_inverter
FROM
  ( SELECT DISTINCT ON (ireg.time,
                        plant.name) ireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
                                                           plant.name AS plant_name,
                                                           inverter.name AS inverter_name,
                                                           temperature_dc
   FROM inverterregistry ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE now() - interval '2h' < ireg.time
   ORDER BY ireg.time,
            plant.name,
            temperature_dc ASC, inverter.name) tmax
JOIN
  ( SELECT DISTINCT ON (ireg.time,
                        plant.name) ireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
                                                           plant.name AS plant_name,
                                                           inverter.name AS inverter_name,
                                                           temperature_dc
   FROM inverterregistry ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE now() - interval '2h' < ireg.time
   ORDER BY ireg.time,
            plant.name,
            temperature_dc DESC, inverter.name) tmin USING (TIME,
                                                            plant_name)
group by plant_name
