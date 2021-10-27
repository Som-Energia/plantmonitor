SELECT ireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
      plant.name AS plant_name,
      inverter.name AS inverter_name,
      max(temperature_dc)
   FROM inverterregistry ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE now() - interval '2h' < ireg.time
      GROUP BY plant_name,
      inverter.name,
            ireg.time
   HAVING max(temperature_dc) - min(temperature_dc) > 10
    ORDER BY ireg.time, plant.name,
            inverter.name