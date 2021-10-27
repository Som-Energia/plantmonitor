SELECT ireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
      plant.name AS plant_name,
      inverter.name AS inverter_name,
      dense_rank() over (partition by plant,ireg.time order by ireg.temperature_dc) as rank_t,
	  temperature_dc
   FROM inverterregistry ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE now() - interval '2h' < ireg.time
    ORDER BY ireg.time, plant.name, rank_t, inverter.name