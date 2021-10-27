SELECT *,
       avg_temperature_dc - avg(avg_temperature_dc) OVER (partition by plant_name) AS dtmp,
       (avg_temperature_dc - avg(avg_temperature_dc) OVER (partition by plant_name) > 10)::integer as hot_inverter
FROM
  (SELECT max(ireg.time) AT TIME ZONE 'Europe/Madrid' AS TIME,
                                      plant.name AS plant_name,
                                      inverter.name AS inverter_name,
                                      avg(temperature_dc) AS avg_temperature_dc
   FROM inverterregistry ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE now() - interval '2h' < ireg.time
   GROUP BY plant_name,
            inverter_name
    ORDER BY plant.name,
            inverter.name) s
where avg_temperature_dc is not null
order by hot_inverter desc, time, plant_name, inverter_name;