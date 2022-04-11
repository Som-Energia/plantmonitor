SELECT sub.temps::DATE,
       SUM(sub.energy_daily_wh) / 1000 as total_energy_daily_kwh
FROM
  (SELECT Max(energy_wh) AS energy_daily_wh,
          date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid' AS temps
   FROM inverterregistry AS reg
   LEFT JOIN inverter ON inverter.id = reg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE plant.name= '{{ planta }}'
     AND date_trunc('day',reg.time AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid' = CURRENT_DATE
   GROUP BY date_trunc('day',reg.time AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid',
            inverter.name,
            plant.name
   ORDER BY temps DESC) AS sub
WHERE date_trunc('day', sub.temps AT TIME ZONE 'Europe/Madrid') AT TIME ZONE 'Europe/Madrid' = CURRENT_DATE
GROUP BY temps;