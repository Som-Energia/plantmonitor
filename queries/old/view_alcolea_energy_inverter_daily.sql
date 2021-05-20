 SELECT 
    date_trunc('day'::text, sistema_inversor."time" AT TIME ZONE 'UTC') AS day_date,
    sistema_inversor.inverter_name,
    (max(sistema_inversor.daily_energy_l) * 100) AS energy_inversor_wh
   FROM public.sistema_inversor
  WHERE (sistema_inversor.location = 'Alcolea'::text)
  GROUP BY (date_trunc('day'::text, sistema_inversor."time" AT TIME ZONE 'UTC')), sistema_inversor.inverter_name
  ORDER BY (date_trunc('day'::text, sistema_inversor."time" AT TIME ZONE 'UTC')), sistema_inversor.inverter_name;


