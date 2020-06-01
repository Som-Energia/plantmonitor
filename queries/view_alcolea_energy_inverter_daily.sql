 SELECT date_trunc('day'::text, sistema_inversor."time") AS day_date,
    (max(sistema_inversor.daily_energy_l) * 100) AS energy_inversor_wh,
    sistema_inversor.inverter_name
   FROM public.sistema_inversor
  WHERE (sistema_inversor.location = 'Alcolea'::text)
  GROUP BY (date_trunc('day'::text, sistema_inversor."time")), sistema_inversor.inverter_name
  ORDER BY (date_trunc('day'::text, sistema_inversor."time"));


