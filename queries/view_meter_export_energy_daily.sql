 SELECT date_trunc('day'::text, sistema_contador."time") AS day_date,
    sistema_contador.name AS contador,
    (sum(sistema_contador.export_energy) * (1000)::numeric) AS export_energy_wh
   FROM public.sistema_contador
  GROUP BY (date_trunc('day'::text, sistema_contador."time")), sistema_contador.name
  ORDER BY (date_trunc('day'::text, sistema_contador."time"));


