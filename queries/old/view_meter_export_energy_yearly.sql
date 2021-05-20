 SELECT date_trunc('year'::text, sistema_contador."time") AS year_count,
    (sum(sistema_contador.export_energy) * (1000)::numeric) AS export_energy_wh,
    sistema_contador.name AS contador
   FROM public.sistema_contador
  GROUP BY (date_trunc('year'::text, sistema_contador."time")), sistema_contador.name
  ORDER BY (date_trunc('year'::text, sistema_contador."time"));


