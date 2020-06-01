 SELECT (sum(sistema_contador.export_energy) * (1000)::numeric) AS export_energy_wh,
    date_trunc('month'::text, sistema_contador."time") AS "time",
    sistema_contador.name
   FROM public.sistema_contador
  GROUP BY sistema_contador.name, (date_trunc('month'::text, sistema_contador."time"));


