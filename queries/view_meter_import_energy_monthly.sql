 SELECT (sum(sistema_contador.import_energy) * (1000)::numeric) AS import_energy_wh,
    date_trunc('month'::text, sistema_contador."time") AS "time",
    sistema_contador.name
   FROM public.sistema_contador
  GROUP BY sistema_contador.name, (date_trunc('month'::text, sistema_contador."time"));


