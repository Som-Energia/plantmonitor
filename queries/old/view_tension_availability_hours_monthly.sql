 SELECT date_trunc('month'::text, sistema_contador."time") AS month,
    count(*) AS hd,
    sistema_contador.name
   FROM public.sistema_contador
  WHERE (sistema_contador.export_energy > 0)
  GROUP BY sistema_contador.name, (date_trunc('month'::text, sistema_contador."time"))
  ORDER BY (date_trunc('month'::text, sistema_contador."time")) DESC;


