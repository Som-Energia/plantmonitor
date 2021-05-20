SELECT
   date_trunc('month'::text, sistema_contador."time") AS "time",
   sistema_contador.name,
   (sum(sistema_contador.export_energy) * (1000)::numeric) AS export_energy_wh
FROM public.sistema_contador
GROUP BY (date_trunc('month'::text, sistema_contador."time")), sistema_contador.name
ORDER BY (date_trunc('month'::text, sistema_contador."time")), sistema_contador.name;


