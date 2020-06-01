 SELECT (((sistema_contador.export_energy)::double precision / (2160.0)::double precision) / (NULLIF((integrated_sensors.integral_irradiation_wh_m2)::double precision, (0.0)::double precision) / (1000000.0)::double precision)) AS pr_hourly,
    timezone('Europe/Madrid'::text, sistema_contador."time") AS timezone,
    sistema_contador.name AS contador
   FROM public.integrated_sensors,
    public.sistema_contador
  WHERE ((timezone('UTC'::text, sistema_contador."time") = integrated_sensors."time") AND (sistema_contador.name = '501600324'::text));


