 SELECT
    timezone('Europe/Madrid'::text, sistema_contador."time") AS timezone,
    sistema_contador.name AS contador,
    CASE
        WHEN (integrated_sensors.integral_irradiation_wh_m2 = 0) THEN (0.0)::double precision
        WHEN ((1.05)::double precision < (((sistema_contador.export_energy)::double precision / (2160.0)::double precision) / (NULLIF((integrated_sensors.integral_irradiation_wh_m2)::double precision, (0.0)::double precision) / (1000000.0)::double precision))) THEN (1.0)::double precision
        ELSE (((sistema_contador.export_energy)::double precision / (2160.0)::double precision) / (NULLIF((integrated_sensors.integral_irradiation_wh_m2)::double precision, (0.0)::double precision) / (1000000.0)::double precision))
    END AS pr_hourly
FROM
    public.integrated_sensors,
    public.sistema_contador
WHERE ((timezone('UTC'::text, sistema_contador."time") = integrated_sensors."time") AND (sistema_contador.name = '501600324'::text));


