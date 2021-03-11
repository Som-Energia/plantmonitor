 SELECT timezone('Europe/Madrid'::text, sistema_contador."time") AS timezone,
    sistema_contador.name AS contador,
    (sistema_contador.export_energy * 1000) AS meter_export_energy_wh,
    integrated_sensors.integral_irradiation_wh_m2,
        CASE
            WHEN (integrated_sensors.integral_irradiation_wh_m2 = 0) THEN (0.0)::double precision
            WHEN ((1.05)::double precision < raw_pr_tmp.pr_hourly_raw) THEN (1.0)::double precision
            ELSE raw_pr_tmp.pr_hourly_raw
        END AS pr_hourly
   FROM (public.integrated_sensors
     JOIN public.sistema_contador ON ((integrated_sensors."time" = timezone('UTC'::text, sistema_contador."time")))),
    LATERAL ( VALUES ((((sistema_contador.export_energy)::double precision / (2160.0)::double precision) / (NULLIF((integrated_sensors.integral_irradiation_wh_m2)::double precision, (0.0)::double precision) / (1000000.0)::double precision)))) raw_pr_tmp(pr_hourly_raw);


