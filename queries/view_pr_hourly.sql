SELECT timezone('Europe/Madrid'::text, sub."time") AS timezone,
    sub.meter AS contador,
    sub.export_energy_wh AS meter_export_energy_wh,
    sub.integral_irradiation_wh_m2,
    CASE
        WHEN (sub.integral_irradiation_wh_m2 = 0) THEN (0.0)::double precision
	WHEN ((1.05)::double precision < brute_pr) THEN (1.0)::double precision -- TODO: ELSE MIN(brute_pr, 1)
        ELSE brute_pr
    END AS pr_hourly
FROM (
     SELECT 
        (
            (public.sistema_contador.export_energy)::double precision / (2160.0)::double precision
        ) / (
            NULLIF((integrated_sensors.integral_irradiation_wh_m2)::double precision, (0.0)::double precision) / 1000000.0::double precision
        ) AS brute_pr,
	public.sistema_contador."time" AS "time",
	public.sistema_contador.name as meter,
	public.sistema_contador.export_energy * 1000 AS export_energy_wh,
	public.integrated_sensors.integral_irradiation_wh_m2
     FROM public.sistema_contador
     JOIN public.integrated_sensors
	ON integrated_sensors."time" = timezone('UTC'::text, sistema_contador."time")
) as sub


