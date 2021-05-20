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
            (public.meterregistry.export_energy_wh)::double precision / (2160.0)::double precision
        ) / (
            NULLIF((hourly_sensor_registry.irradiation_wh_m2)::double precision, (0.0)::double precision) / 1000000.0::double precision
        ) AS brute_pr,
	public.meterregistry."time" AS "time",
	public.meter.name as meter,
	public.meterregistry.export_energy_wh * 1000 AS export_energy_wh,
	public.hourly_sensor_registry.irradiation_wh_m2
     FROM public.hourly_sensor_registry
     JOIN public.meter
	ON meterregistry.meter = meter.id
     JOIN public.hourlysensorirradiationregistry
	ON hourlysensorirradiationregistry.meter = meter.id
	AND hourlysensorirradiationregistry."time" = timezone('UTC'::text, meterregistry."time")
) as sub


