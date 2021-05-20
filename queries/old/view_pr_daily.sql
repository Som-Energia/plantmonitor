 SELECT export_energy_daily.contador,
    irradiation_daily.daily AS day,
        CASE
            WHEN ((1.05)::double precision < (((export_energy_daily.export_energy_wh / 2160000.0) / (NULLIF(irradiation_daily.daily_acumulated_irradiation, 0.0) / 1000000.0)))::double precision) THEN 1.0
            ELSE ((export_energy_daily.export_energy_wh / 2160000.0) / (NULLIF(irradiation_daily.daily_acumulated_irradiation, 0.0) / 1000000.0))
        END AS pr_daily
   FROM (public.view_irradiation_pr_daily irradiation_daily
     JOIN public.view_meter_export_energy_daily export_energy_daily ON ((irradiation_daily.daily = export_energy_daily.day_date)))
  WHERE (export_energy_daily.contador = '501600324'::text);


