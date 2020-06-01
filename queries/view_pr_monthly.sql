 SELECT (((view_meter_export_energy_monthly.export_energy_wh / (2160000)::numeric))::double precision / (NULLIF((view_irradiation_pr_monthly.monthly_acumulated_irradiation)::double precision, (0)::double precision) / (1000000)::double precision)) AS pr_monthly,
    view_meter_export_energy_monthly.month_date,
    view_meter_export_energy_monthly.contador
   FROM public.view_irradiation_pr_monthly,
    public.view_meter_export_energy_monthly
  WHERE (view_irradiation_pr_monthly.monthly = view_meter_export_energy_monthly.month_date);


