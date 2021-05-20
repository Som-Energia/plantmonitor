 SELECT (((view_meter_export_energy_yearly.export_energy_wh / (2160000)::numeric))::double precision / (NULLIF((view_irradiation_pr_yearly.yearly_acumulated_irradiation)::double precision, (0)::double precision) / (1000000)::double precision)) AS pr_yearly,
    view_irradiation_pr_yearly.year_date_irradiation_pr,
    view_meter_export_energy_yearly.contador
   FROM public.view_irradiation_pr_yearly,
    public.view_meter_export_energy_yearly
  WHERE (view_irradiation_pr_yearly.year_date_irradiation_pr = view_meter_export_energy_yearly.year_count);


