 SELECT ((view_meter_energy_monthly.export_energy_wh - view_aggregated_energy_inverters_monthly.energy_inverters_wh) / view_aggregated_energy_inverters_monthly.energy_inverters_wh) AS energy_difference_percent,
    view_aggregated_energy_inverters_monthly.month_date,
    view_meter_energy_monthly."time",
    view_meter_energy_monthly.export_energy_wh,
    view_aggregated_energy_inverters_monthly.energy_inverters_wh
   FROM public.view_meter_energy_monthly,
    public.view_aggregated_energy_inverters_monthly
  WHERE ((view_meter_energy_monthly."time" = view_aggregated_energy_inverters_monthly.month_date) AND (view_meter_energy_monthly.name = '501600324'::text));


