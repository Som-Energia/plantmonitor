 SELECT view_meter_export_energy_monthly.contador,
    teoric_energy."time",
    view_meter_export_energy_monthly.month_date,
    abs((view_meter_export_energy_monthly.export_energy_wh - (teoric_energy.energy_w)::numeric)) AS diff_prod_abs,
    view_meter_export_energy_monthly.export_energy_wh,
    teoric_energy.energy_w
   FROM public.teoric_energy,
    public.view_meter_export_energy_monthly
  WHERE ((teoric_energy."time" = view_meter_export_energy_monthly.month_date) AND (view_meter_export_energy_monthly.contador = '501600324'::text));


