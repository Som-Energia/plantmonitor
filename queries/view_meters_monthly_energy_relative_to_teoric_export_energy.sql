 SELECT ((view_meter_energy_monthly.export_energy_wh - (teoric_energy.energy_w)::numeric) / (teoric_energy.energy_w)::numeric) AS energy_difference_percent,
    teoric_energy.energy_w AS energy_teorica_w,
    view_meter_energy_monthly.export_energy_wh,
    teoric_energy."time" AS time_teoric_view,
    view_meter_energy_monthly."time" AS time_teoric,
    view_meter_energy_monthly.name
   FROM public.view_meter_energy_monthly,
    public.teoric_energy
  WHERE (view_meter_energy_monthly."time" = teoric_energy."time");


