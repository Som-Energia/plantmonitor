 SELECT forecast."time",
    meter.name,
    forecast.facilityid,
    meter.export_energy,
    forecast.percentil50,
    (((meter.export_energy)::numeric - (forecast.percentil50)::numeric) / (NULLIF(meter.export_energy, 0))::numeric) AS meter_forecast_difference_percent
   FROM ((public.sistema_contador meter
     JOIN public.view_join_forecast_table forecast ON ((timezone('Europe/Madrid'::text, meter."time") = forecast."time")))
     JOIN public.facility_meter ON (((facility_meter.meter)::text = meter.name)))
  WHERE ((facility_meter.facilityid)::text = (forecast.facilityid)::text);


