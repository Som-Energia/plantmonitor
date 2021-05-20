 SELECT forecast.month_date,
    meter.contador,
    forecast.facilityid,
    meter.export_energy_wh,
    forecast.forecasted_energy_wh,
    ((meter.export_energy_wh - (forecast.forecasted_energy_wh)::numeric) / NULLIF(meter.export_energy_wh, (0)::numeric)) AS meter_forecast_difference_percent
   FROM ((public.view_meter_export_energy_monthly meter
     JOIN public.view_meteologica_monthly_forecasted_energy forecast ON ((timezone('Europe/Madrid'::text, meter.month_date) = forecast.month_date)))
     JOIN public.facility_meter ON (((facility_meter.meter)::text = meter.contador)))
  WHERE ((facility_meter.facilityid)::text = (forecast.facilityid)::text);


