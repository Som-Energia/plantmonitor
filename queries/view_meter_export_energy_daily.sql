 SELECT date_trunc('day'::text, meterregistry."time") AS day_date,
    meter.name AS contador,
    sum(meterregistry.export_energy_wh) AS export_energy_wh
   FROM public.meter
   LEFT JOIN public.meterregistry ON meter.id = meterregistry.meter
  GROUP BY date_trunc('day'::text, meterregistry."time"), meter.name
  ORDER BY date_trunc('day'::text, meterregistry."time");


