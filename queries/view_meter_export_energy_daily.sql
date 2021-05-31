SELECT
   date_trunc('day'::text, meterregistry."time") AS "time",
   plant.id AS plant_id,
   plant.name AS plant_name,
   meter.id AS meter_id,
   meter.name AS meter_name,
   sum(meterregistry.export_energy_wh) AS export_energy_wh
FROM public.meter
LEFT JOIN public.plant ON plant.id = meter.plant
LEFT JOIN public.meterregistry ON meter.id = meterregistry.meter
GROUP BY date_trunc('day'::text, meterregistry."time"), plant.id, plant.name, meter.id, meter.name
ORDER BY date_trunc('day'::text, meterregistry."time"), plant.id, plant.name, meter.id, meter.name
;

