SELECT
   time_bucket_gapfill('5 minutes', sir.time::timestamptz) as time,
   sir.inverter,
   avg(sir.temperature_dc)::int as temperature_dc,
   avg(sir.power_w)::int as power_w,
   max(sir.energy_wh)::bigint as energy_wh
FROM inverterregistry as sir
WHERE time >= '{}' and time < '{}'
GROUP by sir.inverter, time_bucket_gapfill('5 minutes', sir.time::timestamptz)
order by time desc,sir.inverter