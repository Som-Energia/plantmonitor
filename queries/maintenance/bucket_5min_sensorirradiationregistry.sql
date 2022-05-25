SELECT
   time_bucket_gapfill('5 minutes', sir.time::timestamptz) as time,
   sir.sensor,
   avg(sir.irradiation_w_m2)::int as irradiation_w_m2,
   avg(sir.temperature_dc)::int as temperature_dc
FROM sensorirradiationregistry as sir
WHERE time >= '{}' and time < '{}'
GROUP by sir.sensor, time_bucket_gapfill('5 minutes', sir.time::timestamptz)
order by time desc, sir.sensor