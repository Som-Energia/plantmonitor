SELECT
   time_bucket_gapfill('5 minutes', sir.time::timestamptz) as time,
   sir.string,
   avg(sir.intensity_ma)::int as intensity_ma
FROM stringregistry as sir
WHERE time >= '{}' and time < '{}'
GROUP by sir.string, time_bucket_gapfill('5 minutes', sir.time::timestamptz)
order by time desc,sir.string