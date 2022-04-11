SELECT date_trunc('day', "time") AS time_bucket,
       count(*)
FROM sensorirradiationregistry
GROUP BY time_bucket order by time_bucket;