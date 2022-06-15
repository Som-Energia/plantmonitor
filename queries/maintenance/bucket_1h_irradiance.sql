-- we will not do this on maintenance, we'll do it in redash: TODO with 12 average and NULL if one record is NULL (e.g. quality < 1)
/*
select
	sensor,
	time + interval '1 hour', -- projectes wants end-hour
	case
		when quality < 1 then
			NULL
		else
			irradiation_wh_m2
	end as irradiation_w_m2
from bucket_1h_irradiance
*/

select
		time_bucket_gapfill('1 hour', time) as time,
		sensor,
	  avg(reg.irradiation_w_m2) as irradiation_wh_m2,
		count(irradiation_w_m2)::float/12 as quality -- open right interval [11:00, 12:00)
		--count(irradiation_w_m2)::float/13 as quality -- closed interval [11:00, 12:00]
		--count(irradiation_w_m2)::float/count(*) as quality
from bucket_5min_sensorirradiationregistry reg
left join sensor on sensor.id = reg.sensor
WHERE time >= date_trunc('hour', '{}'::timestamptz) and time < date_trunc('hour', '{}'::timestamptz) -- irradiation is hourly
group by sensor, time_bucket_gapfill('1 hour', time)
order by time desc, sensor