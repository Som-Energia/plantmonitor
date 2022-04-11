select date_trunc('hour', subtime) + interval '01:00' as time,
sensor,
sum(irradiation_w_m2_h) as irradiation_w_m2_h
from (
	select time as subtime,
		sensor,
		irradiation_w_m2,
		extract( epoch from (lead(time) over (partition by sensor order by time) - time)::interval) as dt,
		(lead(irradiation_w_m2) over (partition by sensor order by time) + irradiation_w_m2)/2::float as dy,
		(lead(irradiation_w_m2) over (partition by sensor order by time) + irradiation_w_m2)*extract(epoch from (lead(time) over (partition by sensor order by time) - time ))/3600/2 as irradiation_w_m2_h_optimist,
		case when
			lead(time) over (order by time) - time > '00:07' then irradiation_w_m2*300/3600/2
		else (lead(irradiation_w_m2) over (partition by sensor order by time) + irradiation_w_m2)*extract(epoch from (lead(time) over (partition by sensor order by time) - time ))/3600/2
		end as irradiation_w_m2_h
	from sensorirradiationregistry
	order by sensor, subtime
	  ) as sub
group by sensor, time
order by time;