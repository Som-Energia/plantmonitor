select date_trunc('hour', subtime) as time, 
sum(irradiation_w_m2_h) as irradiation_w_m2_h
from (
	select time as subtime, 
		irradiation_w_m2, 
		extract( epoch from (lead(time) over (order by time) - time)::interval) as dt,
		(lead(irradiation_w_m2) over (order by time) + irradiation_w_m2)/2::float as dy,
		(lead(irradiation_w_m2) over (order by time) + irradiation_w_m2)*extract(epoch from (lead(time) over (order by time) - time ))/3600/2 as irradiation_w_m2_h_optimist,
		case when
			lead(time) over (order by time) - time > '00:07' then NULL 
		else (lead(irradiation_w_m2) over (order by time) + irradiation_w_m2)*extract(epoch from (lead(time) over (order by time) - time ))/3600/2
		end as irradiation_w_m2_h
	from sensorirradiationregistry
	  ) as sub
group by time
order by time;