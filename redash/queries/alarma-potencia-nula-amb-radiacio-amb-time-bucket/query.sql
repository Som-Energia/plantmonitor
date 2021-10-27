SELECT time_bucket('60 minutes', ireg.time) as inverter_time,
	time_bucket('60 minutes', sreg.time) as sensor_time,
	sreg.sensor,
	sum(sreg.irradiation_w_m2) as irradiation_w_m2,
	inverter.plant as plant,
	inverter,
	sum(ireg.power_w) as power_w,
	sum(sreg.irradiation_w_m2) > 100 and sum(ireg.power_w) = 0 as inverter_down 
	from inverterregistry ireg
left join inverter on inverter.id = ireg.inverter
left join plant on plant.id = inverter.plant
inner join (
	select * from sensorirradiationregistry
	left join sensor on sensor.id = sensor
	where now() - interval '1h' < time 
	order by time desc
	limit 1
) as sreg on plant.id = sreg.plant
where now() - interval '1h' <= ireg.time
group by inverter_time, sensor_time, sreg.sensor, inverter, inverter.plant
order by inverter, inverter_time desc, plant
