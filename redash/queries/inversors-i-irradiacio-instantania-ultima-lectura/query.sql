SELECT distinct on (inverter) 
	ireg.time as inverter_time,
	sreg.time as sensor_time,
	sreg.sensor,
	sreg.irradiation_w_m2,
	inverter.plant,
	inverter,
	power_w from inverterregistry ireg
left join inverter on inverter.id = ireg.inverter
left join plant on plant.id = inverter.plant
inner join (
	select * from sensorirradiationregistry
	left join sensor on sensor.id = sensor
	where now() - interval '1h' < time 
	order by time desc
	limit 1
) as sreg on plant.id = sreg.plant
order by inverter, inverter_time desc, plant
