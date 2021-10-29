select foo.*, plant.name as plant_name, inverter.name as inverter_name, plant.name || ':' || inverter.name as device_name from (
	select hot_inv.*,
        interval '2 hours' < time - lag(time) over (order by inverter, time) as long_state_change_to_hot
	from (
		select time,
			   plant as plant,
			   inverter,
			   tmp_diff,
			   lag(tmp_diff,1) over (order by inverter, time) as previous_tmp_diff,
			   hot_inverter,
			   lag(hot_inverter,1) over (order by inverter, time) as previous_hot_inverter,
			   hot_inverter != lag(hot_inverter,1) over (order by inverter, time) as hot_inverter_changed
		  from (SELECT subireg.time AS TIME,
				  pl.id AS plant,
				  inv.id AS inverter,
				  subireg.temperature_dc AS temperature_dc,
				  subireg.temperature_dc - subinvs.temperature_dc AS tmp_diff,
				  100 < (subireg.temperature_dc - subinvs.temperature_dc) and 400 < subireg.temperature_dc AS hot_inverter
		  FROM inverterregistry AS subireg
		  LEFT JOIN inverter AS inv ON inv.id = subireg.inverter
		  LEFT JOIN plant AS pl ON pl.id = inv.plant
		  LEFT JOIN
			( SELECT DISTINCT ON (TIME,
								  plant) subireg.time AS TIME,
								 pl.id AS plant,
								 inv.id AS inverter,
								 temperature_dc AS temperature_dc
			 FROM inverterregistry AS subireg
			 LEFT JOIN inverter AS inv ON inv.id = subireg.inverter
			 LEFT JOIN plant AS pl ON pl.id = inv.plant
			   WHERE --pl.name = '{{ plant }}' and
			   subireg.time >= '{{ interval.start }}'
			   AND subireg.time <= '{{ interval.end }}'
			 ORDER BY TIME,
					  plant,
					  temperature_dc ) subinvs ON subinvs.time = subireg.time AND subinvs.plant = pl.id
		order by inverter,time desc
		) inv_diff
	) hot_inv
	where hot_inverter_changed = true and hot_inverter = true
	order by inverter, time desc
) foo
left join inverter on inverter.id = foo.inverter
left join plant on plant.id = inverter.plant
where long_state_change_to_hot = true and hot_inverter = true
order by time desc, foo.plant, foo.inverter