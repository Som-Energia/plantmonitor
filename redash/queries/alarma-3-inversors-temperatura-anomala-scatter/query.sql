select
		date_trunc('hour', time) as time,
		plant_name,
		inverter_name,
		min(tmp_diff)/10.0 as tmp_diff_min,
		min(temperature_dc)/10.0 as temperature_dc_min
	from (
		SELECT
			subireg.time AS TIME,
			pl.name AS plant_name,
			inv.name AS inverter_name,
			subireg.temperature_dc as temperature_dc,
			subireg.temperature_dc - subinvs.temperature_dc as tmp_diff
		FROM inverterregistry AS subireg
		LEFT JOIN inverter as inv ON inv.id = subireg.inverter
		LEFT JOIN plant as pl ON pl.id = inv.plant
		LEFT JOIN (
			SELECT distinct on (time, plant)
				subireg.time AS TIME,
				pl.name AS plant_name,
				inv.name AS inverter_name,
				temperature_dc as temperature_dc
			FROM inverterregistry AS subireg
			LEFT JOIN inverter as inv ON inv.id = subireg.inverter
			LEFT JOIN plant as pl ON pl.id = inv.plant
	where now() - interval '1 hour' < time
			order by time, plant, temperature_dc
		) subinvs
		ON subinvs.time = subireg.time and subinvs.plant_name = pl.name
	) diff_inverter
	where now() - interval '1 hour' < time
	group by date_trunc('hour', time), plant_name, inverter_name
	order by time, inverter_name


