select time at time zone 'Europe/Madrid' as time,
plant_name,
inverter_name,
plant_id
from (
	select
		date_trunc('hour', time) as time,
		plant_name,
		inverter_name,
		plant_id,
		min(tmp_diff) as tmp_diff_min,
		min(temperature_dc) as temperature_dc_min
	from (
		SELECT
			subireg.time AS TIME,
			pl.name AS plant_name,
			pl.id as plant_id,
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
				pl.id as plant_id,
				inv.name AS inverter_name,
				temperature_dc as temperature_dc
			FROM inverterregistry AS subireg
			LEFT JOIN inverter as inv ON inv.id = subireg.inverter
			LEFT JOIN plant as pl ON pl.id = inv.plant
			
			order by time, plant, temperature_dc
		) subinvs
		ON subinvs.time = subireg.time and subinvs.plant_name = pl.name
	) diff_inverter
	
	group by date_trunc('hour', time), plant_name, inverter_name, plant_id
	order by time, inverter_name
) as historic_diff_tmp
where tmp_diff_min > 100 and temperature_dc_min >= 400
order by time at time zone 'Europe/Madrid'


