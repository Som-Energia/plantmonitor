with subinvs as (
	SELECT distinct on (time, plant)
			subireg.time AS TIME,
			pl.name AS plant_name,
			inv.name AS inverter_name,
			temperature_dc as temperature_dc
		FROM inverterregistry AS subireg
		LEFT JOIN inverter as inv ON inv.id = subireg.inverter
		LEFT JOIN plant as pl ON pl.id = inv.plant
		where '2021-01-01' < subireg.time
		order by time, plant, temperature_dc
)

select ireg_h.time, ireg_h.inverter, inv_temp_diff.*
from inverterregistry ireg_h
LEFT JOIN inverter as inv ON inv.id = ireg_h.inverter
LEFT JOIN plant as pl ON pl.id = inv.plant
left join lateral
(
	SELECT
		ireg_h.time AS TIME,
		  pl.name AS plant_name,
		  inv.name AS inverter_name,
		min(subireg.temperature_dc - subinvs.temperature_dc) as tmp_diff
	FROM inverterregistry AS subireg
	LEFT JOIN subinvs
	ON subinvs.time = subireg.time and subinvs.plant_name = pl.name
	WHERE inverter = ireg_h.inverter
		and subireg.time between ireg_h.time - interval '2h' and ireg_h.time
	group by pl.name, inv.name
	order by time desc, plant_name, inverter_name
) inv_temp_diff on inv_temp_diff.time = ireg_h.time
WHERE pl.name = 'Alcolea'
and '2021-10-01' < ireg_h.time
order by ireg_h.time
limit 1000;