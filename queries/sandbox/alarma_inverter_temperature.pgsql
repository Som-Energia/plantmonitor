-- SELECT
-- 	ireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
-- 	plant.name AS plant_name,
-- 	inverter.name AS inverter_name,
-- 	temperature_dc
-- FROM inverterregistry ireg
-- LEFT JOIN inverter ON inverter.id = ireg.inverter
-- LEFT JOIN plant ON plant.id = inverter.plant
-- left JOIN lateral (
-- 	SELECT subireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
-- 		plant.name AS plant_name,
-- 		inverter.name AS inverter_name,
-- 		ireg.temperature_dc - temperature_dc
-- 	FROM inverterregistry subireg
-- 	LEFT JOIN inverter ON inverter.id = ireg.inverter
-- 	LEFT JOIN plant ON plant.id = inverter.plant
-- 	WHERE ireg.time - interval '2h' < subireg.time
-- 	ORDER BY subireg.time, plant.name, temperature_dc desc, inverter.name
-- ) tinv2 on tinv1.time = tinv2.time


SELECT
    subireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
	  pl.name AS plant_name,
	  inv.name AS inverter_name,
    subireg.temperature_dc as inv_temperature_dc,
    subinvs.inverter_name as min_inverter_name,
    subinvs.temperature_dc as min_temperature_dc,
    subireg.temperature_dc - subinvs.temperature_dc as tmp_diff
FROM inverterregistry AS subireg
LEFT JOIN inverter as inv ON inv.id = subireg.inverter
LEFT JOIN plant as pl ON pl.id = inv.plant
LEFT JOIN (
    SELECT
        subireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
        pl.name AS plant_name,
        inv.name AS inverter_name,
        temperature_dc as temperature_dc
    FROM inverterregistry AS subireg
    LEFT JOIN inverter as inv ON inv.id = subireg.inverter
    LEFT JOIN plant as pl ON pl.id = inv.plant
    WHERE pl.name ~ 'Alcolea'
    and '2021-10-01' < subireg.time
) as subinvs
ON subinvs.time = subireg.time and subinvs.plant_name = pl.name
WHERE pl.name ~ 'Alcolea'
and '2021-10-01' < subireg.time
order by time desc, plant_name, inverter_name
LIMIT 100

-- TODO: treure una select que et torni el inversor amb la temperatura mínima per aquella planta i aquell instant de temps
