select *, tinv1.temperature_dc - tinv2.temperature_dc as tempdiff from (
	SELECT
		ireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
		plant.name AS plant_name,
		inverter.name AS inverter_name,
		temperature_dc
	FROM inverterregistry ireg
	LEFT JOIN inverter ON inverter.id = ireg.inverter
	LEFT JOIN plant ON plant.id = inverter.plant
	WHERE now() - interval '2h' < ireg.time
	ORDER BY ireg.time, plant.name, temperature_dc asc, inverter.name
) tinv1
left JOIN (
	SELECT ireg.time AT TIME ZONE 'Europe/Madrid' AS TIME,
		plant.name AS plant_name,
		inverter.name AS inverter_name,
		temperature_dc
	FROM inverterregistry ireg
	LEFT JOIN inverter ON inverter.id = ireg.inverter
	LEFT JOIN plant ON plant.id = inverter.plant
	WHERE now() - interval '2h' < ireg.time
	ORDER BY ireg.time, plant.name, temperature_dc desc, inverter.name
) tinv2 on tinv1.time = tinv2.time