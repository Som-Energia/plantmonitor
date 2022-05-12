SELECT reg.time AS "time",
       plant.name AS plant_name,
       inverter.name as inverter_name,
       string.name as string_name,
       string.stringbox_name as stringbox_name,
       reg.intensity_mA as intensity_mA
FROM stringregistry AS reg
LEFT JOIN string on string.id = reg.string
LEFT JOIN inverter ON inverter.id=string.inverter
LEFT JOIN plant ON plant.id=inverter.plant
WHERE plant.id in (select id from view_plants_ercam);
