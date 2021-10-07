SELECT reg.time AS "time",
       plant.name AS plant,
       inverter.name as inverter_name,
       string.name as string,
       intensity_mA as intensity_mA
FROM stringregistry AS reg
LEFT JOIN string on string.id = reg.string
LEFT JOIN inverter ON inverter.id=string.inverter
LEFT JOIN plant ON plant.id=inverter.plant
WHERE plant.name = 'Alcolea' or plant.name = 'Florida';