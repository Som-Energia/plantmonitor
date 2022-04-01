SELECT inverterregistry."time",
  plant.id AS plant,
  plant.name AS plant_name,
  inverter.id AS inverter,
  inverter.name AS inverter_name,
  inverterregistry.temperature_dc
  FROM inverterregistry
    LEFT JOIN inverter ON inverter.id = inverterregistry.inverter
    LEFT JOIN plant ON plant.id = inverter.plant
WHERE plant.id in (select id from view_plants_exiom);
