SELECT date_trunc('day', reg."time") AS "time",
  plant.id AS plant_id,
  plant.name AS plant_name,
  inverter.id AS inverter_id,
  inverter.name AS inverter_name,
  max(reg.energy_wh) AS energy_wh
  FROM inverterregistry reg
    LEFT JOIN inverter ON inverter.id = reg.inverter
    LEFT JOIN plant ON plant.id = inverter.plant
WHERE plant.name = 'Llanillos'
GROUP BY (date_trunc('day', reg."time")), plant.id, plant.name, inverter.id, inverter.name
ORDER BY (date_trunc('day', reg."time")), plant.id, plant.name, inverter.id, inverter.name;
