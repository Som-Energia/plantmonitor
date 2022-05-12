SELECT timezone('Europe/Madrid', reg."time") AS "time",
  plant.name AS plant,
  inverter.name AS inverter,
  sum(reg.power_w) / 1000::numeric AS potencia_kw
  FROM inverterregistry reg
    LEFT JOIN inverter ON inverter.id = reg.inverter
    LEFT JOIN plant ON plant.id = inverter.plant
WHERE plant.id in (select id from view_plants_ercam)
GROUP BY (timezone('Europe/Madrid', reg."time")), plant.name, ROLLUP(inverter.name)
ORDER BY (timezone('Europe/Madrid', reg."time"));
