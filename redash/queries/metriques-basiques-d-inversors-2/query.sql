SELECT TIME AT TIME ZONE 'Europe/Madrid' AT TIME ZONE 'Europe/Madrid' AS TIME,
                                                      inverter.name AS inverter,
                                                      energy_wh,
                                                      power_w
FROM inverterregistry AS ireg
LEFT JOIN inverter ON inverter.id = ireg.inverter
LEFT JOIN plant ON plant.id = inverter.plant
WHERE plant.name = '{{ plant }}'
  AND ireg.time BETWEEN '{{ interval.start }}' AND '{{ interval.end }}'