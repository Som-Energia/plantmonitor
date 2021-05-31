SELECT reg.time as "time",
       0.97*reg.expected_energy_wh / 1000.0 as expected_energy_kwh
FROM hourlysensorirradiationregistry AS reg
ORDER BY "time" DESC
LIMIT 2000;