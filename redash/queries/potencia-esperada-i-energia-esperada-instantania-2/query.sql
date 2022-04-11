SELECT coalesce(view_expected_power.time, reg.time) as "time",
       0.97*reg.expected_energy_wh / 1000000.0 as expected_energy_mwh,
       view_expected_power.expectedpower*1000/1000000.0 as expectedpower_mw
FROM hourlysensorirradiationregistry AS reg
full outer JOIN view_expected_power on reg.time = view_expected_power.time
ORDER BY "time" DESC
LIMIT 1000;

