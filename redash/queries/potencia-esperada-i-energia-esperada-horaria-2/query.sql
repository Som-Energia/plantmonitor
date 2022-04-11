SELECT coalesce(reg.time, view_expected_power.time) AS "time",
       reg.expected_energy_wh,
       view_expected_power.expected_power_w_hourly_sum
FROM hourlysensorirradiationregistry AS reg
LEFT JOIN
  (SELECT date_trunc('hour', view_expected_power.time) AS "time",
          sum(view_expected_power.expectedpower)*1000.0 AS expected_power_w_hourly_sum
   FROM view_expected_power
   GROUP BY date_trunc('hour', view_expected_power.time)) as view_expected_power ON view_expected_power.time = reg.time
ORDER BY TIME DESC
LIMIT 1000;

