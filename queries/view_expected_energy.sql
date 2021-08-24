SELECT date_trunc('hour', "time") + interval '1 hour' AS hour,
           plant,
           sensor,
           1000*avg(expectedpower) as expected_energy_wh,
           count(expectedpower) as num_readings
    FROM view_expected_power
    GROUP BY hour, plant, sensor;