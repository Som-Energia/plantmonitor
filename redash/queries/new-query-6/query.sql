SELECT 
    date_trunc('month', view_target_energy.time at time zone 'Europe/Madrid')  AS "time",
    sum(view_target_energy.target_energy_kwh)/1000.0 AS target_energy_mwh FROM view_target_energy
    GROUP BY 
    date_trunc('month', view_target_energy.time at time zone 'Europe/Madrid')

    