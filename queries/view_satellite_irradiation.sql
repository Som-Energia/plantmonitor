 SELECT sir.plant,
    time_bucket('1 hour', sir."time") as time,
    sir.global_tilted_irradiation_wh_m2 AS irradiation_wh_m2
    FROM satellite_readings as sir;