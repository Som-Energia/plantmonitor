-- TODO decide:
-- delete this view and do it in redash instead, with proper filters
-- [chosen option] or hope that it's true that the planner limits the rows necessary by the subquery given a parent where clause
-- Time it to decide
 SELECT sir.sensor,
    sir."time",
    COALESCE(sir.irradiation_wh_m2, sat.global_tilted_irradiation_wh_m2) AS irradiation_wh_m2_coalesce,
    sir.irradiation_wh_m2 AS sonda_irradiation_wh_m2,
    sat.global_tilted_irradiation_wh_m2 AS satellite_irradiation_wh_m2,
        CASE
            WHEN sir.quality < 1 THEN sat.global_tilted_irradiation_wh_m2
            ELSE sir.irradiation_wh_m2
        END AS irradiation_wh_m2,
        CASE
            WHEN sir.quality < 1 THEN 'satellite'
            ELSE 'sonda'
        END AS reading_source
   FROM irradiationregistry sir
     LEFT JOIN sensor ON sensor.id = sir.sensor
     LEFT JOIN plant ON plant.id = sensor.plant
     LEFT JOIN satellite_readings sat ON sat.plant = sensor.plant AND time_bucket('1 hour', sat."time") = time_bucket('1 hour', sir."time");