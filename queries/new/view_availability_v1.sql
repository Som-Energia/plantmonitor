 SELECT
    hourlysensorirradiationregistry."time" AS "time",
    plant.name as plant,
    count(case integral_irradiation_wh_m2 > 5 else NULL)/count(case view_pr_hourly > 0.70 ) AS "availability"
 FROM hourlysensorirradiationregistry
 JOIN sensor
    ON hourlysensorirradiationregistry.sensor = sensor.id
 JOIN plant
    ON meter.plant = plant.id
 JOIN view_pr_hourly
    ON view_pr_hourly.plant = sensor.plant;