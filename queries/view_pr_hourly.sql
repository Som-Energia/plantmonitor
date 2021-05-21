 SELECT
    meterregistry."time" AS "time",
    plant.name as plant,
    meter.name as meter,
    export_energy_wh,
    integratedirradiation_wh_m2,
    (export_energy_wh / 2160.0) / (NULLIF(integratedirradiation_wh_m2, 0.0) / 1000.0) AS pr_hourly
 FROM meterregistry
 JOIN meter
    ON meterregistry.meter = meter.id
 JOIN hourlysensorirradiationregistry
    ON hourlysensorirradiationregistry."time" = meterregistry."time"
 JOIN sensor
    ON sensor.plant = meter.plant
    AND hourlysensorirradiationregistry.sensor = sensor.id
 JOIN plant
    ON meter.plant = plant.id;