 SELECT
    meterregistry."time" AS "time",
    plant.name as plant,
    meter.name as meter,
    export_energy_wh,
    irradiation_w_m2_h,
    (export_energy_wh / 2160000.0) / (NULLIF(irradiation_w_m2_h, 0.0) / 1000.0) AS pr_hourly
 FROM meterregistry
 JOIN meter
    ON meterregistry.meter = meter.id
 JOIN view_irradiation
    ON view_irradiation."time" = meterregistry."time"
 JOIN sensor
    ON sensor.plant = meter.plant
    AND view_irradiation.sensor = sensor.id
 JOIN plant
    ON meter.plant = plant.id;