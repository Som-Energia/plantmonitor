 SELECT 
    meterregistry."time" AS "time",
    meter.name as meter,
    plant.name as plant,
    export_energy_wh,
    view_irradiation.irradiation_w_m2_h,
    100*(export_energy_wh / (plantparameters.peak_power_w/1000)::float) / (NULLIF(view_irradiation.irradiation_w_m2_h, 0.0) / 1000.0) AS pr_hourly
 FROM meterregistry
 JOIN meter
    ON meterregistry.meter = meter.id
 JOIN view_irradiation
    ON view_irradiation."time" = meterregistry."time"
 join sensor
    on sensor.plant = meter.plant 
    and view_irradiation.sensor = sensor.id
 join plant
    on meter.plant = plant.id
 join plantparameters
    on plantparameters.plant = plant.id;
    