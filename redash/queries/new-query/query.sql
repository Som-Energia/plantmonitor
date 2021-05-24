 SELECT 
    meterregistry."time" AS "time",
    meter.name as meter,
    plant.name as plant,
    export_energy_wh,
    integratedirradiation_wh_m2,
    100*(export_energy_wh / 2160.0) / (NULLIF(integratedirradiation_wh_m2, 0.0) / 1000.0) AS pr_hourly
 FROM meterregistry
 JOIN meter
    ON meterregistry.meter = meter.id
 JOIN hourlysensorirradiationregistry
    ON hourlysensorirradiationregistry."time" = meterregistry."time"
 join sensor
    on sensor.plant = meter.plant 
    and hourlysensorirradiationregistry.sensor = sensor.id
 join plant
    on meter.plant = plant.id;