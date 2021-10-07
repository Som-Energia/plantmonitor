 SELECT 
    meterregistry."time" AS "time",
    meter.name as meter,
    meterregistry.export_energy_wh AS export_energy_wh,
    view_irradiation.irradiation_w_m2_h,
    (
        meterregistry.export_energy_wh / 2160000.0 -- potencia pic 2.16 MWp
    ) / (
        NULLIF(view_irradiation.irradiation_w_m2_h, 0.0) / 1000.0 -- GSTC 1000 W/m2
    ) AS pr_hourly
 FROM meterregistry
 JOIN meter
    ON meterregistry.meter = meter.id
 JOIN view_irradiation
    ON view_irradiation."time" = meterregistry."time"
 join sensor
    on sensor.plant = meter.plant 
    and view_irradiation.sensor = sensor.id;