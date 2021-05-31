 SELECT 
    meterregistry."time" AS "time",
    meter.name as meter,
    meterregistry.export_energy_wh AS export_energy_wh,
    hourlysensorirradiationregistry.integratedirradiation_wh_m2,
    (
        meterregistry.export_energy_wh / 2160000.0 -- potencia pic 2.16 MWp
    ) / (
        NULLIF(hourlysensorirradiationregistry.integratedirradiation_wh_m2, 0.0) / 1000.0 -- GSTC 1000 W/m2
    ) AS pr_hourly
 FROM meterregistry
 JOIN meter
    ON meterregistry.meter = meter.id
 JOIN hourlysensorirradiationregistry
    ON hourlysensorirradiationregistry."time" = meterregistry."time"
 join sensor
    on sensor.plant = meter.plant 
    and hourlysensorirradiationregistry.sensor = sensor.id;