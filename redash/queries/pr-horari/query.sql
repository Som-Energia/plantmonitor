 SELECT 
    meterregistry."time" AS "time",
    meter.name as meter,
    meterregistry.export_energy_wh AS export_energy_wh,
    hourlysensorirradiationregistry.integratedirradiation_wh_m2,
    (
        (meterregistry.export_energy_wh)::double precision / (2160.0)::double precision
    ) / (
        NULLIF((hourlysensorirradiationregistry.integratedirradiation_wh_m2)::double precision, (0.0)::double precision) / 1000.0::double precision
    ) AS pr_hourly
 FROM meterregistry
 JOIN meter
    ON meterregistry.meter = meter.id
 JOIN hourlysensorirradiationregistry
    ON hourlysensorirradiationregistry."time" = meterregistry."time"
 join sensor
    on sensor.plant = meter.plant 
    and hourlysensorirradiationregistry.sensor = sensor.id;