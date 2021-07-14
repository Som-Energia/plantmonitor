-- Necesitamos los historicos de la energia 
SELECT date_trunc('year', meterregistry.time at time zone 'Europe/Madrid') AS "time",
       meter.name AS meter,
       plant.name AS plant,
       sensor as sensor,
       sum(export_energy_wh)/1000000.0 AS export_energy_mwh,
       sum(integratedirradiation_wh_m2)/1000.0 AS integratedirradiation_kwh_m2,
       100*(sum(export_energy_wh / 2160000.0) / sum(NULLIF(integratedirradiation_wh_m2, 0.0) / 1000.0)) AS pr,
       (sum(export_energy_wh)/2160000)::bigint AS hores_equivalents, --TODO parametrize 2160000 Potencia pic,
       MAX(sub.target_energy_mwh) as target_energy_mwh,
       MAX(historic.historic_avg) as historic_avg_mwh
FROM (SELECT 
    date_trunc('year', view_target_energy.time at time zone 'Europe/Madrid') at time zone 'Europe/Madrid' AS "time",
    sum(view_target_energy.target_energy_kwh)/1000.0 AS target_energy_mwh FROM view_target_energy
    GROUP BY 
    date_trunc('year', view_target_energy.time at time zone 'Europe/Madrid') at time zone 'Europe/Madrid'
    ) AS sub,
    (SELECT 
    plant,
    date_trunc('year', historic.time at time zone 'Europe/Madrid') AS "time",
    sum(historic.historic_avg) as historic_avg
    FROM view_average_yearly_production_plant AS historic WHERE plant = '{{plant}}'
    GROUP BY 
    date_trunc('year', historic.time at time zone 'Europe/Madrid'),
    plant
    ) AS historic,
    meterregistry
LEFT JOIN meter ON meterregistry.meter = meter.id
LEFT JOIN hourlysensorirradiationregistry ON hourlysensorirradiationregistry."time" = meterregistry."time"
LEFT JOIN sensor ON sensor.plant = meter.plant
AND hourlysensorirradiationregistry.sensor = sensor.id
LEFT JOIN plant ON meter.plant = plant.id
LEFT JOIN view_target_energy ON extract('month' from meterregistry.time at time zone 'Europe/Madrid') = extract('month' from view_target_energy.time)
--LEFT JOIN historic ON extract('year' from historic.time at time zone 'Europe/Madrid') = extract('year' from view_target_energy.time)
WHERE plant.name = '{{ plant }}' and 
date_trunc('year', historic.time at time zone 'Europe/Madrid') = date_trunc('year', meterregistry.time at time zone 'Europe/Madrid')
GROUP BY date_trunc('year', meterregistry.time at time zone 'Europe/Madrid'),
         meter.name,
         plant.name,
         historic.plant,
         sensor;