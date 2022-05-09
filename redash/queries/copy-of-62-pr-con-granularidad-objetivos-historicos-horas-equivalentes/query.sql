/*
SELECT date_trunc('year', meterregistry.time at time zone 'Europe/Madrid') AS "time",
       meter.name AS meter,
       plant.name AS plant,
       sensor as sensor,
       sum(export_energy_wh)/1000000.0 AS export_energy_mwh,
       sum(view_irradiation.irradiation_w_m2_h)/1000.0 AS irradiation_kwh_m2,
       coalesce(max(legacy_pr.pr),100*(sum(export_energy_wh / plantparameters.peak_power_w::float) / sum(NULLIF(view_irradiation.irradiation_w_m2_h, 0.0) / 1000.0))) as pr,
       (sum(export_energy_wh)/plantparameters.peak_power_w::float)::bigint AS hores_equivalents, --TODO parametrize 2160000 Potencia pic,
       MAX(sub.target_energy_mwh) as target_energy_mwh,
       MAX(historic.historic_avg) as historic_avg_mwh
      */

SELECT date_trunc('year', meterregistry.time at time zone 'Europe/Madrid') AS "time",
sum(export_energy_wh)/1000000.0 AS value,
'export_energy_mwh' as metric
from meterregistry
left join meter on meter.id = meterregistry.meter
left join plant on plant.id = meter.plant
where plant.name = '{{ plant }}'
group by date_trunc('year', meterregistry.time at time zone 'Europe/Madrid');

/*
-- Necesitamos los historicos de la energia 
SELECT date_trunc('year', meterregistry.time at time zone 'Europe/Madrid') AS "time",
       meter.name AS meter,
       plant.name AS plant,
       sensor as sensor,
       sum(export_energy_wh)/1000000.0 AS export_energy_mwh,
       sum(view_irradiation.irradiation_w_m2_h)/1000.0 AS irradiation_kwh_m2,
       coalesce(max(legacy_pr.pr),100*(sum(export_energy_wh / plantparameters.peak_power_w::float) / sum(NULLIF(view_irradiation.irradiation_w_m2_h, 0.0) / 1000.0))) as pr,
       (sum(export_energy_wh)/plantparameters.peak_power_w::float)::bigint AS hores_equivalents, --TODO parametrize 2160000 Potencia pic,
       MAX(sub.target_energy_mwh) as target_energy_mwh,
       MAX(historic.historic_avg) as historic_avg_mwh
FROM (SELECT 
    date_trunc('year', pe.time at time zone 'Europe/Madrid') at time zone 'Europe/Madrid' AS "time",
    sum(pe.monthly_target_energy_kwh)/1000.0 AS target_energy_mwh,
    sum(pe.monthly_historic_energy_kwh)/1000.0 as historic_avg
    FROM plantestimatedmonthlyenergy as pe
    GROUP BY 
    date_trunc('year', pe.time at time zone 'Europe/Madrid') at time zone 'Europe/Madrid'
    ) AS sub,
    (SELECT 
    plant,
    date_trunc('year', historic.time at time zone 'Europe/Madrid') AS "time",
    sum(historic.historic_avg) as historic_avg
    FROM view_average_yearly_production_plant AS historic WHERE plant = '{{plant}}'
    GROUP BY 
    date_trunc('year', historic.time at time zone 'Europe/Madrid'),
    plant
    ) AS historic
LEFT JOIN (
select * from (values 
('2016-01-01+01:00'::timestamptz, 0.8), 
('2017-01-01+01:00'::timestamptz, 0.9194), 
('2018-01-01+01:00'::timestamptz, 0.8594), 
('2019-01-01+01:00'::timestamptz, 0.8455), 
('2020-01-01+01:00'::timestamptz, 0.84)
) as s(time,pr)) as legacy_pr on extract('year' from legacy_pr.time) = extract('year' from historic."time"),
    meterregistry
LEFT JOIN meter ON meterregistry.meter = meter.id
LEFT JOIN view_irradiation ON view_irradiation."time" = meterregistry."time"
LEFT JOIN sensor ON sensor.plant = meter.plant
AND view_irradiation.sensor = sensor.id
LEFT JOIN plant ON meter.plant = plant.id
LEFT JOIN plantparameters ON plant.id = plantparameters.plant
LEFT JOIN view_target_energy ON extract('month' from meterregistry.time at time zone 'Europe/Madrid') = extract('month' from view_target_energy.time)
--LEFT JOIN historic ON extract('year' from historic.time at time zone 'Europe/Madrid') = extract('year' from view_target_energy.time)
WHERE plant.name = '{{ plant }}' and 
date_trunc('year', historic.time at time zone 'Europe/Madrid') = date_trunc('year', meterregistry.time at time zone 'Europe/Madrid')
GROUP BY date_trunc('year', meterregistry.time at time zone 'Europe/Madrid'),
         meter.name,
         plant.name,
         historic.plant,
         sensor,
         plantparameters.peak_power_w;
*/