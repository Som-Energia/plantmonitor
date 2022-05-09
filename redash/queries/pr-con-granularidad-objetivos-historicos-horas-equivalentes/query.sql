
SELECT date_trunc('year', meterregistry.time at time zone 'Europe/Madrid') AS "time",
sum(export_energy_wh)/1000000.0 AS value,
'export_energy_mwh' as metric
from meterregistry
left join meter on meter.id = meterregistry.meter
left join plant on plant.id = meter.plant
where plant.name = '{{ plant }}'
group by date_trunc('year', meterregistry.time at time zone 'Europe/Madrid')

union

SELECT date_trunc('year', meterregistry.time at time zone 'Europe/Madrid') AS "time",
(sum(export_energy_wh)/max(plantparameters.peak_power_w)::float)::bigint AS value,
'hores_equivalents' as metric
from meterregistry
left join meter on meter.id = meterregistry.meter
left join plant on plant.id = meter.plant
left join plantparameters on plantparameters.plant = plant.id
where plant.name = '{{ plant }}'
group by date_trunc('year', meterregistry.time at time zone 'Europe/Madrid')

union 

select date_trunc('year', time at time zone 'Europe/Madrid') AS "time",
historic_avg as value,
'historic_avg_mwh' as metric
from view_average_yearly_production_plant AS historic 
WHERE plant = '{{ plant }}'

union

-- TODO this will break if there's more than one year per plant
select ys as "time", 
value as value,
metric as metric
from generate_series('2016-01-01', NOW(), interval '1 year') ys,   (
	select date_trunc('year', pe.time at time zone 'Europe/Madrid') AS create_year,
    plantparameters, 
    sum(monthly_target_energy_kwh)/1000.0 as value,
    'target_energy_mwh' as metric
    from plantestimatedmonthlyenergy as pe
	left join plantparameters as pp on pp.id = pe.plantparameters
	left join plant on plant.id = pp.plant
	WHERE plant.name = '{{ plant }}'
	group by date_trunc('year', pe.time at time zone 'Europe/Madrid'), plantparameters
	order by date_trunc('year', pe.time at time zone 'Europe/Madrid') desc
) as yearly_target

union

SELECT date_trunc('year', time at time zone 'Europe/Madrid') AS "time",
sum(irradiation_w_m2_h)/1000.0 AS value,
'irradiation_kwh_m2' as metric
from view_irradiation
left join sensor on view_irradiation.sensor = sensor.id
left join plant on plant.id = sensor.plant
where plant.name = '{{ plant }}'
group by date_trunc('year', time at time zone 'Europe/Madrid')

union 

select coalesce(legacy_pr."time", sub."time") at time zone 'Europe/Madrid' as "time",
coalesce(100.0*legacy_pr.pr,sub.pr) as value,
'pr' as metric
from (
 SELECT date_trunc('year', meterregistry.time at time zone 'Europe/Madrid') at time zone 'Europe/Madrid' AS "time",
    100*sum(meterregistry.export_energy_wh::double precision / plantparameters.peak_power_w::double precision) / sum(NULLIF(view_irradiation.irradiation_w_m2_h, 0.0::double precision) / 1000.0::double precision) AS pr
   FROM meterregistry
     JOIN meter ON meterregistry.meter = meter.id
     JOIN view_irradiation ON view_irradiation."time" = meterregistry."time"
     JOIN sensor ON sensor.plant = meter.plant AND view_irradiation.sensor = sensor.id
     JOIN plant ON meter.plant = plant.id
     LEFT JOIN plantparameters ON plant.id = plantparameters.plant
     where plant.name = '{{ plant }}'
    group by date_trunc('year', meterregistry.time at time zone 'Europe/Madrid')
) as sub
full outer JOIN (
select * from (values 
('2016-01-01+01:00'::timestamptz, 0.8), 
('2017-01-01+01:00'::timestamptz, 0.9194), 
('2018-01-01+01:00'::timestamptz, 0.8594), 
('2019-01-01+01:00'::timestamptz, 0.8455), 
('2020-01-01+01:00'::timestamptz, 0.84)
) as s(time,pr)) as legacy_pr on extract('year' from legacy_pr.time) = extract('year' from sub."time")

order by "time" desc;
