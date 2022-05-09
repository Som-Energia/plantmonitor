select
 date_trunc('month', reg.time at time zone 'Europe/Madrid') AS time,
 plant.name as plant_name,
 meter.name as meter_name,
 sum(reg.export_energy_wh)/1000000 as energia_comptadors_mensual_mwh,
 (sum(reg.export_energy_wh)/plantparameters.peak_power_w::float)::bigint AS hores_equivalents --TODO parametrize 2160000 Potencia pic
from meterregistry as reg
left join meter
on meter.id = reg.meter
left join plant
on plant.id = meter.plant
left JOIN plantparameters
ON plant.id = plantparameters.plant
where plant.id = {{ plant }}
GROUP BY date_trunc('month', reg.time at time zone 'Europe/Madrid'),                               
    plant.name,
    reg.meter,
    meter.name,
    plantparameters.peak_power_w
order by time desc;
