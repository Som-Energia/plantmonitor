select reg.*,  replace(meter.name,'1','9') from meterregistry reg
left join meter on meter.id = reg.meter
where reg.time >= '2021-01-01'
order by reg.time desc, meter
limit 100000;