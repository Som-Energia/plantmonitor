select meter.plant, plant.name as plant_name, meter.name as meter_name, reg.* from meterregistry reg
left join meter on meter.id = reg.meter
left join plant on plant.id = meter.plant
where plant.id in (select id from view_plants_energetica);
