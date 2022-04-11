
select inverterregistry_5min_avg.time as time, plant.id as plant,
    inverterregistry_5min_avg.inverter,
    inverterregistry_5min_avg.power_w,
    solarevent.sunrise as sunrise,
    solarevent.sunset as sunset
    from inverterregistry_5min_avg
    left join inverter on inverter.id = inverterregistry_5min_avg.inverter
    left join plant on plant.id = inverter.plant
    left join solarevent on inverterregistry_5min_avg.time::date = solarevent.sunrise::date and solarevent.plant = plant.id
    where power_w = 0
    and inverterregistry_5min_avg.time between solarevent.sunrise and solarevent.sunset
    and inverterregistry_5min_avg.time > {}
    order by time desc
