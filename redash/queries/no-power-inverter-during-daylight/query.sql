
select inverterregistry_5min_avg.time5min as time, plant.id as plant,
    inverterregistry_5min_avg.inverter,
    inverterregistry_5min_avg.power_w,
    solarevent.sunrise as sunrise,
    solarevent.sunset as sunset
    from (
        SELECT
           ireg.inverter as inverter,
           time_bucket('5 minutes', ireg.time::timestamptz) as time5min,
           avg(ireg.power_w)::int as power_w
        FROM inverterregistry as ireg
        left join inverter on inverter.id = ireg.inverter
        GROUP by ireg.inverter, time_bucket('5 minutes', ireg.time::timestamptz)
    ) as inverterregistry_5min_avg
    left join inverter on inverter.id = inverterregistry_5min_avg.inverter
    left join plant on plant.id = inverter.plant
    left join solarevent on inverterregistry_5min_avg.time5min::date = solarevent.sunrise::date and solarevent.plant = plant.id
    where power_w = 0 and inverterregistry_5min_avg.time5min between solarevent.sunrise and solarevent.sunset
    order by time5min desc
    limit 1000;