 select date_trunc('{{granularity}}', sub.time at time zone 'Europe/Madrid') AS temps,
       plant as plant,
       inverter as inverter,
       sum(sub.energy_kwh_incremental) as energy_kwh_acumulated
 from (
     SELECT reg.time AS "time",
           reg.plant_name as plant,
           reg.inverter_name as inverter,
           reg.energy_wh/1000 as energy_kwh_acumulated,
           (reg.energy_wh - lag(reg.energy_wh) over (order by plant_name, inverter_name, reg.time))/1000 as energy_kwh_incremental
       FROM view_inverterregistry_energes as reg
       where plant_name = 'Florida' 
       and reg.time between '{{ interval.start }}' and '{{ interval.end }}'
       order by plant, inverter, "time"
) sub
where plant = 'Florida' 
and '2021-08-01' at time zone 'Europe/Madrid' < time
and time between '{{ interval.start }}' and '{{ interval.end }}'
group by date_trunc('{{granularity}}', sub.time at time zone 'Europe/Madrid'), plant, ROLLUP(inverter)
order by temps;
