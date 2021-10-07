SELECT reg.time AS "time",
           reg.plant_name as plant,
           reg.inverter_name as inverter,
           reg.energy_wh/1000 as energy_kwh_acumulated,
           (reg.energy_wh - lag(reg.energy_wh) over (order by plant_name, inverter_name, reg.time))/1000 as energy_kwh_incremental
       FROM view_inverterregistry_energes as reg
       where plant_name = 'Florida' 
       and reg.time between '{{ interval.start }}' and '{{ interval.end }}'
       order by plant, inverter, "time"