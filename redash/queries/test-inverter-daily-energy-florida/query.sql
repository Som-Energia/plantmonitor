     SELECT reg.time AS "time",
     inverter,
           reg.energy_wh/1000 as energy_kwh_acumulated,
           (reg.energy_wh - lag(reg.energy_wh) over (order by reg.inverter, reg.time))/1000 as energy_kwh_incremental
       FROM inverterregistry as reg
       where inverter in (20,21)
       and reg.time between '{{ interval.start }}' and '{{ interval.end }}'
       order by inverter, "time"