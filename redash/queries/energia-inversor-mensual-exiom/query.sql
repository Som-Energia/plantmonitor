 SELECT date_trunc('{{granularity}}', reg.time at time zone 'Europe/Madrid') AS temps,
       reg.plant_name as plant,
       reg.inverter_name as inverter,
       sum(reg.energy_wh)/1000 as energy_kwh
   FROM view_inverter_energy_daily_exiom as reg
   where date_trunc('{{granularity}}', reg.time at time zone 'Europe/Madrid') between '{{ interval.start }}' and '{{ interval.end }}'
   group by date_trunc('{{granularity}}', reg.time at time zone 'Europe/Madrid'), reg.plant_name, rollup(inverter)
   order by temps ;
