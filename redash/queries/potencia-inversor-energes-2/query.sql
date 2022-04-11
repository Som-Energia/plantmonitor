SELECT reg.time at time zone 'Europe/Madrid' AS temps,
       plant_name AS plant,
       inverter_name AS inverter,
       sum(power_w)/1000 as potencia_kw
FROM view_inverterregistry_energes AS reg
WHERE plant_name = '{{ plant }}'
  AND reg.time between '{{ interval.start }}' AND '{{ interval.end }}'
GROUP BY reg.time at time zone 'Europe/Madrid',
         plant_name,
         rollup(inverter_name)
order by temps asc;