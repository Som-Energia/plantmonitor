SELECT reg.time AS temps,
       reg.plant,
       reg.inverter,
       sum(reg.potencia_kw) as potencia_kw
FROM view_inverter_power_exiom AS reg
WHERE reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY reg.time,
         reg.plant,
         reg.inverter
ORDER BY reg.time ASC;

