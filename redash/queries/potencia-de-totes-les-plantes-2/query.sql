SELECT date_trunc('minute',reg.time) at time zone 'Europe/Madrid' as temps,
       sum(power_w)/1000.0 AS power_kw
FROM inverterregistry AS reg
LEFT JOIN inverter ON inverter.id=reg.inverter
LEFT JOIN plant ON plant.id=inverter.plant
group by temps
order by temps desc
limit 1;