SELECT date_trunc('day',reg.time) at time zone 'Europe/Madrid' as temps,
       sum(export_energy_wh)/1000.0 AS export_energy_kwh
FROM meterregistry AS reg
group by temps
order by temps desc
limit 1;