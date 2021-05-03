SELECT date_trunc('minute',reg.time) as temps,
       plant.name AS plant,
       --inverter.name as inverter,
       sum(power_w)/1000 AS power_kw
FROM inverterregistry AS reg
LEFT JOIN inverter ON inverter.id=reg.inverter
LEFT JOIN plant ON plant.id=inverter.plant
WHERE plant.name='{{ planta }}'
group by plant.name, date_trunc('minute',reg.time)
order by temps desc
limit 1;