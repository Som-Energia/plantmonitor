SELECT CASE
           WHEN now() - max(reg.time) > interval '24 hours' THEN 'Error: Sense lectures des de ' || now() - max(reg.time)
           ELSE 'OK'
       END AS meterAlarm
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id=reg.meter
LEFT JOIN plant ON plant.id=meter.plant
WHERE plant.name='{{ planta }}'
GROUP BY plant.name,
         meter.name;