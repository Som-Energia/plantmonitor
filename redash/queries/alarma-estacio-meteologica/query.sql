SELECT reg.time at time zone 'Europe/Madrid' as temps,
       plant.name as plant_name,
       reg.sensor,
       reg.irradiation_w_m2,
       reg.irradiation_w_m2 = 0 as sonda_zero,
       reg.irradiation_w_m2 = lag(reg.irradiation_w_m2, 1) OVER(ORDER BY reg.time) AND reg.irradiation_w_m2 = lag(reg.irradiation_w_m2, 2) OVER(ORDER BY reg.time) as sonda_3_equal,
       reg.irradiation_w_m2 > 1400 as sonda_massa_irradiacio
from sensorirradiationregistry as reg
LEFT JOIN sensor ON sensor.id = reg.sensor
LEFT JOIN plant ON sensor.plant = plant.id
INNER JOIN 
(SELECT
       date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid') as time,
       reg.sensor,
       MIN(reg.time) as sunrise,
       MAX(reg.time) as sunset
FROM sensorirradiationregistry AS reg
LEFT JOIN sensor ON sensor.id = reg.sensor
LEFT JOIN plant ON sensor.plant = plant.id
WHERE TIME >= '{{ interval.start }}'
  AND TIME <= '{{ interval.end }}'
  AND reg.irradiation_w_m2 > 10 
GROUP by date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid'),
reg.sensor
ORDER BY time DESC
) as sunrise_sunset ON sunrise_sunset.time = date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid')
WHERE reg.time between sunrise and sunset 
ORDER by reg.time