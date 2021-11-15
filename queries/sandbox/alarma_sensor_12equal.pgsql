-- TODO: doesn't take into account gaps, it just takes the last 12 rows
SELECT reg.time at time zone 'Europe/Madrid' as temps,
       plant.name as plant_name,
       reg.sensor,
       reg.irradiation_w_m2,
       reg.irradiation_w_m2 = 0 as sonda_zero,
       reg.irradiation_w_m2 = lag(reg.irradiation_w_m2, 1) OVER(ORDER BY reg.time) AND reg.irradiation_w_m2 = lag(reg.irradiation_w_m2, 2) OVER(ORDER BY reg.time) as sonda_3_equal,
       max(reg.irradiation_w_m2) over w = min(reg.irradiation_w_m2) OVER w as sonda_12_equal,
       reg.irradiation_w_m2 > 1400 as sonda_massa_irradiacio
from sensorirradiationregistry as reg
LEFT JOIN sensor ON sensor.id = reg.sensor
LEFT JOIN plant ON sensor.plant = plant.id
window w as (partition by reg.sensor order by reg.time ROWS between 11 preceding and current row)
