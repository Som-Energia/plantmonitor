select
 date_trunc('{{ granularity }}', reg.time) AS time,
 plant.name as plant_name,
 reg.inverter as inverter_name,
 max(reg.energy_wh) AS energy_wh
from inverterregistry as reg
left join inverter
on inverter.id = reg.inverter
left join plant
on plant.id = inverter.plant
where plant.id = '{{ plant }}'
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', reg.time),                               
    plant.name,
    reg.inverter,
    rollup(inverter.name)                                             
