select time, plant, plant.name as plant_name, sensor as sensor, sensor.name as sensor_name, irradiation_w_m2 
from sensorirradiationregistry_5min_avg as ireg
left join sensor on sensor.id = ireg.sensor
left join plant on plant.id = sensor.plant
  WHERE plant.name = '{{ plant }}'
  AND ireg.time between '{{ interval.start }}' and '{{ interval.end }}'
order by time desc
limit 10000;