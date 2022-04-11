select time, plant.name as plant, sensor.name as device, irradiation_w_m2 as value, 'irradiation_w_m2' as metric from sensorirradiationregistry sir
left join sensor on sensor.id = sir.sensor
left join plant on plant.id = sensor.plant
   WHERE plant.name = '{{ plant }}'
     AND sir."time" at time zone 'Europe/Madrid' >= '{{ interval.start }}'
     AND sir."time" at time zone 'Europe/Madrid' <= '{{ interval.end }}'
union 
select time, plant.name as plant, 'inverters' as device, sum(power_w)/1000 as value, max('power_kw') as metric from inverterregistry reg
left join inverter on inverter.id = reg.inverter
left join plant on plant.id = inverter.plant
   WHERE plant.name = '{{ plant }}'
     AND reg."time" at time zone 'Europe/Madrid' >= '{{ interval.start }}'
     AND reg."time" at time zone 'Europe/Madrid' <= '{{ interval.end }}'
group by time, plant.name
union
select time, plant.name as plant, 'expectedpower' as device, expectedpower as value, 'expectedpower_kw' as metric from view_expected_power as epreg
left join plant on plant.id = epreg.plant
   WHERE plant.name = '{{ plant }}'
     AND epreg."time" at time zone 'Europe/Madrid' >= '{{ interval.start }}'
     AND epreg."time" at time zone 'Europe/Madrid' <= '{{ interval.end }}'
order by time desc, device;