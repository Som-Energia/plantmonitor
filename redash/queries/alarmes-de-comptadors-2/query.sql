SELECT reg.time at time zone 'Europe/Madrid' as time,
       plant.name as plant_name,
        meter.name as device_name,
        CASE
           WHEN reg.export_energy_wh = 0 THEN 'Energia a 0'
           ELSE 'OK'
       END AS alarm,
       sunrise at time zone 'Europe/Madrid' ,
       sunset at time zone 'Europe/Madrid' ,
       reg.time + interval '1 hour' between solarevent.sunrise and solarevent.sunset as daylight
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id=reg.meter
LEFT JOIN plant ON plant.id=meter.plant
left join solarevent on plant.id = solarevent.plant and date(solarevent.sunset) = date(reg.time)
WHERE reg.time + interval '1 hour' between solarevent.sunrise and solarevent.sunset
  AND reg.time between '{{ interval.start }}' and '{{ interval.end }}'
  order by time desc, plant.id, alarm asc;