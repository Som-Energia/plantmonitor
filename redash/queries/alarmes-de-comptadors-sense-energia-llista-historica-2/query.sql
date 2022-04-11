/*
2 Riudarenes Zona Esportiva, 4 Riudarenes Semursa, 5 Riudarenes Brigada,
14 Manlleu Piscina, 15 Manlleu Pavelló, 16 Lleida (Coberta TMI), 17 Torrefarrera Pavelló
9 Picanya (Coberta Frupale)
*/
select * from (
SELECT reg.time at time zone 'Europe/Madrid' as time,
        plant.id as plant_id,
       plant.name as plant_name,
        meter.name as device_name,
        case 
           when plant.id = 11 then 'CH'
           when plant.id = 10 then 'PB'
           else 'PV' 
        end as plant_type,
        CASE
           WHEN reg.export_energy_wh = 0 THEN 'Energia a 0'
           ELSE 'OK'
       END AS alarm_no_energy,
       sunrise at time zone 'Europe/Madrid' as sunrise,
       sunset at time zone 'Europe/Madrid' as sunset,
       --reg.time + interval '1 hour' between solarevent.sunrise and solarevent.sunset as is_daylight --why did we add one hour?
       reg.time between solarevent.sunrise and solarevent.sunset as is_daylight
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id=reg.meter
LEFT JOIN plant ON plant.id=meter.plant
left join solarevent on plant.id = solarevent.plant and date(solarevent.sunset) = date(reg.time)
WHERE 
  reg.time between '{{ interval.start }}' and '{{ interval.end }}'
  AND reg.export_energy_wh = 0
) as sub
where
time between '{{ interval.start }}' and '{{ interval.end }}'
and plant_type in ({{plant_type}})
  AND alarm_no_energy != 'OK'
  AND ((plant_id in ({{ fv_plants }}) 
  --AND time + interval '1 hour' between sunrise and sunset) --why did we add one hour?
  AND time between sunrise and sunset) 
  OR plant_id in ({{ biogas_plants }}))
  order by time desc, plant_id, alarm_no_energy asc;