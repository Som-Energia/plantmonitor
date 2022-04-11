/*
2 Riudarenes Zona Esportiva, 4 Riudarenes Semursa, 5 Riudarenes Brigada,
14 Manlleu Piscina, 15 Manlleu Pavelló, 16 Lleida (Coberta TMI), 17 Torrefarrera Pavelló
9 Picanya (Coberta Frupale)
10 Torregrossa 11 Valteina
*/
SELECT distinct on (plant.id) reg.time at time zone 'Europe/Madrid' as time,
        plant.id as plant_id,
       plant.name as plant_name,
        meter.name as device_name,
        case 
           when plant.id = 11 then 'CH'
           when plant.id = 10 then 'PB'
           else 'PV' 
        end as plant_type,
        CASE
           WHEN reg.export_energy_wh = 0 THEN 1
           ELSE 0
       END AS alarm_no_energy,
       sunrise at time zone 'Europe/Madrid' as sunrise,
       sunset at time zone 'Europe/Madrid' as sunset,
       reg.time + interval '1 hour' between solarevent.sunrise and solarevent.sunset as is_daylight
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id=reg.meter
LEFT JOIN plant ON plant.id=meter.plant
left join solarevent on plant.id = solarevent.plant and date(solarevent.sunset) = date(reg.time)
WHERE 
  plant.id in (10, 11)
  and reg.time + interval '1 hour' between solarevent.sunrise and solarevent.sunset
  order by plant.id, time desc;