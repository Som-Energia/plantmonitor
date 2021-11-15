select time at time zone 'Europe/Madrid' as temps,
    plant.name as plant_name,
    case when sonda_zero then plant.name || '-alarm' end as alarm_zero,
    case when sonda_12_equal then plant.name || '-alarm' end as alarm_12_equal,
    case when sonda_massa_irradiacio then plant.name || '-alarm' end as alarm_massa_irradiacio,
    case when now() at time zone 'Europe/Madrid' - interval '7 minutes' < time then (
        case when (sonda_massa_irradiacio or sonda_12_equal or sonda_zero) = false then plant.name || '-ok'
        end)
    else NULL
    end as current_ok,
    case when now() at time zone 'Europe/Madrid' - interval '7 minutes' < time then (
        case when (sonda_massa_irradiacio or sonda_12_equal or sonda_zero) = true then plant.name || '-CurrentAlarm'
        end)
    end as current_alarm
from
    (SELECT reg.time at time zone 'Europe/Madrid' as time,
               reg.sensor,
               reg.irradiation_w_m2,
               reg.irradiation_w_m2 = 0 as sonda_zero,
               reg.irradiation_w_m2 = lag(reg.irradiation_w_m2, 1) OVER(ORDER BY reg.time) AND reg.irradiation_w_m2 = lag(reg.irradiation_w_m2, 2) OVER(ORDER BY reg.time) as sonda_3_equal,
               case when count(*) over w < 3 then NULL else max(reg.irradiation_w_m2) over w = min(reg.irradiation_w_m2) OVER w end as sonda_12_equal,
               reg.irradiation_w_m2 > 1400 as sonda_massa_irradiacio
        from sensorirradiationregistry as reg
        INNER JOIN
            (SELECT
                   date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid') as time,
                   reg.sensor,
                   MIN(reg.time) as sunrise,
                   MAX(reg.time) as sunset
                FROM sensorirradiationregistry AS reg
                WHERE TIME >= '{{ interval.start }}'
                  AND TIME <= '{{ interval.end }}'
                  AND reg.irradiation_w_m2 > 10
                GROUP by date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid'),
                reg.sensor
                ORDER BY time DESC
            ) as sunrise_sunset ON sunrise_sunset.time = date_trunc('day', reg.time AT TIME ZONE 'Europe/Madrid')
        WHERE reg.time between sunrise and sunset
        window w as (partition by reg.sensor order by reg.time ROWS between 11 preceding and current row)
        ORDER by reg.time
    ) sub
    LEFT JOIN sensor ON sensor.id = sub.sensor
    LEFT JOIN plant ON sensor.plant = plant.id
    where sub.time between '{{ interval.start }}' and '{{ interval.end }}'
    and (sub.sonda_zero or sub.sonda_12_equal or sub.sonda_massa_irradiacio or sub.time > now() at time zone 'Europe/Madrid' - interval '7 minutes')
    order by temps desc;