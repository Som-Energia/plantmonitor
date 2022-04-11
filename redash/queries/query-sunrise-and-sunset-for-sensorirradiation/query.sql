-------------------------
WITH sol as (
    select solarevent.id,plant.id as plant,
    sunrise at time zone 'Europe/Madrid' as sunrise,
    sunset at time zone 'Europe/Madrid' as sunset
    from solarevent
    join plant ON plant.id=solarevent.plant
    WHERE sunrise >= '{{ interval.start }}'
    AND sunset <= '{{ interval.end }}'
)

select date_trunc('day',reg.time at time zone 'Europe/Madrid') as time,
    reg.sensor,
    sol.sunrise at time zone 'Europe/Madrid' as sunrise,
    sol.sunset at time zone 'Europe/Madrid' as sunset
from sensorirradiationregistry AS reg
LEFT JOIN sensor ON sensor.id=reg.sensor
LEFT JOIN plant ON plant.id=sensor.plant  
LEFT JOIN sol ON sol.plant=plant.id
    WHERE TIME >= '{{ interval.start }}'
    AND TIME <= '{{ interval.end }}'
    and date_trunc('day',reg.time at time zone 'Europe/Madrid') = date_trunc('day',sol.sunrise at time zone 'Europe/Madrid')
    GROUP by date_trunc('day',reg.time at time zone 'Europe/Madrid'),
    reg.sensor,
    sol.sunrise at time zone 'Europe/Madrid',
    sol.sunset at time zone 'Europe/Madrid'

