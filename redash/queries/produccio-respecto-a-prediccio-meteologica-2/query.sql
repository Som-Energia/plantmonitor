SELECT reg.time at time zone 'Europe/Madrid' as meter_time,
       forecastreg.time at time zone 'Europe/Madrid' as forecast_time,
       forecastreg.forecastdate at time zone 'Europe/Madrid' as forecastmetadata_time,
       export_energy_wh as export_energy_wh,
       forecastreg.meteo_export_energy_wh,
       meter.name as meter_name
FROM meterregistry AS reg
LEFT JOIN meter ON meter.id = reg.meter
LEFT JOIN plant ON plant.id = meter.plant
INNER JOIN LATERAL
    (SELECT sreg.time as time,
            percentil50 as meteo_export_energy_wh,
            forecastmetadata.plant,
            forecastmetadata.forecastdate
            from forecast as sreg
            LEFT JOIN forecastmetadata on forecastmetadata.id = sreg.forecastmetadata
            where sreg.time = reg.time and date_trunc('day',forecastmetadata.forecastdate) = date_trunc('day',sreg.time) - interval '1 day'
            and forecastmetadata.plant = meter.plant
            order by sreg.time desc limit 1
            ) forecastreg ON forecastreg.plant = plant.id
where plant.name = '{{plant}}'
AND reg.time >= '{{ interval.start }}'
AND reg.time - interval '25 hour' <= '{{ interval.end }}' --added so that last month includes until 00:00 from the 1st day
order by forecastreg.time desc


