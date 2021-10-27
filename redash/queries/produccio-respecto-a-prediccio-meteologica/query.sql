SELECT reg.time as meter_time,
       forecastreg.time as forecast_time,
       forecastreg.forecastdate as forecastmetadata_time,
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
AND reg.time <= '{{ interval.end }}'
order by forecastreg.time desc


