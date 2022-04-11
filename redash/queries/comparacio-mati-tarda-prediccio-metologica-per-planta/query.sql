SELECT date_trunc('hour', time) as hour,
       sum(percentil50) as energia,
       plant.name,
       --forecastdate as request_time,
       date_trunc('hour', forecastdate) as forecasthour
FROM forecast
LEFT JOIN forecastmetadata ON forecastmetadata.id = forecast.forecastmetadata
left join plant on plant.id = forecastmetadata.plant
WHERE forecastmetadata.forecastdate::date = '2022-01-20' and plant.name = '{{ plant }}'
group by hour, forecasthour, plant.name
order by hour desc;