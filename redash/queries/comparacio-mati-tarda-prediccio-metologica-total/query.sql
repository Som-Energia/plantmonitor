SELECT date_trunc('hour', time) as hour,
       sum(percentil50) as energia,
       --forecastdate as request_time,
       date_trunc('hour', forecastdate) as forecasthour
FROM forecast
LEFT JOIN forecastmetadata ON forecastmetadata.id = forecast.forecastmetadata
left join plant on plant.id = forecastmetadata.plant
WHERE forecastmetadata.forecastdate::date = '2022-01-20'
group by hour, forecasthour
order by hour desc;