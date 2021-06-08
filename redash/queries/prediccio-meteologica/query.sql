SELECT forecast.time as "time",
plant.name as plant,
forecast.percentil50/1000.0 as energy_kwh
FROM forecast
LEFT JOIN forecastmetadata ON forecastmetadata.id = forecast.forecastmetadata
LEFT JOIN plant ON plant.id = forecastmetadata.plant
WHERE plant.name = '{{ plant }}'
  AND "time" >= '{{ interval.start }}'
  AND "time" <= '{{ interval.end }}';