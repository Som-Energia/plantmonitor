SELECT date_trunc('{{ granularitat }}', freg.time at time zone 'Europe/Madrid') AS "time",
          plant.id as plant,
          sum(freg.percentil50)/1000.0 as energy_kwh
   FROM forecast AS freg
   LEFT JOIN forecastmetadata ON forecastmetadata.id = freg.forecastmetadata
   LEFT JOIN plant ON plant.id = forecastmetadata.plant
   WHERE plant.name = '{{ plant }}'
     AND freg."time" >= '{{ interval.start }}'
     AND freg."time" <= '{{ interval.end }}'
   GROUP BY date_trunc('{{ granularitat }}', freg.time at time zone 'Europe/Madrid'),
            plant.id