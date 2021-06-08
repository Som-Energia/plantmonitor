SELECT date_trunc('{{ granularitat }}', ireg.time at time zone 'Europe/Madrid') AS "time",
          plant.id as plant,
          sum(irradiation_w_m2)/1000 AS irradiation_kw_m2
   FROM sensorirradiationregistry AS ireg
   LEFT JOIN sensor ON sensor.id = ireg.sensor
   LEFT JOIN plant ON plant.id = sensor.plant
   WHERE plant.name = '{{ plant }}'
     AND ireg."time" >= '{{ interval.start }}'
     AND ireg."time" <= '{{ interval.end }}'
   GROUP BY date_trunc('{{ granularitat }}', ireg.time at time zone 'Europe/Madrid'),
            plant.id
            