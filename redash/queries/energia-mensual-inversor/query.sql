SELECT date_trunc('{{ granularitat }}', ireg.time at time zone 'Europe/Madrid') AS "time",
          plant.id as plant,
          sum(ireg.power_w)/1000000 AS power_mw
   FROM inverterregistry AS ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE plant.name = '{{ plant }}'
     AND ireg."time" at time zone 'Europe/Madrid' >= '{{ interval.start }}'
     AND ireg."time" at time zone 'Europe/Madrid' <= '{{ interval.end }}'
   GROUP BY date_trunc('{{ granularitat }}', ireg.time at time zone 'Europe/Madrid'),
            plant.id