SELECT date_trunc('{{ granularity }}', reg."time") AT TIME ZONE 'Europe/Madrid' AS "time",
                                                                             reg.plant_name,
                                                                             reg.inverter_name,
                                                                             avg(reg.temperature_dc)/10.0 AS avg_temperature_dc
FROM view_inverter_temperature_exiom reg
WHERE "time" >= '{{ interval.start }}'
  AND "time" <= '{{ interval.end }}'
GROUP BY (date_trunc('{{ granularity }}', reg."time")), plant_name,
                                                                     inverter_name
ORDER BY "time" DESC,
         inverter_name ASC;