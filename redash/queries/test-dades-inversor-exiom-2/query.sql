SELECT TIME,
       inverter,
       energy_wh
FROM inverterregistry as reg
WHERE inverter IN (30,
                   31,
                   32,
                   33)
  AND date_trunc('{{granularity}}', reg.time AT TIME ZONE 'Europe/Madrid') BETWEEN '{{ interval.start }}' AND '{{ interval.end }}' ;

