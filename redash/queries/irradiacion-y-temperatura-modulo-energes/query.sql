SELECT TIME,
       temperature_dc/10 AS temperatura_c,
       irradiation_w_m2
FROM view_sensorirradiationregistry_energes
WHERE TIME BETWEEN '{{ interval.start }}' AND '{{ interval.end }}'
ORDER BY sensor,
         "time";