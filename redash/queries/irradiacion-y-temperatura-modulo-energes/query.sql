SELECT TIME,
       plant_name,
       sensor_name,
       temperature_dc/10 AS temperatura_c,
       irradiation_w_m2
FROM view_sensorirradiationregistry_energes as reg
WHERE TIME BETWEEN '{{ interval.start }}' AND '{{ interval.end }}' AND plant_name = '{{ plant }}'
ORDER BY sensor_name,
         "time";