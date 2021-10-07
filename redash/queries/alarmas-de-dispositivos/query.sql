SELECT device_name, alarm, time as last_reading,
CASE alarm WHEN 'OK' then 1
ELSE 0
END as alarm_status_int
FROM
  (SELECT *
   FROM query_6
   UNION SELECT *
   FROM query_68
   UNION SELECT *
   FROM query_69) AS alarms
WHERE plant_name='{{ plant }}'