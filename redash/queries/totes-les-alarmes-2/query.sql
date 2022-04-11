SELECT status, count(*) AS estat
FROM
  (SELECT device_name, CASE when alarm NOT LIKE 'OK' THEN 'error'
              ELSE 'ok'
          END AS status
   FROM
     (SELECT *
      FROM query_6
      UNION SELECT *
      FROM query_68
      UNION SELECT *
      FROM query_69)
    )
GROUP BY status