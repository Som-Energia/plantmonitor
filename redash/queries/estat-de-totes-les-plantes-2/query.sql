SELECT status, count(*) AS estat
FROM
  (SELECT plant_name,
          CASE when sum(alarm NOT LIKE 'OK') > 0 THEN 'error'
              ELSE 'ok'
          END AS status
   FROM
     (SELECT *
      FROM query_6
      UNION SELECT *
      FROM query_68
      UNION SELECT *
      FROM query_69)
   GROUP BY plant_name)
GROUP BY status