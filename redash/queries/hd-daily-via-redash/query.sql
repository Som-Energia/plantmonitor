 SELECT strftime('%Y-%m-%dT00:00:00', "time") AS "time",
    plant,
    count(pr_hourly) AS hd
   FROM query_32
  WHERE pr_hourly > 0.70
  GROUP BY strftime('%Y-%m-%d', time), plant;