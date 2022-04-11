SELECT strftime('%Y-%m-15T00:00:00', "time") AS month,
       avg(pr_hourly) AS pr_monthly
FROM query_32
GROUP BY strftime('%Y-%m', "time");