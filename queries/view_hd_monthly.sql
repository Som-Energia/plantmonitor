 SELECT date_trunc('month', "time") AS "time",
    plant,
    count(*) AS hd
   FROM view_pr_hourly
  WHERE pr_hourly > 0.70
  group by date_trunc('month', "time"), plant;