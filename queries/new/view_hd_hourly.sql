 SELECT "time" AS "time",
    plant,
    1 AS hd
   FROM view_pr_hourly
  WHERE pr_hourly > 0.70;