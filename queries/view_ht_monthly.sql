 SELECT
      date_trunc('month', "time") AS "time",
      count(integratedirradiation_wh_m2) AS ht
   FROM hourlysensorirradiationregistry
  WHERE integratedirradiation_wh_m2 > 5
  GROUP BY date_trunc('month', "time") order by "time";