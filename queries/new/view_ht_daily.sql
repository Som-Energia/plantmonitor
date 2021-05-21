 SELECT
      date_trunc('day', "time") AS "time",
      count(integratedirradiation_wh_m2) AS ht
   FROM hourlysensorirradiationregistry
  WHERE integratedirradiation_wh_m2 > 5
  GROUP BY date_trunc('day', "time") order by "time";