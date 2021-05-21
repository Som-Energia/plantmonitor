 SELECT "time",
 1 AS ht
   FROM hourlysensorirradiationregistry
  WHERE integratedirradiation_wh_m2 > 5
  order by time;