SELECT r.sensor,
       date_trunc('month', r.time AT TIME ZONE 'EUROPE/MADRID') AT TIME ZONE 'EUROPE/MADRID' AS "time",
       count(r.integratedirradiation_wh_m2) AS ht,
       p.name as plant
FROM plant AS p
INNER JOIN sensor s ON s.plant = p.id
INNER JOIN hourlysensorirradiationregistry AS r ON r.sensor = s.id
WHERE integratedirradiation_wh_m2 > 5
GROUP BY date_trunc('month', r.time AT TIME ZONE 'EUROPE/MADRID'),
         r.sensor,
         p.name,
	 r.time
ORDER BY r.time;
