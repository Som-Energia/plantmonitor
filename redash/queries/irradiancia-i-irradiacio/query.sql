SELECT coalesce(reg.time, hourlysensorirradiationregistry.time) AS "time",
reg.irradiation_w_m2,
hourlysensorirradiationregistry.integratedirradiation_wh_m2
FROM sensorirradiationregistry AS reg
FULL OUTER JOIN hourlysensorirradiationregistry
on hourlysensorirradiationregistry.time = reg.time
ORDER BY "time" DESC
LIMIT 1000;

