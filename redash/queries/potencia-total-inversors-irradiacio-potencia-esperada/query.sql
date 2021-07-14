SELECT coalesce(inv_power."time"  at time zone 'Europe/Madrid', sensorirradiationregistry."time", view_expected_power."time") at time zone 'Europe/Madrid' as temps,
    inv_power.power_w/1000.0 as power_kw,
    sensorirradiationregistry.irradiation_w_m2,
    view_expected_power.expectedpower as expectedpower_kw
    FROM
  (SELECT ireg."time" at time zone 'Europe/Madrid' AS "time",
          plant.id as plant,
          sum(ireg.power_w) AS power_w
   FROM inverterregistry AS ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE plant.name = '{{ planta }}'
     AND ireg."time" at time zone 'Europe/Madrid' >= '{{ interval.start }}'
     AND ireg."time" at time zone 'Europe/Madrid' <= '{{ interval.end }}'
   GROUP BY ireg.time,
            plant.id) AS inv_power
join sensor on sensor.plant = inv_power.plant
join sensorirradiationregistry on sensorirradiationregistry.sensor = sensor and sensorirradiationregistry.time = inv_power.time
join view_expected_power on view_expected_power.plant = inv_power.plant and view_expected_power.time = inv_power.time
join plant on plant.id = inv_power.plant
   WHERE sensor.classtype = 'SensorIrradiation' and plant.name = '{{ planta }}'
     AND inv_power."time" at time zone 'Europe/Madrid' >= '{{ interval.start }}'
     AND inv_power."time" at time zone 'Europe/Madrid' <= '{{ interval.end }}'
ORDER BY temps
LIMIT 10000;