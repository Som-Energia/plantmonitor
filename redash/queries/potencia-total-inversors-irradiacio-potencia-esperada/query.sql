SELECT coalesce(inv_power."time", sensorirradiationregistry."time", view_expected_power."time") as temps,
    inv_power.power_w/1000.0 as power_kw,
    sensorirradiationregistry.irradiation_w_m2,
    view_expected_power.expectedpower as expectedpower_kw
    FROM
  (SELECT ireg."time" AS "time",
          plant.id as plant,
          sum(ireg.power_w) AS power_w
   FROM inverterregistry AS ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   WHERE plant.name = '{{ plant }}'
     AND ireg."time" >= '{{ interval.start }}'
     AND ireg."time" <= '{{ interval.end }}'
   GROUP BY ireg.time,
            plant.id) AS inv_power
join sensor on sensor.plant = inv_power.plant
join sensorirradiationregistry on sensorirradiationregistry.sensor = sensor and sensorirradiationregistry.time = inv_power.time
join view_expected_power on view_expected_power.plant = inv_power.plant and view_expected_power.time = inv_power.time
join plant on plant.id = inv_power.plant
   WHERE sensor.classtype = 'SensorIrradiation' and plant.name = '{{ plant }}'
     AND inv_power."time" >= '{{ interval.start }}'
     AND inv_power."time" <= '{{ interval.end }}'
ORDER BY temps
LIMIT 10000;