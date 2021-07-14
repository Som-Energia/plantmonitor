SELECT coalesce(ireg."time", sensorirradiationregistry.time, view_expected_power.time) AS "time",
           COALESCE(inverter.name, 'Tots els inversors') as "inverter::filter",
           sum(ireg.power_w)/1000.0 AS power_kw,
           sensorirradiationregistry.irradiation_w_m2,
           view_expected_power.expectedpower as inverter_expected_power_kw
   FROM inverterregistry as ireg
   LEFT JOIN inverter ON inverter.id = ireg.inverter
   LEFT JOIN plant ON plant.id = inverter.plant
   JOIN sensor on sensor.plant = inverter.plant
   JOIN sensorirradiationregistry on sensor.id = sensorirradiationregistry.sensor and sensorirradiationregistry.time = ireg.time
   JOIN view_expected_power on view_expected_power.sensor = sensorirradiationregistry.sensor and view_expected_power.time = ireg.time
WHERE 
    sensor.classtype = 'SensorIrradiation' and
    plant.name = '{{ plant }}'  AND
    ireg."time" >= '{{ interval.start }}' AND
    ireg."time" <= '{{ interval.end }}'
GROUP BY ireg.time,
     sensorirradiationregistry.time,
     view_expected_power.time,
     sensorirradiationregistry.irradiation_w_m2,
     view_expected_power.expectedpower,
     plant.name,
     rollup(inverter.name)
ORDER BY ireg."time"
limit 10000;

