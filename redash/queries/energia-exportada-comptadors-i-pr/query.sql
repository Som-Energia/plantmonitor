SELECT coalesce(meter_energy.temps, sub_pr.time) as temps,
       meter_energy.total_energy_wh/1000.0 as exported_energy_kwh,
       sub_pr.pr as pr
FROM
  (SELECT date_trunc('{{ granularity }}', reg.time at time zone 'Europe/Madrid') AS temps,
          plant.name AS plant,
          meter.name AS meter,
          sum(reg.export_energy_wh) AS total_energy_wh
   FROM meterregistry AS reg
   LEFT JOIN meter ON meter.id=reg.meter
   LEFT JOIN plant ON plant.id=meter.plant
   WHERE plant.name='{{ plant }}'
     AND reg.time >= '{{ interval.start }}'
     AND reg.time <= '{{ interval.end }}'
   GROUP BY date_trunc('{{ granularity }}', reg.time at time zone 'Europe/Madrid'),
            plant.name,
            meter.name) AS meter_energy
FULL OUTER JOIN
  (
SELECT date_trunc('{{ granularity }}', meterregistry.time at time zone 'Europe/Madrid') AS "time",
       meter.name AS meter,
       plant.name AS plant,
       sum(export_energy_wh) AS export_energy_wh,
       sum(view_irradiation.irradiation_w_m2_h) AS irradiation_w_m2_h,
       100*(sum(export_energy_wh / plantparameters.peak_power_w::float) / sum(NULLIF(view_irradiation.irradiation_w_m2_h, 0.0) / 1000.0)) AS pr
FROM meterregistry
JOIN meter ON meterregistry.meter = meter.id
JOIN view_irradiation ON view_irradiation."time" = meterregistry."time"
JOIN sensor ON sensor.plant = meter.plant
AND view_irradiation.sensor = sensor.id
JOIN plant ON meter.plant = plant.id
JOIN plantparameters ON plant.id = plantparameters.plant
WHERE plant.name = '{{ plant }}'
  AND meterregistry.time >= '{{ interval.start }}'
  AND meterregistry.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', meterregistry.time at time zone 'Europe/Madrid'),
         meter.name,
         plant.name) AS sub_pr ON meter_energy.temps = sub_pr.time;