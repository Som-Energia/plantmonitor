SELECT coalesce(meter_energy.temps, inverter_energy.temps) as temps,
       meter_energy.total_energy_wh as exported_energy,
       inverter_energy.total_energy_wh as produced_energy
FROM
  (SELECT date_trunc('{{ granularity }}', reg.time) AS temps,
          plant.name AS plant,
          meter.name AS meter,
          sum(reg.export_energy_wh) AS total_energy_wh
   FROM meterregistry AS reg
   LEFT JOIN meter ON meter.id=reg.meter
   LEFT JOIN plant ON plant.id=meter.plant
   WHERE plant.name='{{ planta }}'
     AND reg.time >= '{{ interval.start }}'
     AND reg.time <= '{{ interval.end }}'
   GROUP BY date_trunc('{{ granularity }}', reg.time),
            plant.name,
            meter.name) AS meter_energy
FULL OUTER JOIN
  (SELECT date_trunc('{{ granularity }}', reg.time) AS temps,
          plant.name AS plant,
          sum(reg.energy_wh) AS total_energy_wh
   FROM inverterregistry AS reg
   LEFT JOIN inverter ON inverter.id=reg.inverter
   LEFT JOIN plant ON plant.id=inverter.plant
   WHERE plant.name='{{ planta }}'
     AND reg.time >= '{{ interval.start }}'
     AND reg.time <= '{{ interval.end }}'
   GROUP BY date_trunc('{{ granularity }}', reg.time),
            plant.name) AS inverter_energy ON meter_energy.temps = inverter_energy.temps;

