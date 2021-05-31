SELECT inverter."time",
       inverter.plant_id,
       inverter.plant_name,
       max(meter.export_energy_wh) AS meter_energy_wh,
       sum(inverter.energy_wh) AS inverter_energy_wh,
       ((max(meter.export_energy_wh) - sum(inverter.energy_wh))/sum(inverter.energy_wh))*100 AS producction_vs_inverter_pc
FROM view_inverter_energy_monthly AS inverter
LEFT JOIN view_meter_export_energy_monthly AS meter ON inverter.time = meter.time
AND inverter.plant_id = meter.plant_id
WHERE inverter.plant_id = {{ plant }}
  AND inverter.time >= '{{ interval.start }}'
  AND inverter.time <= '{{ interval.end }}'
GROUP BY inverter.time,
         inverter.plant_id,
         inverter.plant_name
ORDER BY inverter.time DESC,
         inverter.plant_id,
         inverter.plant_name ;

