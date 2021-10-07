SELECT date_trunc('{{ granularity }}', TIME) AS TIME,
       inverter_name,
       sum(energy_wh)/1000 AS energy_kwh
FROM view_inverter_energy_daily_exiom
GROUP BY inverter_name,
         date_trunc('{{granularity}}', TIME);