SELECT
    time,
    COALESCE(reg.inverter_name, 'Tots els inversors') as "inverter::filter",
    sum(reg.energy_wh) AS energy_wh,
    max(reg.max_temperature_dc) AS max_temperature_dc,
    min(reg.min_temperature_dc) AS min_temperature_dc,
    avg(reg.avg_temperature_dc) AS avg_temperature_dc,
    sum(reg.power_w) AS power_w
FROM (
    SELECT date_trunc('{{ granularity }}', reg."time") AS "time",
          reg.plant AS plant,
          reg.plant_name,
          reg.inverter AS inverter,
          reg.inverter_name,
          sum(reg.energy_wh) AS energy_wh,
          max(reg.max_temperature_dc) AS max_temperature_dc,
          min(reg.min_temperature_dc) AS min_temperature_dc,
          avg(reg.avg_temperature_dc) AS avg_temperature_dc,
          max(reg.power_w) AS power_w
    FROM view_inverter_metrics_daily AS reg
    WHERE '{{ granularity }}' IN ('month', 'year')
    GROUP BY (date_trunc('{{ granularity }}', reg."time")), reg.plant,
                                                           reg.plant_name,
                                                           reg.inverter,
                                                           reg.inverter_name
UNION
SELECT date_trunc('{{ granularity }}', inverterregistry."time") AS "time",
           plant.id AS plant,
           plant.name AS plant_name,
           inverter.id AS inverter,
           inverter.name AS inverter_name,
           max(inverterregistry.energy_wh) AS energy_inversor_wh,
           max(inverterregistry.temperature_dc) AS max_temperature_dc,
           min(inverterregistry.temperature_dc) AS min_temperature_dc,
           avg(inverterregistry.temperature_dc) AS avg_temperature_dc,
           max(inverterregistry.power_w) AS power_w
   FROM public.inverterregistry
   LEFT JOIN public.inverter ON inverter.id = inverterregistry.inverter
   LEFT JOIN public.plant ON plant.id = inverter.plant
   WHERE '{{ granularity }}' NOT IN ('month', 'year')
   GROUP BY (date_trunc('{{ granularity }}', inverterregistry."time")), plant.id,
                                                                        plant.name,
                                                                        inverter.id,
                                                                        inverter.name
) as reg
WHERE 
    plant = {{ plant }}  AND
    "time" >= '{{ interval.start }}' AND
    "time" <= '{{ interval.end }}' AND
    TRUE
GROUP bY time, ROLLUP(inverter_name)
ORDER BY "time",
         inverter_name;

