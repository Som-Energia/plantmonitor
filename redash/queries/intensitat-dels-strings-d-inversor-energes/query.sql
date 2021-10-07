SELECT date_trunc('{{ granularity }}', reg.time at time zone 'Europe/Madrid') AS temps,
       plant_name AS plant,
       inverter_name as "inverter::filter",
       string_name as string,
       avg(intensity_mA)/1000.0 as intensity_A
FROM view_inverter_intensity_energes AS reg
WHERE plant_name='{{ plant }}'
  AND reg.time >= '{{ interval.start }}'
  AND reg.time <= '{{ interval.end }}'
GROUP BY date_trunc('{{ granularity }}', reg.time at time zone 'Europe/Madrid'),
         plant_name,
         "inverter::filter",
         string_name
order by temps, plant, "inverter::filter", string desc;

