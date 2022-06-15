select
sensor,
sir.time,
coalesce(sir.irradiation_wh_m2, sat.global_tilted_irradiation_wh_m2) as irradiation_wh_m2_coalesce,
case when sir.quality < 1 then sat.global_tilted_irradiation_wh_m2 else sir.irradiation_wh_m2 end as irradiation_wh_m2,
case when sir.quality < 1 then 'satellite' else 'sonda' end as reading_source
from
(
   -- replace wieh view_irradiation (v2 with 12 average and NULL if one record is NULL (e.g. quality < 1))
    select sensor, time_bucket_gapfill('1 hour', time) as time, avg(irradiation_w_m2) as irradiation_wh_m2, count(irradiation_w_m2)::float/count(*) as quality
    from (
       -- replace with bucket_5min_sensorirradiation from maintenance
    SELECT
       time_bucket_gapfill('5 minutes', sir.time::timestamptz) as time,
       sir.sensor,
       avg(sir.irradiation_w_m2)::int as irradiation_w_m2,
       avg(sir.temperature_dc)::int as temperature_dc
    FROM sensorirradiationregistry as sir
    WHERE time between '2022-05-16 10:00' and '2022-05-16 13:00'
    GROUP by sir.sensor, time_bucket_gapfill('5 minutes', sir.time::timestamptz)
    order by time desc, sir.sensor

    ) as reg
    left join sensor on sensor.id = reg.sensor
    left join plant on plant.id = sensor.plant
    where time between '2022-05-16 10:00' and '2022-05-16 13:00'
    and (sensor = 16)
    group by sensor, time_bucket_gapfill('1 hour', time)
) as sir
left join sensor on sensor.id = sir.sensor
left join plant on plant.id = sensor.plant
left join satellite_readings as sat on sat.plant = sensor.plant and time_bucket('1 hour', sat.time) = time_bucket('1 hour', sir.time)
limit 10;
