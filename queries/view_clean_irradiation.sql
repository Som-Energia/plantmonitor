-- TODO decide:
-- delete this view and do it in redash instead, with proper filters
-- [chosen option] or hope that it's true that the planner limits the rows necessary by the subquery given a parent where clause
-- Time it to decide
select
   sensor,
   sir.time + interval '1 hour',
   coalesce(sir.irradiation_wh_m2, sat.global_tilted_irradiation_wh_m2) as irradiation_wh_m2_coalesce,
   case when sir.quality < 1 then sat.global_tilted_irradiation_wh_m2 else sir.irradiation_wh_m2 end as irradiation_wh_m2,
   case when sir.quality < 1 then 'satellite' else 'sonda' end as reading_source
from irradiationregistry as sir
left join sensor on sensor.id = sir.sensor
left join plant on plant.id = sensor.plant
left join satellite_readings as sat on sat.plant = sensor.plant and time_bucket('1 hour', sat.time) = time_bucket('1 hour', sir.time);
