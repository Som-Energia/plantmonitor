 SELECT sum(integrated_sensors.integral_irradiation_wh_m2) AS yearly_acumulated_irradiation,
    date_trunc('year'::text, integrated_sensors."time") AS year_date_irradiation_pr
   FROM public.integrated_sensors
  GROUP BY (date_trunc('year'::text, integrated_sensors."time"));


